"""Orchestration behind the ``compute_weights`` step.

Runs the weighting via
[`WeightingPipeline`][processing.weighting.core.pipeline.WeightingPipeline]:

1.  **Setup** — parse YAML config → specs, target names, merges, importance.
    Cross-tab controls are registered with pre-merged dimensions so the
    enum reflects the effective cell count.
2.  **Data fetching** — read the official PUMS files (downloading any that are
    missing); receive survey tables.
3.  **Zone assignment** — crosswalk assigns ``target_zone_id`` and
    ``zone_group_id`` to survey households (point-in-polygon). Zone groups (if
    configured) are applied inside the crosswalk so ``zone_group_id`` is ready
    for balancing.
4.  **Conformance** — recode both PUMS and survey through identical control
    expressions → same control-column schema.
5.  **Incidence pivot** — unified pivoter produces identical
    ``{ctrl}__{member}`` column layout for both datasets; the crosswalk then
    allocates the PUMS weights to target zones.
6.  **1-D merges** — global merges collapse incidence columns symmetrically
    on both tables (originals dropped).  Zone-specific merges add merged
    columns (originals kept) and modify control totals for the specified
    zones after aggregation.
7.  **Control totals** — aggregate PUMS incidence into target totals per zone.
8.  **Balancer** — base weights → max-entropy balancing → weight propagation.

# Design decisions

* **Balancer** — PopulationSim's numba balancer (``np_balancer_numba``),
  vendored in ``balancing/_np_balancer.py``: a pure ``@njit`` function taking
  numpy arrays, so PopulationSim itself is not a dependency.
* **Geography columns** — three distinct levels: ``PUMA`` (raw Census
  PUMA), ``target_zone_id`` (crosswalk target zones from the user's polygon
  file), and ``zone_group_id`` (zone group, equal to ``target_zone_id``
  unless zone groups are configured).  Downstream balancing always uses
  ``zone_group_id``; diagnostics/maps use ``target_zone_id`` for spatial detail.
* **Symmetric incidence** — both the survey sample and the PUMS universe are
  first recoded and pivoted into incidence tables with identical column
  layouts.  Geography, merges, and crosstabs are applied *after* incidence
  construction, keeping the recode/pivot logic independent of geography.

!!! Algorithm

    Find weight vector **w** closest to seed weights **w₀** (KL-divergence)
    subject to marginal constraints:

    min Σᵢ wᵢ ln(wᵢ / w₀ᵢ)   s.t.  A w = t,  wᵢ ≥ 0

    where **A** is the incidence matrix and **t** is the target totals vector.
    Runs independently per zone group (zone groups are parallelisable).
"""

import logging

import polars as pl

from data_canon.core.dataclass import CanonicalData
from processing.formatting.kept_records import suggest_usability_columns
from processing.usability import usable_col_for
from processing.weighting.balancing.balancer import (
    balance_weights,
    grid_search_expansion_factor,
)
from processing.weighting.balancing.base_weights import (
    compute_base_weights,
    grow_block_households,
)
from processing.weighting.balancing.importance import compute_control_moe, compute_moe_importance
from processing.weighting.controls.household import HHPumaControl, HHTargetZoneControl
from processing.weighting.controls.registry import (
    register_crosstabs_from_config,
    register_puma_control,
    register_target_zone_control,
    resolve_targets,
)
from processing.weighting.core.hierarchy import seed_col_for, weight_col_for
from processing.weighting.core.propagation import (
    propagate_weights,
    safe_join_weight,
    seed_admits,
)
from processing.weighting.core.run_report import generate_diagnostics
from processing.weighting.core.specs import (
    BalancingConfig,
    ControlRegistryConfig,
    ControlTotals,
    DiagnosticsConfig,
    ProfileFit,
    WeightingConfig,
)
from processing.weighting.data_prep.census_geo import get_block_gdf, get_block_households
from processing.weighting.data_prep.control_data import (
    recode_allocated_pums,
    recode_pums_households,
    recode_pums_persons,
)
from processing.weighting.data_prep.crosswalk import PumaCrosswalk
from processing.weighting.data_prep.fractional_impute import TrainedModel, fill_null_incidence
from processing.weighting.data_prep.incidence import (
    IncidenceBundle,
    aggregate_control_totals,
    build_incidence_table,
    drop_lone_target_zones,
    pivot_allocated_controls,
)
from processing.weighting.data_prep.merges import (
    apply_1d_merges,
    merge_control_totals,
)
from processing.weighting.data_prep.pums_cleaning import clean_pums
from processing.weighting.data_prep.pums_data import read_pums
from processing.weighting.data_prep.seed_data import (
    recode_survey_households,
    recode_survey_persons,
    restore_original_answers,
)
from processing.weighting.validation.checksums import check_incidence_sums
from processing.weighting.validation.control_validation import (
    validate_total_control_categories,
    warn_crosstab_sparsity,
)
from processing.weighting.validation.coverage import check_control_geography_coverage
from processing.weighting.validation.survey_shares import (
    check_survey_shares,
    survey_share_table,
)

logger = logging.getLogger(__name__)


# ===========================================================================
# Pipeline class
# ===========================================================================


class WeightingPipeline:
    """Stateful weighting pipeline.

    Separates *configuration* (frozen at ``__init__``) from *intermediate
    state* (built up phase-by-phase).  Phase methods are designed to be
    called in sequence from the
    [`@step() compute_weights`][processing.weighting.compute_weights.compute_weights]
    entry-point; each stores results as instance attributes.

    # Usage

    ```python
    pipeline = WeightingPipeline(
        controls=..., config=..., data=..., balancing=..., importance=...
    )

    pipeline.setup()
    pipeline.fetch_pums()

    pipeline.place_households()
    pipeline.recode_and_pivot()
    pipeline.allocate_pums()
    pipeline.apply_merges()

    pipeline.aggregate_totals()
    pipeline.resolve_importance()

    pipeline.fit_all(diagnostics=...)  # one fit per configured profile
    ```

    # One fit per profile

    Everything above ``fit_all`` describes the survey and the controls, so it is
    shared. Everything from the seed onward belongs to a *profile*: which
    households are admitted, what they were balanced to, and the weight columns
    written. Each fit's products live on its own
    [`ProfileFit`][processing.weighting.core.specs.ProfileFit], and its columns
    carry the profile name as a suffix.

    A profile is fitted, never derived from another profile's fit. Reusing one
    profile's household weights for a different universe produces numbers
    indistinguishable from balanced ones that match no control total; the honest
    way to impose weights from outside is
    [`add_existing_weights`][processing.weighting.existing_weights.add_existing_weights],
    which says so in its name.

    Each fit keeps its own seed, weights and statuses on ``fits``. Sharing
    instance attributes for those would leave the last profile's numbers
    standing as the ones the diagnostics and the checks describe.
    """

    # -- Shared state (populated by the phases every fit reads) ---------
    crosswalk: PumaCrosswalk
    pums_hh: pl.DataFrame
    pums_per: pl.DataFrame
    survey_bundle: IncidenceBundle
    pums_bundle: IncidenceBundle
    pums_incidence: pl.DataFrame
    pums_person_incidence: pl.DataFrame
    control_totals: ControlTotals
    # Per control: what the MOE gave (None when not MOE-based), and what balancing used.
    moe_importance: dict[str, float] | None
    importance_used: dict[str, float]
    control_moe: pl.DataFrame | None
    survey_shares: pl.DataFrame

    # -- Per-profile state ----------------------------------------------
    fits: dict[str, ProfileFit]

    def __init__(
        self,
        *,
        controls: ControlRegistryConfig,
        config: WeightingConfig,
        data: CanonicalData,
        balancing: BalancingConfig,
    ) -> None:
        """Initialise with configuration and survey data."""
        self.controls = controls
        self.config = config
        self.data = data
        self.balancing = balancing

        # Mutable state initialised empty; populated by phases
        self.control_moe = None
        self.moe_importance = None
        self.fits = {}
        self.block_households: pl.DataFrame | None = None
        self.puma_growth: pl.DataFrame | None = None
        # Forests trained this run, by control; every profile's imputation reuses them
        self.rf_models: dict[str, TrainedModel] = {}

    # ------------------------------------------------------------------
    # Phase methods
    # ------------------------------------------------------------------

    def setup(self) -> None:
        """Build crosswalk, register run-time controls, resolve control instances."""
        # The crosswalk over every zone and zone group, kept for zone assignment and diagnostics
        self.crosswalk = PumaCrosswalk(
            self.config.geography,
            state_fips=self.config.state_fips,
            pums_year=self.config.pums_year,
            census_dir=self.config.census_dir,
        )

        # h_puma's categories are the crosswalk's PUMAs, so it is registered here
        register_puma_control(
            self.crosswalk.allocated_puma_ids
            if HHPumaControl.name in self.controls.target_names
            else None
        )
        register_target_zone_control(
            sorted(self.crosswalk.crosswalk_df["target_zone_id"].unique().to_list())
            if HHTargetZoneControl.name in self.controls.target_names
            else None
        )
        register_crosstabs_from_config(self.controls.specs)
        ctrl_instances = resolve_targets(self.controls.target_names)
        validate_total_control_categories(ctrl_instances)
        logger.info("Controls: %s", self.controls.target_names)

    def fetch_pums(self) -> None:
        """Read PUMS from the official files in the census folder, then clean it."""
        self.pums_hh, self.pums_per = read_pums(
            self.config.census_dir,
            self.config.state_fips,
            self.config.pums_year,
            load_replicate_weights=self.balancing.moe_based_importance,
        )
        self.pums_hh, self.pums_per = clean_pums(
            self.pums_hh,
            self.pums_per,
            pums_weights=self.config.pums_weights,
            household_income=self.config.household_income,
        )

        # Ensure that PUMA is 0 padded to 5 digits for consistency with crosswalk
        self.pums_hh = self.pums_hh.with_columns(pl.col("PUMA").cast(pl.Utf8).str.zfill(5))
        self.pums_per = self.pums_per.with_columns(pl.col("PUMA").cast(pl.Utf8).str.zfill(5))

    def recode_and_pivot(self) -> None:
        """Recode both datasets and build incidence tables.

        Two parallel streams — PUMS (complete Census microdata) and
        survey (travel-diary seed) — are recoded through *identical*
        control expressions, then pivoted into incidence tables with the
        same ``{ctrl}__{member}`` column layout.  The PUMS bundle is
        completed first because it serves as the training set for
        RF-based fractional imputation of null survey values.

        Stream diagram::

            PUMS HH/PER ─► recode ─► pivot ─► IncidenceBundle (pums)
                                                       │
                                                       ▼  (training data)
            Survey HH/PER ─► recode ─► pivot ─► IncidenceBundle (survey)
                                                       │
                                                       ▼  (per profile)
                                                  `build_seed`

        The survey bundle is left unfiltered and un-imputed: both depend on which
        profile's universe is being fitted, and one run may fit several.
        """
        names = self.controls.target_names

        # ==============================================================
        # PUMS stream — complete Census microdata (no nulls)
        # ==============================================================
        # Recode raw PUMS HH + person tables through control expressions
        self.pums_hh = recode_pums_households(self.pums_hh, self.pums_per, names)
        self.pums_per = recode_pums_persons(self.pums_per, names)

        # Pivot into incidence bundle (one row per SERIALNO)
        self.pums_bundle = build_incidence_table(
            self.pums_hh,
            self.pums_per,
            names,
            hh_id_col="SERIALNO",
            extra_cols=["WGTP", "PUMA"],
            person_extra_cols=["PWGTP", "PUMA"],
        )
        self.pums_incidence = self.pums_bundle.incidence
        check_incidence_sums(self.pums_incidence, names, source_label="pums")
        # Person targets sum person weights, so they are built from one row per person.
        self.pums_person_incidence = self.pums_bundle.person_pivot

        # ==============================================================
        # Survey stream — travel-diary seed (may contain nulls)
        # ==============================================================
        # Recode canonical survey HH + person tables through the same
        # control expressions used for PUMS above.
        strict_nulls = self.config.strict_survey_nulls
        households, persons = self.data.households, self.data.persons
        if not self.config.use_imputed_values:
            households, persons = restore_original_answers(households, persons)  # pyright: ignore[reportArgumentType]
        hh_recoded = recode_survey_households(
            households,  # pyright: ignore[reportArgumentType]
            persons,  # pyright: ignore[reportArgumentType]
            names,
            strict_nulls=strict_nulls,
        )
        per_recoded = recode_survey_persons(persons, names, strict_nulls=strict_nulls)  # pyright: ignore[reportArgumentType]

        # Pivot into incidence bundle (one row per hh_id). Unfiltered: which
        # households enter a seed is a profile's question, answered per fit in
        # `build_seed`, and one run may build several seeds from this bundle.
        self.survey_bundle = build_incidence_table(hh_recoded, per_recoded, names)

        logger.info(
            "Survey after recoding: %d households, %d persons (before any profile gate)",
            self.survey_bundle.household_pivot["h_total"].sum(),
            self.survey_bundle.person_pivot["p_total"].sum(),
        )

    def place_households(self) -> None:
        """Point-in-polygon zone and PUMA assignment + optional block-group assignment.

        Mutates ``self.data.households`` in place (adds geo columns). Runs before
        `recode_and_pivot`, which reads ``home_puma`` for ``h_puma``. Each seed
        takes its geo columns off ``self.data.households`` in `build_seed`, since a
        seed only exists once a profile has been named.
        """
        hh = self.crosswalk.assign_households(self.data.households)  # pyright: ignore[reportArgumentType]
        n_assigned = hh.filter(pl.col("target_zone_id").is_not_null()).height
        logger.info("Assigned %d / %d households to target zones", n_assigned, len(hh))

        if self.config.sample_plan is not None:
            hh = self.crosswalk.assign_block_groups(hh)
            n_bg = hh.filter(pl.col("bg_geo_id").is_not_null()).height
            logger.info("Assigned %d / %d households to block groups", n_bg, len(hh))

        self.data.households = hh

    def allocate_pums(self) -> None:
        """Allocate the PUMS household and person incidence across zones through the crosswalk."""
        self.pums_incidence = pivot_allocated_controls(
            self.crosswalk.allocate_pums_incidence(self.pums_incidence),
            self.controls.target_names,
        )
        self.pums_person_incidence = self.crosswalk.allocate_pums_incidence(
            self.pums_person_incidence,
            weight_col="PWGTP",
        )

    def apply_merges(self) -> None:
        """Apply global 1-D merges to the PUMS household and person incidence tables.

        Cross-tab merges are applied at registration time (pre-merge into
        the enum), so only 1-D merges need post-pivot application here. The seed
        side is merged in `build_seed`, symmetrically and once per profile.

        Zone merges are left to `aggregate_totals`, which makes each merged group a
        target in its own areas only. Added here, its column would be a target in
        every area, and twice in its own.
        """
        global_merges = [m for m in self.controls.merges_1d if m.zones is None]
        if global_merges:
            self.pums_incidence = apply_1d_merges(self.pums_incidence, global_merges)
            self.pums_person_incidence = apply_1d_merges(self.pums_person_incidence, global_merges)
            logger.info(
                "Applied %d global 1-D merge specs to PUMS; incidence now %d columns",
                len(global_merges),
                len(self.pums_incidence.columns),
            )

    def aggregate_totals(self) -> None:
        """Aggregate PUMS incidence into per-zone control totals."""
        names = self.controls.target_names
        self.control_totals = aggregate_control_totals(
            self.pums_incidence,
            self.pums_person_incidence,
            names,
            geo_col="zone_group_id",
        )
        if self.controls.merges_1d:
            self.control_totals = merge_control_totals(
                self.control_totals,
                self.controls.merges_1d,
            )
        if HHTargetZoneControl.name in names:
            self.control_totals = drop_lone_target_zones(
                self.control_totals, self.crosswalk.crosswalk_df
            )
        logger.info(
            "Control totals: %d zones, %d PUMS households, %d PUMS persons",
            len(self.control_totals.zone_group_ids),
            self.control_totals.pums_hh_count,
            self.control_totals.pums_person_count,
        )

    def resolve_importance(self) -> None:
        """Build the importance of every control (explicit override, MOE-based, or default)."""
        names = self.controls.target_names
        overrides = self.controls.importance_overrides

        if self.balancing.moe_based_importance:
            pums_hh_xw, pums_per_xw = self.crosswalk.allocate_pums_weights(
                self.pums_hh,
                self.pums_per,
            )
            pums_hh_xw = recode_allocated_pums(pums_hh_xw, names)
            # Per-cell MOE, shown in the diagnostics and read for the importance
            self.control_moe = compute_control_moe(pums_hh_xw, pums_per_xw, names)
            self.moe_importance = compute_moe_importance(self.control_moe, names)
            # YAML explicit overrides take precedence over MOE-derived
            overrides = self.moe_importance | overrides

        default = self.balancing.default_importance
        self.importance_used = {name: overrides.get(name, default) for name in names}
        imp_lines = "\n".join(f"  {k}: {v:.1f}" for k, v in self.importance_used.items())
        logger.info("Importance weights:\n%s", imp_lines)

    def build_seed(self, profile: str) -> ProfileFit:
        """Gate, impute, place and merge one profile's seed.

        The profile's own flag decides who enters the seed, so the balancer
        spreads each zone's population over exactly the households that will keep
        a weight. Gating afterwards instead deletes fitted mass that nothing
        re-spreads, households being the hierarchy anchor, and the survey then
        expands to less than the population it was fitted to.

        Geography is a property of the household, but how much of a universe it
        covers is a property of this seed, so it is counted per profile. A
        household in no zone reaches no ``ZoneInput``, so keeping it cannot
        weight it -- it only inflates the response count its sample segment
        divides by. It is dropped before imputation, which needs each
        household's zone and never fills ``h_puma``.

        Null imputation fills RF-predicted fractional probabilities where survey
        respondents with null demographics got zero incidence from the pivot. It
        trains on PUMS split across zones as the targets are, but unmerged, as the
        seed is.

        Args:
            profile: Usability profile to fit.

        Returns:
            The seed and its metadata, ready to balance.

        Raises:
            ValueError: If the gating column is absent from households.
        """
        flag = usable_col_for(profile)
        names = self.controls.target_names
        households = self.data.households
        if flag not in households.columns:
            msg = (
                f"The weighting is gated on {flag!r}, which households does not carry. "
                f"{suggest_usability_columns(households)}"
            )
            raise ValueError(msg)
        admitted = households.filter(seed_admits(flag)).select("hh_id").to_series()
        n_gated = households.height - admitted.len()
        bundle = self.survey_bundle.filter_households(admitted)

        logger.info(
            "Seed for %s: %d households, %d persons, %d households gated out",
            profile,
            bundle.household_pivot["h_total"].sum(),
            bundle.person_pivot["p_total"].sum(),
            n_gated,
        )

        geo_names = ("target_zone_id", "zone_group_id", "bg_geo_id")
        geo_cols = [c for c in geo_names if c in households.columns]
        geo = households.filter(pl.col("hh_id").is_in(admitted)).select("hh_id", *geo_cols)
        coverage = check_control_geography_coverage(
            geo,
            profile=profile,
            max_unplaceable_share=self.config.max_unplaceable_share,
        )
        if coverage.n_unplaceable:
            placed = geo.filter(pl.col("zone_group_id").is_not_null())["hh_id"]
            bundle = bundle.filter_households(placed)

        pre_imputation = bundle.incidence
        seed, imputation_summary = fill_null_incidence(
            bundle,
            self.crosswalk.allocate_pums_incidence(self.pums_bundle.incidence),
            self.crosswalk.allocate_pums_incidence(
                self.pums_bundle.person_pivot, weight_col="PWGTP"
            ),
            geo.select("hh_id", "zone_group_id"),
            names,
            models=self.rf_models,
        )
        # After imputation, every control must sum correctly (with tolerance
        # for floating-point fractions introduced by the RF predictions).
        check_incidence_sums(seed, names, source_label="survey", tolerance=0.01)
        seed = seed.join(geo, on="hh_id", how="left")

        if self.controls.merges_1d:
            seed = apply_1d_merges(seed, self.controls.merges_1d)
        warn_crosstab_sparsity(seed, resolve_targets(names))

        return ProfileFit(
            profile=profile,
            usability_flag_col=flag,
            seed_incidence=seed,
            pre_imputation_incidence=pre_imputation,
            imputation_summary=imputation_summary,
            coverage=coverage,
        )

    def grown_block_households(self) -> pl.DataFrame:
        """Census households per block, grown to the PUMS year, for sample-plan base weights.

        Built once and shared by every profile. The growth by PUMA is kept on
        ``puma_growth``.
        """
        if self.block_households is None:
            state, year = self.config.state_fips, self.config.pums_year
            census_dir = self.config.census_dir
            blocks = self.crosswalk.assign_blocks(get_block_gdf(state, year, census_dir)).join(
                get_block_households(state, year, census_dir), on="block_id"
            )
            self.block_households, self.puma_growth = grow_block_households(blocks, self.pums_hh)
        return self.block_households

    def balance_fit(self, fit: ProfileFit) -> None:
        """Compute base weights and balance one seed, in place on *fit*.

        A non-converged zone is recorded rather than raised on, so every profile
        still produces a diagnostics report to debug from. ``fit_all`` raises once
        at the end, naming each profile that failed.
        """
        names = self.controls.target_names
        fit.seed_incidence = compute_base_weights(
            fit.seed_incidence,
            self.control_totals,
            sample_plan=self.config.sample_plan,
            block_households=self.grown_block_households() if self.config.sample_plan else None,
        )

        fit.weights, fit.statuses = balance_weights(
            fit.seed_incidence,
            self.control_totals,
            names,
            balancing=self.balancing,
            importance=self.importance_used,
        )

        if fit.unconverged_zones:
            logger.error(
                "Balancing failed to converge for %d zones under %s. See logs for details.",
                fit.unconverged_zones,
                fit.profile,
            )

        if self.config.expansion_factor_grid:
            fit.grid_results = grid_search_expansion_factor(
                fit.seed_incidence,
                self.control_totals,
                names,
                ef_grid=self.config.expansion_factor_grid,
                selected_ef=self.balancing.max_expansion_factor,
                balancing=self.balancing,
                importance=self.importance_used,
            )

    def propagate_fit(self, fit: ProfileFit) -> None:
        """Attach one fit's weights to households and propagate them down.

        The only place a profile suffix is applied to canonical data: the
        balancer emits the base column name, knowing nothing about profiles, and
        the rename happens here as it is joined on.

        Households the seed rejected never received a balanced weight. Zeroing
        them on the same expression the seed used is what makes a null mean
        "no estimate exists" rather than "excluded".
        """
        if fit.weights is None:  # pragma: no cover - balance_fit always sets it
            msg = f"Cannot propagate {fit.profile}: it has not been balanced"
            raise ValueError(msg)

        hh_weight_col = weight_col_for("hh_weight", fit.profile)
        base_weight_col = seed_col_for("base_weight", fit.profile)

        self.data.households = safe_join_weight(
            self.data.households,  # pyright: ignore[reportArgumentType]
            fit.weights.select("hh_id", "hh_weight").rename({"hh_weight": hh_weight_col}),
            "hh_id",
        )
        self.data.households = safe_join_weight(
            self.data.households,
            fit.seed_incidence.select("hh_id", "base_weight").rename(
                {"base_weight": base_weight_col}
            ),
            "hh_id",
        )
        tables = self.data.as_dict()
        has_weight: dict[str, str] = {"households": hh_weight_col}

        hh = tables["households"]
        flag = fit.usability_flag_col
        if flag in hh.columns:
            tables["households"] = hh.with_columns(
                pl.when(seed_admits(flag))
                .then(pl.col(hh_weight_col))
                .otherwise(0.0)
                .alias(hh_weight_col)
            )

        # Households, then persons, days, trips and tours
        propagate_weights(
            tables,
            has_weight,
            usability_flag_col=flag,
            profile=fit.profile,
        )

        # Write propagated tables back to self.data
        for name, df in tables.items():
            if df is not None:
                setattr(self.data, name, df)

    def fit_all(self, *, diagnostics: DiagnosticsConfig) -> dict[str, ProfileFit]:
        """Run one fit per configured profile, then raise if any failed to converge.

        Every profile is attempted and the report written before raising, so a
        convergence failure in one costs a diagnosis of the others rather than
        hiding it -- including the failure itself, which the report names.

        Every profile's seed is built before any is balanced, so the survey-share
        check sees them all and stops the run before balancing if a group is too
        small a share of its area's survey to reach its target.

        Raises:
            ValueError: If a group's survey share is below ``min_survey_share_ratio``.
            RuntimeError: If any profile left zones unconverged.
        """
        seeds = {p: self.build_seed(p) for p in self.config.weight_profiles}
        self.survey_shares = survey_share_table(
            {p: fit.seed_incidence for p, fit in seeds.items()}, self.control_totals
        )
        check_survey_shares(self.survey_shares, self.balancing.min_survey_share_ratio)
        for fit in seeds.values():
            self.balance_fit(fit)
            self.propagate_fit(fit)
            self.fits[fit.profile] = fit
        generate_diagnostics(self, diagnostics)

        failed = {
            fit.profile: fit.unconverged_zones
            for fit in self.fits.values()
            if fit.unconverged_zones
        }
        if failed:
            detail = ", ".join(f"{name}: {n} zones" for name, n in failed.items())
            msg = f"Balancing failed to converge ({detail}). See logs and the reports written."
            raise RuntimeError(msg)
        return self.fits
