"""Top-level weighting pipeline step.

Orchestrates the full weighting pipeline via
[`WeightingPipeline`][processing.weighting.core.pipeline.WeightingPipeline]:

1.  **Setup** — parse YAML config → specs, target names, merges, importance.
    Cross-tab controls are registered with pre-merged dimensions so the
    enum reflects the effective cell count.
2.  **Data fetching** — load PUMS (API or files); receive survey tables.
3.  **Conformance** — recode both PUMS and survey through identical control
    expressions → same control-column schema.
4.  **Incidence pivot** — unified pivoter produces identical
    ``{ctrl}__{member}`` column layout for both datasets.
5.  **Zone assignment** — crosswalk assigns ``study_geoid`` and
    ``ctrl_geoid`` to survey HHs (point-in-polygon) and allocates PUMS
    weights to target zones.  Zone groups (if configured) are applied
    inside the crosswalk so ``ctrl_geoid`` is ready for balancing.
6.  **1-D merges** — global merges collapse incidence columns symmetrically
    on both tables (originals dropped).  Zone-specific merges add merged
    columns (originals kept) and modify control totals for the specified
    zones after aggregation.
7.  **Control totals** — aggregate PUMS incidence into target totals per zone.
8.  **Balancer** — base weights → max-entropy balancing → weight propagation.

# Design decisions

* **PopulationSim dependency** — uses PopulationSim's core numba balancer
  (``np_balancer_numba``) directly — a pure ``@njit`` function (~120 lines)
  taking numpy arrays.  No PopulationSim pipeline infrastructure involved.
* **Geography columns** — three distinct levels: ``PUMA`` (raw Census
  PUMA), ``study_geoid`` (crosswalk target zones from the user's polygon
  file), and ``ctrl_geoid`` (balancing geography, equal to ``study_geoid``
  unless zone groups are configured).  Downstream balancing always uses
  ``ctrl_geoid``; diagnostics/maps use ``study_geoid`` for spatial detail.
* **Symmetric incidence** — both the survey sample and the PUMS universe are
  first recoded and pivoted into incidence tables with identical column
  layouts.  Geography, merges, and crosstabs are applied *after* incidence
  construction, keeping the recode/pivot logic independent of geography.

!!! Algorithm

    Find weight vector **w** closest to seed weights **w₀** (KL-divergence)
    subject to marginal constraints:

    min Σᵢ wᵢ ln(wᵢ / w₀ᵢ)   s.t.  A w = t,  wᵢ ≥ 0

    where **A** is the incidence matrix and **t** is the target totals vector.
    Runs independently per control geography zone (zones are parallelisable).
"""

import logging
from pathlib import Path

import polars as pl

from data_canon.core.dataclass import CanonicalData
from processing.completeness import suggest_usability_columns
from processing.weighting.balancing.balancer import (
    balance_weights,
    grid_search_expansion_factor,
)
from processing.weighting.balancing.base_weights import compute_base_weights
from processing.weighting.balancing.importance import compute_control_moe, compute_moe_importance
from processing.weighting.controls.registry import register_crosstabs_from_config, resolve_targets
from processing.weighting.core.hierarchy import seed_col_for, weight_col_for
from processing.weighting.core.propagation import (
    propagate_weights,
    safe_join_weight,
    seed_admits,
)
from processing.weighting.core.specs import (
    BalancingConfig,
    ControlRegistryConfig,
    ControlTotals,
    ImportanceConfig,
    ProfileFit,
    WeightingConfig,
)
from processing.weighting.data_prep.control_data import (
    recode_pums_households,
    recode_pums_persons,
)
from processing.weighting.data_prep.crosswalk import GeographyConfig, PumaCrosswalk
from processing.weighting.data_prep.fractional_impute import fill_null_incidence
from processing.weighting.data_prep.incidence import (
    IncidenceBundle,
    aggregate_control_totals,
    build_incidence_table,
)
from processing.weighting.data_prep.merges import (
    apply_1d_merges,
    merge_control_totals,
)
from processing.weighting.data_prep.pums_data import (
    PUMSSource,
    fetch_pums_data,
    load_pums_from_files,
)
from processing.weighting.data_prep.seed_data import (
    recode_survey_households,
    recode_survey_persons,
)
from processing.weighting.diagnostics import generate_report
from processing.weighting.validation.checksums import check_incidence_sums
from processing.weighting.validation.control_validation import (
    validate_total_control_categories,
    warn_crosstab_sparsity,
)
from processing.weighting.validation.coverage import check_control_geography_coverage

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
    pipeline = WeightingPipeline(controls=..., config=..., data=...)

    pipeline.setup()
    pipeline.fetch_pums()

    pipeline.recode_and_pivot()
    pipeline.assign_zones()
    pipeline.apply_merges()

    pipeline.aggregate_totals()
    pipeline.resolve_importance()

    pipeline.fit_all()          # one fit per configured profile
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
    """

    # -- Shared state (populated by the phases every fit reads) ---------
    crosswalk: PumaCrosswalk
    pums_hh: pl.DataFrame
    pums_per: pl.DataFrame
    survey_bundle: IncidenceBundle
    pums_bundle: IncidenceBundle
    pums_incidence: pl.DataFrame
    control_totals: ControlTotals
    resolved_importance: dict[str, float]
    control_moe: pl.DataFrame | None

    # -- Per-profile state ----------------------------------------------
    # Each fit keeps its own seed, weights and statuses. Sharing instance
    # attributes for those would leave the last profile's numbers standing as
    # the ones the diagnostics and the checks describe.
    fits: dict[str | None, ProfileFit]

    def __init__(
        self,
        *,
        controls: ControlRegistryConfig,
        config: WeightingConfig,
        data: CanonicalData,
        balancing: BalancingConfig | None = None,
        importance: ImportanceConfig | None = None,
    ) -> None:
        """Initialise with configuration and survey data."""
        if data.households is None or data.persons is None:
            msg = "WeightingPipeline requires at least households and persons tables."
            raise ValueError(msg)

        self.controls = controls
        self.config = config
        self.data = data
        self.balancing = balancing or BalancingConfig()
        self.importance_cfg = importance or ImportanceConfig()

        # Mutable state initialised empty; populated by phases
        self.control_moe = None
        self.fits = {}

    @property
    def cache_dir(self) -> Path | None:
        """Weighting sub-directory of the pipeline cache (computed from config)."""
        return self.config.cache_dir / "weighting" if self.config.cache_dir else None

    # ------------------------------------------------------------------
    # Phase methods
    # ------------------------------------------------------------------

    def setup(self) -> None:
        """Register crosstabs, resolve control instances, build crosswalk."""
        register_crosstabs_from_config(
            [
                {
                    "name": s.name,
                    **({"importance": s.importance} if s.importance is not None else {}),
                    **({"dimensions": s.dimensions} if s.dimensions is not None else {}),
                    **({"merges": s.merges} if s.merges is not None else {}),
                }
                for s in self.controls.specs
            ]
        )
        ctrl_instances = resolve_targets(self.controls.target_names)
        validate_total_control_categories(ctrl_instances)
        logger.info("Controls: %s", self.controls.target_names)

        zone_groups: dict[str, list[str]] | None = self.config.geography.get("zone_groups")

        # Prepare the crosswalk with the full set of zones (including zone groups)
        # Store as an instance attribute for use in zone assignment and diagnostics
        self.crosswalk = PumaCrosswalk(
            GeographyConfig(**self.config.geography),
            state_fips=self.config.state_fips,
            pums_year=self.config.pums_year,
            cache_dir=self.cache_dir,
            zone_groups=zone_groups,
        )

    def fetch_pums(self) -> None:
        """Load PUMS microdata from local files or the Census API."""
        load_reps = self.importance_cfg.moe_based
        if self.config.pums_households is not None and self.config.pums_persons is not None:
            logger.info("Loading PUMS from local files")
            self.pums_hh, self.pums_per = load_pums_from_files(
                self.config.pums_households,
                self.config.pums_persons,
                load_replicate_weights=load_reps,
            )
        else:
            source = PUMSSource(state_fips=self.config.state_fips, pums_year=self.config.pums_year)
            logger.info(
                "Fetching PUMS via Census API: state=%s year=%d",
                self.config.state_fips,
                self.config.pums_year,
            )
            self.pums_hh, self.pums_per = fetch_pums_data(
                source,
                load_replicate_weights=load_reps,
                cache_dir=self.cache_dir,
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
        )
        self.pums_incidence = self.pums_bundle.incidence
        check_incidence_sums(self.pums_incidence, names, source_label="pums")

        # ==============================================================
        # Survey stream — travel-diary seed (may contain nulls)
        # ==============================================================
        # Recode canonical survey HH + person tables through the same
        # control expressions used for PUMS above.
        strict_nulls = self.config.strict_survey_nulls
        hh_recoded = recode_survey_households(
            self.data.households,  # pyright: ignore[reportArgumentType]
            self.data.persons,  # pyright: ignore[reportArgumentType]
            names,
            strict_nulls=strict_nulls,
        )
        per_recoded = recode_survey_persons(self.data.persons, names, strict_nulls=strict_nulls)  # pyright: ignore[reportArgumentType]

        # Pivot into incidence bundle (one row per hh_id). Unfiltered: which
        # households enter a seed is a profile's question, answered per fit in
        # `build_seed`, and one run may build several seeds from this bundle.
        self.survey_bundle = build_incidence_table(hh_recoded, per_recoded, names)

        logger.info(
            "Survey after recoding: %d HHs, %d persons (before any profile gate)",
            self.survey_bundle.household_pivot["h_total"].sum(),
            self.survey_bundle.person_pivot["p_total"].sum(),
        )

    def assign_zones(self) -> None:
        """Point-in-polygon zone assignment + optional block-group assignment.

        Mutates ``self.data.households`` in place (adds geo columns) and
        allocates ``self.pums_incidence`` across zones. Each seed takes its geo
        columns off ``self.data.households`` in `build_seed`, since a seed only
        exists once a profile has been named.
        """
        hh = self.crosswalk.assign_households(self.data.households)  # pyright: ignore[reportArgumentType]
        n_assigned = hh.filter(pl.col("study_geoid").is_not_null()).height
        logger.info("Assigned %d / %d HHs to target zones", n_assigned, len(hh))

        if self.config.sample_plan is not None:
            hh = self.crosswalk.assign_block_groups(hh)
            n_bg = hh.filter(pl.col("bg_geo_id").is_not_null()).height
            logger.info("Assigned %d / %d HHs to block groups", n_bg, len(hh))

        self.data.households = hh
        self.pums_incidence = self.crosswalk.allocate_pums_incidence(
            self.pums_incidence,
        )

    def apply_merges(self) -> None:
        """Apply 1-D merges to the PUMS incidence table.

        Cross-tab merges are applied at registration time (pre-merge into
        the enum), so only 1-D merges need post-pivot application here. The seed
        side is merged in `build_seed`, symmetrically and once per profile.
        """
        if self.controls.merges_1d:
            self.pums_incidence = apply_1d_merges(
                self.pums_incidence,
                self.controls.merges_1d,
            )
            logger.info(
                "Applied %d 1-D merge specs to PUMS; incidence now %d columns",
                len(self.controls.merges_1d),
                len(self.pums_incidence.columns),
            )

    def aggregate_totals(self) -> None:
        """Aggregate PUMS incidence into per-zone control totals."""
        names = self.controls.target_names
        self.control_totals = aggregate_control_totals(
            self.pums_incidence,
            names,
            weight_col="WGTP",
            geo_col="ctrl_geoid",
        )
        if self.controls.merges_1d:
            self.control_totals = merge_control_totals(
                self.control_totals,
                self.controls.merges_1d,
            )
        logger.info(
            "Control totals: %d zones, %d PUMS HHs, %d PUMS persons",
            len(self.control_totals.geo_ids),
            self.control_totals.pums_hh_count,
            self.control_totals.pums_person_count,
        )

    def resolve_importance(self) -> None:
        """Build the final importance dict (MOE-based, explicit overrides, or default)."""
        overrides = dict(self.controls.importance_overrides)

        if self.importance_cfg.moe_based:
            pums_hh_xw, pums_per_xw = self.crosswalk.allocate_pums_weights(
                self.pums_hh,
                self.pums_per,
            )
            moe_importance = compute_moe_importance(
                pums_hh_xw,
                pums_per_xw,
                self.controls.target_names,
            )
            # YAML explicit overrides take precedence over MOE-derived
            moe_importance.update(overrides)
            overrides = moe_importance

            # Per-cell MOE for diagnostics
            self.control_moe = compute_control_moe(
                pums_hh_xw,
                pums_per_xw,
                self.controls.target_names,
            )

        default = self.importance_cfg.default
        full = {name: overrides.get(name, default) for name in self.controls.target_names}
        imp_lines = "\n".join(f"  {k}: {v:.1f}" for k, v in full.items())
        logger.info("Importance weights:\n%s", imp_lines)
        self.resolved_importance = overrides

    def build_seed(self, profile: str | None) -> ProfileFit:
        """Gate, impute, place and merge one profile's seed.

        The profile's own flag decides who enters the seed, so the balancer
        spreads each zone's population over exactly the households that will keep
        a weight. Gating afterwards instead deletes fitted mass that nothing
        re-spreads, households being the hierarchy anchor, and the survey then
        expands to less than the population it was fitted to.

        Args:
            profile: Usability profile to fit, or None for a single un-suffixed
                weight set gated on ``config.usability_flag_col``.

        Returns:
            The seed and its metadata, ready to balance.

        Raises:
            ValueError: If the gating column is absent from households.
        """
        flag = self.config.flag_for(profile)
        names = self.controls.target_names
        households = self.data.households
        if households is None:  # pragma: no cover - guarded in __init__
            msg = "build_seed requires a households table"
            raise ValueError(msg)

        bundle = self.survey_bundle
        n_gated = 0
        if self.config.exclude_incompletes:
            if flag not in households.columns:
                msg = (
                    f"The weighting is gated on {flag!r}, which households does not carry. "
                    f"{suggest_usability_columns(households)}"
                )
                raise ValueError(msg)
            admitted = households.filter(seed_admits(flag)).select("hh_id").to_series()
            n_gated = households.height - admitted.len()
            bundle = bundle.filter_households(admitted)

        logger.info(
            "Seed for %s: %d HHs, %d persons, %d HHs gated out",
            profile or "the survey",
            bundle.household_pivot["h_total"].sum(),
            bundle.person_pivot["p_total"].sum(),
            n_gated,
        )

        # Null imputation -- RF-predicted fractional probabilities. Survey
        # respondents with null demographics get zero incidence from the pivot;
        # the complete PUMS bundle trains the fill.
        pre_imputation = bundle.incidence
        seed, imputation_summary = fill_null_incidence(
            bundle,
            self.pums_bundle,
            names,
            cache_dir=self.cache_dir,
        )
        # After imputation, every control must sum correctly (with tolerance
        # for floating-point fractions introduced by the RF predictions).
        check_incidence_sums(seed, names, source_label="survey", tolerance=0.01)

        # Geography is a property of the household, but how much of a universe it
        # covers is a property of this seed, so it is counted per profile.
        geo_names = ("study_geoid", "ctrl_geoid", "bg_geo_id")
        geo_cols = [c for c in geo_names if c in households.columns]
        seed = seed.join(households.select("hh_id", *geo_cols), on="hh_id", how="left")
        coverage = check_control_geography_coverage(
            seed,
            profile=profile,
            max_unplaceable_share=self.config.max_unplaceable_share,
        )
        if coverage.n_unplaceable:
            # A household in no zone reaches no ZoneInput, so keeping it cannot
            # weight it -- it only inflates the response count its sample segment
            # divides by.
            seed = seed.filter(pl.col("ctrl_geoid").is_not_null())

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
            names,
            geo_col="ctrl_geoid",
            sample_plan=self.config.sample_plan,
            bg_populations=(
                self.crosswalk.block_group_populations if self.config.sample_plan else None
            ),
        )

        imp_cfg = ImportanceConfig(
            explicit=self.resolved_importance,
            moe_based=False,  # already resolved
            default=self.importance_cfg.default,
        )
        fit.weights, fit.statuses = balance_weights(
            fit.seed_incidence,
            self.control_totals,
            names,
            balancing=self.balancing,
            importance=imp_cfg,
        )

        if fit.unconverged_zones:
            logger.error(
                "Balancing failed to converge for %d zones under %s. See logs for details.",
                fit.unconverged_zones,
                fit.profile or "the survey",
            )

        if self.config.expansion_factor_grid:
            fit.grid_results = grid_search_expansion_factor(
                fit.seed_incidence,
                self.control_totals,
                names,
                ef_grid=self.config.expansion_factor_grid,
                selected_ef=self.balancing.max_expansion_factor,
                balancing=self.balancing,
                importance=imp_cfg,
            )

    def generate_diagnostics(self, fit: ProfileFit, output_path: Path | str | None = None) -> None:
        """Write a self-contained interactive HTML diagnostics report for the weighting run.

        The report covers the full weighting pipeline from geographic crosswalk
        through IPF convergence to final weight quality.  It is intended to be
        opened in any browser with no external dependencies (Plotly is either
        bundled or loaded from CDN).

        Report sections
        ---------------
        1. **Crosswalk Map** — choropleth showing how PUMAs overlap the target
           zones, with allocation weights visualised per PUMA-zone pair.
        2. **Convergence & Weight Summary** — per-zone table of convergence
           status, final weight sum, effective sample size (ESS%), and
           coefficient of variation (CV) of the weights.
        3. **Target Fit** — per-zone summary metrics for household- and
           person-level controls: MAPE, 90th-percentile absolute error, and
           maximum absolute error.
        4. **Weight Distribution** — violin / jitter plots of
           ``final_weight / base_weight`` (the expansion factor applied to each
           household) per zone, with printed summary statistics.
        5. **Target Fit (% Error)** — diverging bar charts showing the signed
           percentage error for every control category per zone.  Bars are
           coloured by user-configurable error thresholds.
        6. **Unweighted Cell Counts (Data Sparsity)** — heatmap of raw survey
           seed counts per control category per zone, flagging cells below the
           minimum-count warning threshold.
        7. **Expansion Factor Calibration** *(optional)* — MAPE vs CV scatter
           across the ``expansion_factor_grid`` values, helping identify the
           best trade-off between fit quality and weight spread.  Only included
           when ``expansion_factor_grid`` was set in the weighting config.

        Parameters
        ----------
        fit:
            The completed fit to describe.  Each profile gets its own report.
        output_path:
            Destination for the HTML file.  Accepts a ``Path``, a string
            (including Jinja-rendered template paths from the YAML config), or
            ``None``.  When ``None`` the file is written to
            ``<cache_dir>/diagnostics.html`` (or ``./weighting/diagnostics.html``
            if no cache directory is configured).  With several profiles fitted
            the profile name is inserted into the stem, so one run's reports do
            not overwrite each other.
        """
        if output_path is not None:
            resolved_path = Path(output_path)
        else:
            report_dir = self.cache_dir or Path.cwd() / "weighting"
            resolved_path = report_dir / "diagnostics.html"
        if fit.profile is not None:
            resolved_path = resolved_path.with_name(
                f"{resolved_path.stem}_{fit.profile}{resolved_path.suffix}"
            )
        zone_groups: dict[str, list[str]] | None = self.config.geography.get("zone_groups")
        generate_report(
            seed=fit.seed_incidence,
            weights=fit.weights,
            control_totals=self.control_totals,
            target_names=self.controls.target_names,
            statuses=fit.statuses,
            output_path=resolved_path,
            puma_gdf=self.crosswalk.puma_gdf,
            target_gdf=self.crosswalk.target_gdf,
            crosswalk_df=self.crosswalk.crosswalk_df,
            zone_groups=zone_groups,
            merge_specs=self.controls.all_merges,
            grid_results=fit.grid_results,
            selected_ef=(self.balancing.max_expansion_factor if fit.grid_results else None),
            control_moe=self.control_moe,
            imputation_summary=fit.imputation_summary,
            pums_incidence=self.pums_incidence,
            pre_imputation_incidence=fit.pre_imputation_incidence,
        )

    def propagate_fit(self, fit: ProfileFit) -> None:
        """Attach one fit's weights to households and propagate them down.

        The only place a profile suffix is applied to canonical data: the
        balancer emits the base column name, knowing nothing about profiles, and
        the rename happens here as it is joined on.
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

        # Households the seed rejected never received a balanced weight. Zeroing
        # them on the same expression the seed used is what makes a null mean
        # "no estimate exists" rather than "excluded".
        hh = tables["households"]
        flag = fit.usability_flag_col
        if self.config.exclude_incompletes and hh is not None and flag in hh.columns:
            tables["households"] = hh.with_columns(
                pl.when(seed_admits(flag))
                .then(pl.col(hh_weight_col))
                .otherwise(0.0)
                .alias(hh_weight_col)
            )

        # Propagate the weights through the survey relational structure (HH → PER → DAY → TRIP/TOUR)
        propagate_weights(
            tables,
            has_weight,
            usability_flag_col=flag if self.config.exclude_incompletes else None,
            profile=fit.profile,
        )

        # Write propagated tables back to self.data
        for name, df in tables.items():
            if df is not None:
                setattr(self.data, name, df)

    def fit(self, profile: str | None, *, output_path: str | None = None) -> ProfileFit:
        """Build, balance, propagate and report one profile's weights."""
        fit = self.build_seed(profile)
        self.balance_fit(fit)
        self.propagate_fit(fit)
        self.generate_diagnostics(fit, output_path=output_path)
        self.fits[fit.profile] = fit
        return fit

    def fit_all(self, *, output_path: str | None = None) -> dict[str | None, ProfileFit]:
        """Run one fit per configured profile, then raise if any failed to converge.

        Every profile is attempted and every report written before raising, so a
        convergence failure in one costs a diagnosis of the others rather than
        hiding it.

        Raises:
            RuntimeError: If any profile left zones unconverged.
        """
        for profile in self.config.fitted_profiles:
            self.fit(profile, output_path=output_path)

        failed = {
            fit.profile or "the survey": fit.unconverged_zones
            for fit in self.fits.values()
            if fit.unconverged_zones
        }
        if failed:
            detail = ", ".join(f"{name}: {n} zones" for name, n in failed.items())
            msg = f"Balancing failed to converge ({detail}). See logs and the reports written."
            raise RuntimeError(msg)
        return self.fits
