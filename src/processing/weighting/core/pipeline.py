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
from processing.weighting.balancing.balancer import (
    balance_weights,
    grid_search_expansion_factor,
)
from processing.weighting.balancing.base_weights import compute_base_weights
from processing.weighting.balancing.importance import compute_control_moe, compute_moe_importance
from processing.weighting.controls.registry import register_crosstabs_from_config, resolve_targets
from processing.weighting.core.propagation import (
    propagate_weights,
    safe_join_weight,
)
from processing.weighting.core.specs import (
    BalancingConfig,
    ControlRegistryConfig,
    ControlTotals,
    GridPoint,
    ImportanceConfig,
    ImputationSummary,
    WeightingConfig,
    ZoneStatus,
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

    pipeline.balance()
    pipeline.propagate()

    pipeline.generate_diagnostics()
    ```
    """

    # -- Intermediate state (populated by phase methods) ----------------
    crosswalk: PumaCrosswalk
    pums_hh: pl.DataFrame
    pums_per: pl.DataFrame
    survey_bundle: IncidenceBundle
    pums_bundle: IncidenceBundle
    seed_incidence: pl.DataFrame
    pums_incidence: pl.DataFrame
    control_totals: ControlTotals
    resolved_importance: dict[str, float]
    control_moe: pl.DataFrame | None
    weights: pl.DataFrame
    statuses: list[ZoneStatus]
    grid_results: list[GridPoint] | None
    imputation_summary: list[ImputationSummary]

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

        # Mutable state initialised to None; populated by phases
        self.control_moe = None
        self.grid_results = None

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
                                                       ▼  (fill nulls)
                                              seed_incidence (final)
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

        # Pivot into incidence bundle (one row per hh_id)
        self.survey_bundle = build_incidence_table(hh_recoded, per_recoded, names)

        # Drop incomplete households from the weighting seed (filter all bundle
        # tables). When excluded, they are re-added with zero weight during
        # propagate(); the balancer distributes each zone's population across the
        # surviving complete respondents, so the weighted totals still sum to the
        # control totals. When exclude_incompletes is False they stay in the seed
        # and receive computed weights.
        n_incompletes = 0
        if self.config.exclude_incompletes:
            complete_hh_ids = (
                self.data.households.filter(pl.col("complete"))  # pyright: ignore[reportOptionalMemberAccess]
                .select("hh_id")
                .to_series()
            )
            n_incompletes = self.data.households.height - complete_hh_ids.len()  # pyright: ignore[reportOptionalMemberAccess]
            self.survey_bundle = self.survey_bundle.filter_households(complete_hh_ids)

        # Report survey content after recoding and filtering (before imputation)
        n_survey_hh = self.survey_bundle.household_pivot["h_total"].sum()
        n_survey_per = self.survey_bundle.person_pivot["p_total"].sum()

        logger.info(
            "Survey after recoding (exclude_incompletes=%s): "
            "%d HHs, %d persons, %d incomplete HHs dropped from the seed",
            self.config.exclude_incompletes,
            n_survey_hh,
            n_survey_per,
            n_incompletes,
        )

        # ==============================================================
        # Null imputation — RF-predicted fractional probabilities
        # ==============================================================
        # Survey respondents with null demographics get zero incidence
        # from the pivot.  Use the complete PUMS bundle as training data
        # to predict class-membership probabilities and fill those zeros
        # with fractional values (predict_proba).
        self.pre_imputation_incidence = self.survey_bundle.incidence
        self.seed_incidence, self.imputation_summary = fill_null_incidence(
            self.survey_bundle,
            self.pums_bundle,
            names,
            cache_dir=self.cache_dir,
        )
        # After imputation, every control must sum correctly (with tolerance
        # for floating-point fractions introduced by the RF predictions).
        check_incidence_sums(self.seed_incidence, names, source_label="survey", tolerance=0.01)

    def assign_zones(self) -> None:
        """Point-in-polygon zone assignment + optional block-group assignment.

        Mutates ``self.data.households`` in place (adds geo columns) and
        updates ``self.seed_incidence`` and ``self.pums_incidence`` with
        zone info.
        """
        hh = self.crosswalk.assign_households(self.data.households)  # pyright: ignore[reportArgumentType]
        n_assigned = hh.filter(pl.col("study_geoid").is_not_null()).height
        logger.info("Assigned %d / %d HHs to target zones", n_assigned, len(hh))

        if self.config.sample_plan is not None:
            hh = self.crosswalk.assign_block_groups(hh)
            n_bg = hh.filter(pl.col("bg_geo_id").is_not_null()).height
            logger.info("Assigned %d / %d HHs to block groups", n_bg, len(hh))

        self.data.households = hh

        hh_join_cols = ["hh_id", "study_geoid", "ctrl_geoid"]
        if "bg_geo_id" in hh.columns:
            hh_join_cols.append("bg_geo_id")

        self.seed_incidence = self.seed_incidence.join(
            hh.select(hh_join_cols),
            on="hh_id",
            how="left",
        )
        self.pums_incidence = self.crosswalk.allocate_pums_incidence(
            self.pums_incidence,
        )

    def apply_merges(self) -> None:
        """Apply 1-D merges symmetrically to both incidence tables.

        Cross-tab merges are applied at registration time (pre-merge into
        the enum), so only 1-D merges need post-pivot application here.
        """
        if self.controls.merges_1d:
            self.seed_incidence = apply_1d_merges(
                self.seed_incidence,
                self.controls.merges_1d,
            )
            self.pums_incidence = apply_1d_merges(
                self.pums_incidence,
                self.controls.merges_1d,
            )
            logger.info(
                "Applied %d 1-D merge specs; incidence now %d columns",
                len(self.controls.merges_1d),
                len(self.seed_incidence.columns),
            )

        # Check for sparse cross-tab cells that may cause balancing issues
        ctrl_instances = resolve_targets(self.controls.target_names)
        warn_crosstab_sparsity(self.seed_incidence, ctrl_instances)

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

    def balance(self) -> None:
        """Compute base weights, run max-entropy balancing, optional grid search."""
        names = self.controls.target_names
        self.seed_incidence = compute_base_weights(
            self.seed_incidence,
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
        self.weights, self.statuses = balance_weights(
            self.seed_incidence,
            self.control_totals,
            names,
            balancing=self.balancing,
            importance=imp_cfg,
        )

        n_failed = sum(not s.converged for s in self.statuses)
        if n_failed:
            msg = f"Balancing failed to converge for {n_failed} zones.  See logs for details."
            raise RuntimeError(msg)

        if self.config.expansion_factor_grid:
            self.grid_results = grid_search_expansion_factor(
                self.seed_incidence,
                self.control_totals,
                names,
                ef_grid=self.config.expansion_factor_grid,
                selected_ef=self.balancing.max_expansion_factor,
                balancing=self.balancing,
                importance=imp_cfg,
            )

    def generate_diagnostics(self, output_path: Path | str | None = None) -> None:
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
        output_path:
            Destination for the HTML file.  Accepts a ``Path``, a string
            (including Jinja-rendered template paths from the YAML config), or
            ``None``.  When ``None`` the file is written to
            ``<cache_dir>/diagnostics.html`` (or ``./weighting/diagnostics.html``
            if no cache directory is configured).
        """
        if output_path is not None:
            resolved_path = Path(output_path)
        else:
            report_dir = self.cache_dir or Path.cwd() / "weighting"
            resolved_path = report_dir / "diagnostics.html"
        zone_groups: dict[str, list[str]] | None = self.config.geography.get("zone_groups")
        generate_report(
            seed=self.seed_incidence,
            weights=self.weights,
            control_totals=self.control_totals,
            target_names=self.controls.target_names,
            statuses=self.statuses,
            output_path=resolved_path,
            puma_gdf=self.crosswalk.puma_gdf,
            target_gdf=self.crosswalk.target_gdf,
            crosswalk_df=self.crosswalk.crosswalk_df,
            zone_groups=zone_groups,
            merge_specs=self.controls.all_merges,
            grid_results=self.grid_results,
            selected_ef=(self.balancing.max_expansion_factor if self.grid_results else None),
            control_moe=self.control_moe,
            imputation_summary=self.imputation_summary,
            pums_incidence=self.pums_incidence,
            pre_imputation_incidence=self.pre_imputation_incidence,
        )

    def propagate(self) -> None:
        """Attach weights to households and propagate to all tables on ``self.data``."""
        self.data.households = safe_join_weight(
            self.data.households,  # pyright: ignore[reportArgumentType]
            self.weights.select("hh_id", "hh_weight"),
            "hh_id",
        )
        self.data.households = self.data.households.join(
            self.seed_incidence.select("hh_id", "base_weight"),
            on="hh_id",
            how="left",
        )
        tables = self.data.as_dict()
        has_weight: dict[str, str] = {"households": "hh_weight"}

        # Usability is a flag stamped upstream by the ``cascade_completeness``
        # step -- the profile this project named. Nothing is re-derived here.
        usability_flag_col = self.config.usability_flag_col

        # Unusable households were held out of the seed, so they never received a
        # balanced weight; zero them so the join leaves nothing behind.
        hh = tables["households"]
        if self.config.exclude_incompletes and hh is not None and usability_flag_col in hh.columns:
            tables["households"] = hh.with_columns(
                pl.when(pl.col(usability_flag_col).fill_null(value=False))
                .then(pl.col("hh_weight"))
                .otherwise(0.0)
                .alias("hh_weight")
            )

        # Propagate the weights through the survey relational structure (HH → PER → DAY → TRIP/TOUR)
        propagate_weights(
            tables,
            has_weight,
            usability_flag_col=usability_flag_col if self.config.exclude_incompletes else None,
        )

        # Write propagated tables back to self.data
        for name, df in tables.items():
            if df is not None:
                setattr(self.data, name, df)
