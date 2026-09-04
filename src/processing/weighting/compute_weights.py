"""Entry point for the weighting pipeline step.

Orchestrates the full weighting pipeline in the following stages:

**A. [Geography Crosswalk](crosswalk.md)**

1. **Setup** -- register controls from YAML config, build the geographic
   crosswalk (translating between Census PUMAs and the project's custom
   weighting geography using block-group population as the intermediary),
   and prepare the sample plan.

**B. [Control Data Preparation](data_preparation.md)**

2. **PUMS recoding** -- load PUMS 1-year microdata and recode into the
   YAML-configured variable bins used by the controls.
3. **Merges** -- apply any user-specified category merges (global or
   zone-specific) to both controls and the survey incidence table.
4. **Control aggregation** -- apply the crosswalk to PUMS and aggregate
   into marginal control totals per zone.

**C. [Survey Seed Preparation](data_preparation.md)**

5. **Survey recoding** -- recode canonical survey variables into the same
   bin / group categories as the PUMS controls.
6. **Null imputation** -- fill null-induced zeros in the survey incidence
   table with RF-predicted fractional class probabilities.
7. **Zone assignment** -- assign survey households to weighting zones via
   the geographic crosswalk.

**D. [Maximum-Entropy Balancing](balancing.md)**

8. **Importance resolution** -- compute per-control importance weights
   from replicate-weight MOE or explicit YAML overrides.
9. **Balancing** -- build per-household incidence vectors and fit weights
   using PopulationSim's numba-accelerated max-entropy balancer,
   independently per zone.
10. **[Weight propagation](balancing.md)** -- propagate final ``hh_weight`` to all
    canonical tables (persons, days, trips, tours).

**E. [Diagnostics](diagnostics.md) & [Validation](validation.md)**

11. **Diagnostics** -- generate a self-contained interactive HTML report
    with convergence, fit, and weight-quality diagnostics.
12. **Validation** -- run sanity checks on the final weights and control
    totals.  Results are logged as warnings but are not currently included
    in the HTML report — check the pipeline log to review them.

"""

import logging

import polars as pl

from data_canon.core.dataclass import CanonicalData
from pipeline.cache import PipelineCache
from pipeline.decoration import step
from processing.weighting.core.hierarchy import (
    describe_weight_columns,
    seed_col_for,
    weight_col_for,
)
from processing.weighting.core.pipeline import WeightingPipeline
from processing.weighting.core.propagation import drop_unsuffixed_weights
from processing.weighting.core.specs import (
    BalancingConfig,
    ControlRegistryConfig,
    ImportanceConfig,
    WeightingConfig,
)
from processing.weighting.validation.weight_checks import weight_sanity_checks

logger = logging.getLogger(__name__)

# Geography the balancer works from. It rides on ``households`` through the run
# because zone assignment, balancing and diagnostics all read it, then moves to
# its own table before the tables are handed back. Profile-independent: where a
# household is does not depend on which universe was fitted.
_GEOGRAPHY_COLUMNS = ("study_geoid", "ctrl_geoid", "bg_geo_id")


def _split_weighting_columns(
    tables: dict[str, pl.DataFrame],
    profiles: tuple[str | None, ...],
) -> None:
    """Move the weighting's working columns off ``households`` in place.

    Each fit's ``base_weight`` moves and its ``hh_weight`` is copied: the weight
    is the deliverable and stays on ``households``, but repeating it beside the
    seed weight lets an expansion factor be read off one table. The table stays
    one row per household, so several profiles widen it rather than lengthen it.
    """
    hh = tables.get("households")
    if hh is None:
        return

    geography = [c for c in _GEOGRAPHY_COLUMNS if c in hh.columns]
    seed_weights = [col for p in profiles if (col := seed_col_for("base_weight", p)) in hh.columns]
    if not geography and not seed_weights:
        return

    carried = [col for p in profiles if (col := weight_col_for("hh_weight", p)) in hh.columns]
    moved = geography + seed_weights
    tables["household_weights"] = hh.select("hh_id", *moved, *carried)
    tables["households"] = hh.drop(moved)


# ===========================================================================
# Pipeline step entry point
# ===========================================================================


@step()
def compute_weights(  # noqa: PLR0913
    # -- Config params (from YAML) --------------------------------------
    state_fips: str,
    pums_year: int,
    controls: list[dict],
    geography: dict,
    *,
    # -- Existing PUMS files (optional) ---------------------------------
    pums_households: str | None = None,
    pums_persons: str | None = None,
    # -- Sample plan (optional) -----------------------------------------
    sample_plan: str | None = None,
    # -- Pipeline plumbing (auto-injected by @step decorator) -----------
    pipeline_cache: PipelineCache | None = None,
    # -- Importance / MOE -----------------------------------------------
    moe_based_importance: bool = False,
    default_importance: float = 100.0,
    # -- Balancing params -----------------------------------------------
    max_expansion_factor: float = 10.0,
    min_expansion_factor: float = 0.1,
    min_weight: float | None = 1,
    max_weight: float | None = None,
    max_iterations: int = 10_000,
    n_workers: int = 1,
    # -- Diagnostics ----------------------------------------------------
    expansion_factor_grid: list[float] | None = None,
    diagnostics: dict | None = None,
    # -- Validation ------------------------------------------------------
    strict_survey_nulls: bool = False,
    # -- Completeness handling ------------------------------------------
    exclude_incompletes: bool = True,
    usability_flag_col: str | None = None,
    weight_profiles: list[str] | None = None,
    max_unplaceable_share: float = 0.01,
    # -- Canonical tables (auto-injected by pipeline) -------------------
    canonical_data: CanonicalData | None = None,
    households: pl.DataFrame | None = None,
    persons: pl.DataFrame | None = None,
    days: pl.DataFrame | None = None,
    unlinked_trips: pl.DataFrame | None = None,
    linked_trips: pl.DataFrame | None = None,
    joint_trips: pl.DataFrame | None = None,
    tours: pl.DataFrame | None = None,
    joint_tours: pl.DataFrame | None = None,
) -> dict[str, pl.DataFrame]:
    """Compute expansion weights from PUMS controls and propagate to all tables.

    Flat-parameter entry point required by the ``@step()`` decorator
    (YAML → keyword args).  Constructs a
    [`WeightingPipeline`][processing.weighting.core.pipeline.WeightingPipeline];
    Full documentation of the algorithm, configuration, and diagnostics in included
    in that class.
    """
    if households is None or persons is None:
        msg = "Weighting requires at least households and persons tables."
        raise ValueError(msg)

    # Prepare our data for the weighting pipeline
    # We reused the canonical data handler :)
    data = CanonicalData(
        households=households,
        persons=persons,
        days=days,
        unlinked_trips=unlinked_trips,
        linked_trips=linked_trips,
        tours=tours,
        joint_trips=joint_trips,
        joint_tours=joint_tours,
    )
    # Prepare the pipeline configuration objects
    wt_config = WeightingConfig(
        geography=geography,
        state_fips=state_fips,
        pums_year=pums_year,
        pums_households=pums_households,  # Optional local files take precedence over API fetching
        pums_persons=pums_persons,  # Optional local files take precedence over API fetching
        sample_plan=sample_plan,
        cache_dir=pipeline_cache.cache_dir if pipeline_cache else None,
        expansion_factor_grid=expansion_factor_grid,
        strict_survey_nulls=strict_survey_nulls,
        exclude_incompletes=exclude_incompletes,
        usability_flag_col=usability_flag_col,
        weight_profiles=tuple(weight_profiles or ()),
        max_unplaceable_share=max_unplaceable_share,
    )
    # Prepare the balancing configs (max expansion factor, weight bounds, max iterations, etc.)
    balance_cfg = BalancingConfig(
        max_expansion_factor=max_expansion_factor,
        min_expansion_factor=min_expansion_factor,
        min_weight=min_weight,
        max_weight=max_weight,
        max_iterations=max_iterations,
        n_workers=n_workers,
    )
    # Prepare the importance config (MOE-based, explicit overrides, or default).
    importance_cfg = ImportanceConfig(
        moe_based=moe_based_importance,
        default=default_importance,
    )
    # Initialize and run the pipeline with the provided configs and data
    wt_pipeline = WeightingPipeline(
        controls=ControlRegistryConfig.from_yaml(controls),
        config=wt_config,
        data=data,
        balancing=balance_cfg,
        importance=importance_cfg,
    )

    # 1. Setup — register controls, build crosswalk, fetch PUMS
    wt_pipeline.setup()
    wt_pipeline.fetch_pums()

    # 2. Incidence prep — recode both PUMS and survey, pivot, fill nulls
    wt_pipeline.recode_and_pivot()

    # 3. Zone assignment and merges are intertwined — merges may depend on zone groups
    wt_pipeline.assign_zones()

    # 4. Merge controls, must be applied after zone assignment and before total aggregation
    wt_pipeline.apply_merges()

    # 5. Control aggregation
    wt_pipeline.aggregate_totals()
    wt_pipeline.resolve_importance()

    # 6. One fit per profile: balance, propagate, and report each in turn
    profiles = wt_config.fitted_profiles
    logger.info(
        "Weighting: %d balancing run(s), one per profile (%s). Each is fitted to the "
        "controls over its own universe and writes its own columns; nothing is derived "
        "from another fit.",
        len(profiles),
        ", ".join(p or "the whole survey" for p in profiles),
    )
    fits = wt_pipeline.fit_all(output_path=diagnostics.get("output_path") if diagnostics else None)

    # Basic sanity check to ensure weights were propagated to all tables before returning
    result_tables = wt_pipeline.data.as_dict_non_null()
    for fit in fits.values():
        weight_sanity_checks(
            result_tables,
            wt_pipeline.control_totals,
            wt_pipeline.controls.specs,
            fit.usability_flag_col,
            profile=fit.profile,
        )

    drop_unsuffixed_weights(result_tables, profiles)

    # The suffixed names come from config, so they cannot be model fields; without
    # registering them the delivered output would drop them.
    if canonical_data is not None:
        for table, described in describe_weight_columns(profiles).items():
            canonical_data.register_generated_columns(table, described)

    # After the checks, which still read the seed off households.
    _split_weighting_columns(result_tables, profiles)

    return result_tables
