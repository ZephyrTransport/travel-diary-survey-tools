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
from processing.weighting.core.pipeline import WeightingPipeline
from processing.weighting.core.specs import (
    BalancingConfig,
    ControlRegistryConfig,
    ImportanceConfig,
    WeightingConfig,
)
from processing.weighting.validation.weight_checks import weight_sanity_checks

logger = logging.getLogger(__name__)


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
    usability_flag_col: str = "model_usable",
    # -- Canonical tables (auto-injected by pipeline) -------------------
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

    # 6. Balance + propagate weights
    wt_pipeline.balance()
    wt_pipeline.propagate()

    # 7. Diagnostics
    wt_pipeline.generate_diagnostics(
        output_path=diagnostics.get("output_path") if diagnostics else None
    )

    # Basic sanity check to ensure weights were propagated to all tables before returning
    result_tables = wt_pipeline.data.as_dict_non_null()
    weight_sanity_checks(
        result_tables,
        wt_pipeline.control_totals,
        wt_pipeline.controls.specs,
    )

    return result_tables
