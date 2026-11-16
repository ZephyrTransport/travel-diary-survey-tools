"""The weighting report for one run: where it goes, what it compares, what it says.

One self-contained HTML document per run, written after every profile is fitted.
See ``generate_diagnostics`` for its sections.
"""

from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from processing.weighting.core.specs import (
    DiagnosticsConfig,
    ReferenceWeightSetConfig,
)
from processing.weighting.data_prep.census_geo import get_county_names
from processing.weighting.diagnostics import generate_report
from processing.weighting.diagnostics.comparison import (
    Comparison,
    all_pairs,
    fitted_weight_sets,
    reference_weight_sets,
)

if TYPE_CHECKING:
    from processing.weighting.core.pipeline import WeightingPipeline


def generate_diagnostics(pipeline: "WeightingPipeline", diagnostics: DiagnosticsConfig) -> None:
    """Write one pipeline-contained interactive HTML report for the whole run.

    Covers the weighting from geographic crosswalk through balancer convergence
    to what carries weight at every level below the household. Opens in any
    browser with no external dependencies beyond Plotly, loaded from CDN.

    One document, not one per profile. The crosswalk geometry, control
    totals and PUMS incidence are built once in ``setup()`` however many
    profiles are fitted -- and on a regional run that geometry is the great
    majority of the file -- so writing a report per fit re-embedded the same
    polygons once per profile. Only the balancer's output actually varies,
    and it sits behind a profile toggle.

    Report sections
    ---------------
    1. **Profile Comparison** — one row per fit: seed size, weight sums by
       level, ESS%, CV, MAPE and convergence. The only view that needs every
       fit at once, and so the only one no per-fit report could produce.
    2. **Crosswalk Map** — choropleth showing how PUMAs overlap the target
       zones, with allocation weights per PUMA-zone pair and each profile's
       seed count in the zone tooltip.
    3. **Weight Cascade** — per level, the records each profile keeps,
       every profile side by side; the records the cleaner excluded, by
       reason; and the weight each kept record carries for dropped ones.
    4. **Weight Set Comparer** *(optional)* — any two weight sets compared
       pairwise at any level, this run's fits against each other or against
       sets named in ``diagnostics.compare_weights``. Omitted when there is
       only one set to show.
    5. **Fractional Seed Imputation** *(per profile)* — null rate per
       control in the seed incidence, and the quality of the PUMS-trained
       model that filled it.
    6. **Balancer Performance** *(per profile)* — per-zone convergence,
       target fit (MAPE, P90, Max), CV and ESS%.
    7. **Weight Quality** *(per profile)* — per-zone weight and expansion
       factor statistics, with a chart of each zone's household weights.
    8. **Expansion Factor Calibration** *(per profile, optional)* — MAPE vs
       CV across the ``expansion_factor_grid`` values. Only included when
       the grid was set in the weighting config.
    9. **Target Fit (% Error)** *(per profile)* — diverging bars per control
       category per zone, with PUMS replicate-weight whiskers.
    10. **Unweighted Cell Counts** *(per profile)* — seed counts per control
       category per zone, flagging sparse cells.

    Numbers are assigned to the sections a run actually renders, so an
    optional section that is absent takes its number with it rather than
    leaving a gap.

    Parameters
    ----------
    diagnostics:
        The ``diagnostics`` block of the weighting config.

        ``output_path`` is the destination for the HTML file, a string
        (including template paths from the YAML). No profile suffix: a run
        writes one file however many profiles it fits.

        ``compare_weights`` names weight sets that arrived with the survey,
        to compare this run's fits against.  See
        [`reference_weight_sets`][processing.weighting.diagnostics.comparison
        .reference_weight_sets] for why they are named rather than found.
    """
    generate_report(
        fits=pipeline.fits,
        control_totals=pipeline.control_totals,
        target_names=pipeline.controls.target_names,
        tables=pipeline.data.as_dict(),
        output_path=Path(diagnostics.output_path),
        run_meta=_run_meta(pipeline),
        puma_gdf=pipeline.crosswalk.puma_gdf,
        target_gdf=pipeline.crosswalk.target_gdf,
        crosswalk_df=pipeline.crosswalk.crosswalk_df,
        zone_groups=pipeline.config.geography.zone_groups,
        zone_counties=pipeline.crosswalk.zone_counties(
            get_county_names(
                pipeline.config.state_fips, pipeline.config.pums_year, pipeline.config.census_dir
            )
        ),
        merge_specs=pipeline.controls.merges_1d,
        control_moe=pipeline.control_moe,
        moe_importance=pipeline.moe_importance,
        importance_used=pipeline.importance_used,
        pums_incidence=pipeline.pums_incidence,
        max_expansion_factor=pipeline.balancing.max_expansion_factor,
        comparison=_weight_comparisons(pipeline, diagnostics.compare_weights),
        survey_shares=pipeline.survey_shares,
    )


def _weight_comparisons(
    pipeline: "WeightingPipeline", references: dict[str, ReferenceWeightSetConfig] | None
) -> Comparison:
    """Compare every pair of weight sets this run can see.

    Read before ``drop_unsuffixed_weights`` runs, which is the only moment a
    supplied un-suffixed weight and this run's own output are both on the
    table -- afterwards the bare column is gone precisely so that no reader
    has to guess which of the two it was.
    """
    supplied = {name: ref.model_dump() for name, ref in (references or {}).items()}
    sets = fitted_weight_sets(list(pipeline.fits)) + reference_weight_sets(supplied)
    if len(sets) < 2:  # noqa: PLR2004 - a comparison needs two sides
        return Comparison(sets=sets, pairs=[])
    return Comparison(sets=sets, pairs=all_pairs(pipeline.data.as_dict(), sets))


def _run_meta(pipeline: "WeightingPipeline") -> dict[str, str]:
    """Label/value pairs identifying this run, for the report header.

    Reports get shared around detached from the run that made them, so the
    file has to say which run it describes.
    """
    profiles = ", ".join(pipeline.fits)
    return {
        "Run": datetime.now(tz=UTC).astimezone().strftime("%Y-%m-%d %H:%M"),
        "PUMS": f"{pipeline.config.pums_year} &middot; FIPS {pipeline.config.state_fips}",
        "Profiles": profiles,
        "Expansion bounds": (
            f"{pipeline.balancing.min_expansion_factor:g}&ndash;"
            f"{pipeline.balancing.max_expansion_factor:g}"
        ),
        "Controls": ", ".join(pipeline.controls.target_names),
    }
