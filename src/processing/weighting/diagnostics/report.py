"""Report orchestration: assemble sections and render the Jinja2 template.

Entry point is [`generate_report`][processing.weighting.diagnostics.report.generate_report],
which collects every fit a run produced, builds Plotly figures and HTML tables via the sibling
modules (``charts``, ``data``, ``tables``), then renders them
into a single ``.html`` file using a bundled Jinja2 template.

# One document per run, not per fit

The crosswalk geometry, the control totals and the PUMS incidence are properties
of the *run*: they are built once in ``setup()`` however many profiles are
fitted. Only the balancer's output varies per profile. Writing one report per
fit therefore re-embedded the same polygons once per profile -- on a Bay Area
run that geometry is 95% of the file -- so the report is assembled once, with
the per-fit sections behind a profile toggle.

What sits where follows from what can be compared on screen:

* **Run-level, always visible** -- the definitions, the profile comparison, the
  map, the weight-cascade tables and the weight set comparer. These are small
  enough to show every profile at once, and a labelled column cannot be misread
  the way a toggled chart can.
* **Per-profile, toggled** -- the per-zone tables and every figure. Three
  profiles of a per-zone fit grid will not fit side by side, so they are
  switched rather than juxtaposed, and each carries its profile in the heading
  so a cropped screenshot still says what it shows.
"""

import json
import logging
from pathlib import Path

import jinja2
import polars as pl
from geopandas import GeoDataFrame

from processing.weighting.core.specs import ControlTotals, ProfileFit

from .charts import (
    crosswalk_figure,
    ef_tradeoff_figure,
    fit_diverging_figure,
    imputation_distribution_figure,
    violins_figure,
)
from .comparison import Comparison, fitted_weight_sets, inheritance, payload
from .data import (
    apply_fit_merges,
    compute_weighted_totals,
    fit_table,
    merge_control_moe,
    profile_summary,
    redistribution,
    split_identity,
    weight_cascade,
    zone_fit_summary,
)
from .glossary import CASCADE, COMPARER, FIT, IMPUTATION, WEIGHTS, glossary_groups, term
from .tables import (
    balancer_performance_table,
    cascade_table,
    coverage_table,
    crosswalk_summary_table,
    imputation_summary_table,
    inheritance_table,
    profile_comparison_table,
    redistribution_table,
    split_identity_table,
    unweighted_cell_counts,
    weight_quality_table,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Template setup
# ---------------------------------------------------------------------------

_TEMPLATE_DIR = Path(__file__).parent
_ENV = jinja2.Environment(
    loader=jinja2.FileSystemLoader(_TEMPLATE_DIR),
    autoescape=False,  # noqa: S701
    undefined=jinja2.StrictUndefined,
)
_ENV.globals["term"] = term
_TEMPLATE = _ENV.get_template("diagnostics_template.html")

# Plotly HTML config: hover tooltips only, no resize/zoom/toolbar.
_PLOTLY_CONFIG: dict = {
    "responsive": False,
    "displayModeBar": False,
    "scrollZoom": False,
}
_PLOTLY_KWARGS: dict = {
    "full_html": False,
    "include_plotlyjs": False,
    "config": _PLOTLY_CONFIG,
}

# Document order for the run-level sections, the ones outside the profile toggle.
_RUN_SECTIONS: tuple[str, ...] = ("comparison", "crosswalk", "cascade", "comparer")

# Document order for the per-profile sections, each paired with the pane field
# whose presence decides whether that pane renders it at all.
_PANE_SECTIONS: tuple[tuple[str, str], ...] = (
    ("imputation", "imputation_table"),
    ("balancer", "balancer_performance_table"),
    ("quality", "weight_quality_table"),
    ("ef_tradeoff", "ef_tradeoff_section"),
    ("fit_bars", "fit_bars_html"),
    ("sparsity", "sparsity_html"),
)


# Section headings, used by both the heading and the sidebar entry for it.
_TITLES: dict[str, str] = {
    "comparison": "Profile Comparison",
    "crosswalk": "Crosswalk Map",
    "cascade": "Weight Cascade",
    "comparer": "Weight Set Comparer",
    "imputation": "Fractional Seed Imputation",
    "balancer": "Balancer Performance",
    "quality": "Weight Quality",
    "ef_tradeoff": "Expansion Factor Calibration",
    "fit_bars": "Target Fit (% Error)",
    "sparsity": "Unweighted Cell Counts",
}

# Which definitions a section needs, so the glossary lists only what is on the page.
_GLOSSARY_FOR: dict[str, str] = {
    "comparison": WEIGHTS,
    "balancer": FIT,
    "imputation": IMPUTATION,
    "cascade": CASCADE,
    "comparer": COMPARER,
}


def _outline(nums: dict[str, int]) -> list[dict]:
    """Sidebar entries for the rendered sections, in document order.

    Per-profile sections are flagged so the sidebar can send a click to whichever
    profile pane is showing, since each pane repeats them.
    """
    per_profile = {key for key, _ in _PANE_SECTIONS}
    return [
        {"key": key, "num": num, "title": _TITLES[key], "per_profile": key in per_profile}
        for key, num in sorted(nums.items(), key=lambda item: item[1])
    ]


def _section_numbers(present: dict[str, bool], panes: list[dict]) -> dict[str, int]:
    """Number the sections this run actually renders, consecutively from 1.

    A section heading carries its number as text, so the numbers have to be
    assigned where the conditions are already known -- here -- rather than
    branched on in the template, where a section that appears under some runs and
    not others makes every later heading a nested conditional.

    Per-profile sections are numbered once for the whole document rather than per
    pane. A number is an address into the report, and an address that moved when
    the reader pressed the profile toggle would be worse than a gap: a section
    absent from one pane keeps its number and that pane simply skips it.

    Args:
        present: Whether each run-level section produced any content.
        panes: The per-profile panes, read for which sections any of them filled.

    Returns:
        Section key to number, holding only the sections that are rendered.
    """
    numbers: dict[str, int] = {}
    for key in _RUN_SECTIONS:
        if present.get(key):
            numbers[key] = len(numbers) + 1
    for key, field in _PANE_SECTIONS:
        if any(pane.get(field) for pane in panes):
            numbers[key] = len(numbers) + 1
    return numbers


def _label(profile: str | None) -> str:
    """How a profile is named to a reader."""
    return profile or "the survey"


def _key(profile: str | None) -> str:
    """How a profile is named in markup, for the toggle to address."""
    return profile or "survey"


def _fit_control_table(
    fit: ProfileFit,
    control_totals: ControlTotals,
    target_names: list[str],
    merge_specs: list | None,
    control_moe: pl.DataFrame | None,
) -> pl.DataFrame:
    """Target-vs-weighted table for one fit, with PUMS MOE joined where available."""
    weighted_totals = compute_weighted_totals(fit.seed_incidence, fit.weights, target_names)
    table = apply_fit_merges(fit_table(control_totals, weighted_totals), merge_specs, target_names)
    if control_moe is not None:
        merged_moe = merge_control_moe(control_moe, merge_specs)
        table = table.join(
            merged_moe.select("geo_id", "control_name", "category", "moe_pct"),
            on=["geo_id", "control_name", "category"],
            how="left",
        )
    return table


def _imputation_blocks(
    fit: ProfileFit,
    target_names: list[str],
    pums_incidence: pl.DataFrame | None,
) -> tuple[str, str]:
    """Fractional seed imputation table and chart for one fit."""
    if not fit.imputation_summary:
        return "", ""
    table_html = imputation_summary_table(fit.imputation_summary)
    filled = [s.control for s in fit.imputation_summary if s.n_null > 0]
    if not filled or pums_incidence is None:
        return table_html, ""
    figure = imputation_distribution_figure(
        fit.seed_incidence,
        pums_incidence,
        target_names,
        filled,
        pre_imputation=fit.pre_imputation_incidence,
    )
    return table_html, figure.to_html(**_PLOTLY_KWARGS)


def _ef_section(fit: ProfileFit, max_expansion_factor: float | None) -> str:
    """Expansion-factor calibration chart, when a grid was searched."""
    if not fit.grid_results or max_expansion_factor is None:
        return ""
    figure = ef_tradeoff_figure(fit.grid_results, max_expansion_factor)
    return f'<div class="chart">{figure.to_html(**_PLOTLY_KWARGS)}</div>'


def _pane(
    fit: ProfileFit,
    control_table: pl.DataFrame,
    control_totals: ControlTotals,
    target_names: list[str],
    merge_specs: list | None,
    pums_incidence: pl.DataFrame | None,
    max_expansion_factor: float | None,
) -> dict:
    """Everything one profile contributes to the document."""
    weighted = fit.seed_incidence.join(
        fit.weights.select("hh_id", "hh_weight"), on="hh_id", how="left"
    )
    imputation_table, imputation_chart = _imputation_blocks(fit, target_names, pums_incidence)
    zone_fit = zone_fit_summary(control_table, target_names)
    return {
        "profile": _label(fit.profile),
        "key": _key(fit.profile),
        "imputation_table": imputation_table,
        "imputation_chart": imputation_chart,
        "balancer_performance_table": balancer_performance_table(fit.statuses, weighted, zone_fit),
        "weight_quality_table": weight_quality_table(weighted),
        "violins_html": violins_figure(weighted).to_html(**_PLOTLY_KWARGS),
        "fit_bars_html": fit_diverging_figure(control_table).to_html(**_PLOTLY_KWARGS),
        "sparsity_html": unweighted_cell_counts(
            fit.seed_incidence, target_names, control_totals, merge_specs
        ),
        "ef_tradeoff_section": _ef_section(fit, max_expansion_factor),
    }


def generate_report(  # noqa: PLR0913
    fits: dict[str | None, ProfileFit],
    control_totals: ControlTotals,
    target_names: list[str],
    tables: dict[str, pl.DataFrame | None],
    output_path: Path,
    *,
    run_meta: dict[str, str] | None = None,
    puma_gdf: GeoDataFrame | None = None,
    target_gdf: GeoDataFrame | None = None,
    crosswalk_df: pl.DataFrame | None = None,
    zone_groups: dict[str, list[str]] | None = None,
    merge_specs: list | None = None,
    control_moe: pl.DataFrame | None = None,
    pums_incidence: pl.DataFrame | None = None,
    max_expansion_factor: float | None = None,
    comparison: Comparison | None = None,
) -> Path:
    """Write the run's self-contained HTML diagnostics report to *output_path*.

    Args:
        fits: Every completed fit in the run, keyed by profile. A run that
            weights one un-profiled set has a single ``None`` key, and the
            toggle collapses to one pane.
        control_totals: The targets every fit was balanced to.
        target_names: Control registry names in the spec.
        tables: The propagated canonical tables, for the weight cascade. Read
            after propagation, so each level carries its own weight column.
        output_path: Destination for the HTML file.
        run_meta: Label/value pairs identifying the run, shown in the header.
        puma_gdf: PUMA boundaries for the crosswalk map.
        target_gdf: Target-zone boundaries for the crosswalk map.
        crosswalk_df: The PUMA-to-zone allocation table.
        zone_groups: Zone groupings, when zones were merged for balancing.
        merge_specs: Category merges, for labelling the fit table.
        control_moe: Per-cell PUMS standard errors from replicate weights.
        pums_incidence: PUMS incidence, as the reference distribution for the
            fractional seed imputation chart.
        max_expansion_factor: The production EF, marked on the calibration
            chart when a grid was searched.
        comparison: Weight sets to compare pairwise. Run-level by construction:
            a comparison names two profiles, so it cannot sit in a pane that
            shows one. Omitted, or holding no pairs, the section is left out.

    Returns:
        The path written.
    """
    if not fits:
        msg = "generate_report needs at least one completed fit"
        raise ValueError(msg)

    control_tables = {
        profile: _fit_control_table(fit, control_totals, target_names, merge_specs, control_moe)
        for profile, fit in fits.items()
    }

    panes = [
        _pane(
            fit,
            control_tables[profile],
            control_totals,
            target_names,
            merge_specs,
            pums_incidence,
            max_expansion_factor,
        )
        for profile, fit in fits.items()
    ]

    # Section 1 — profile comparison, the one view no single fit can produce
    comparison_table = profile_comparison_table(
        [profile_summary(fit, tables, control_tables[profile]) for profile, fit in fits.items()]
    )

    # Section 2 — crosswalk map, drawn once for the run
    crosswalk_section = ""
    if puma_gdf is not None and target_gdf is not None and crosswalk_df is not None:
        figure = crosswalk_figure(
            puma_gdf=puma_gdf,
            target_gdf=target_gdf,
            crosswalk_df=crosswalk_df,
            seeds={profile: fit.seed_incidence for profile, fit in fits.items()},
            zone_groups=zone_groups,
        )
        crosswalk_section = f'<div class="chart-map">{figure.to_html(**_PLOTLY_KWARGS)}</div>'

    first = next(iter(fits.values()))
    crosswalk_table = (
        crosswalk_summary_table(crosswalk_df, first.seed_incidence)
        if crosswalk_df is not None
        else ""
    )

    # Section 3 — the weight cascade, every profile side by side
    cascades = {
        profile: weight_cascade(tables, profile=profile, usability_flag_col=fit.usability_flag_col)
        for profile, fit in fits.items()
    }
    ratios = {
        profile: redistribution(tables, profile=profile, usability_flag_col=fit.usability_flag_col)
        for profile, fit in fits.items()
    }
    splits = {profile: split_identity(tables, profile=profile) for profile in fits}

    # How each set's weight descends, including any supplied sets: it describes
    # each set on its own, so it belongs with the cascade, not the comparer.
    sets = comparison.sets if comparison is not None else fitted_weight_sets(list(fits))
    cascade_blocks = {
        "cascade_table": cascade_table(cascades),
        "redistribution_table": redistribution_table(ratios),
        "inheritance_table": inheritance_table(
            inheritance(tables, sets), {s.name: s.label for s in sets}
        ),
        "split_identity_table": split_identity_table(splits),
        "coverage_table": coverage_table({p: f.coverage for p, f in fits.items()}),
    }

    # Section 4 — the pairwise comparer, embedded as data and drawn in the browser
    comparer = comparison or Comparison(sets=[], pairs=[])

    nums = _section_numbers(
        {
            "comparison": bool(comparison_table),
            "crosswalk": bool(crosswalk_section or crosswalk_table),
            "cascade": any(cascade_blocks.values()),
            "comparer": bool(comparer),
        },
        panes,
    )
    ctx = {
        "title": "Weighting Diagnostics Report",
        "titles": _TITLES,
        "outline": _outline(nums),
        "glossary": glossary_groups({_GLOSSARY_FOR[k] for k in nums if k in _GLOSSARY_FOR}),
        "comparison_json": json.dumps(payload(comparer), separators=(",", ":")),
        "run_meta": run_meta or {},
        "comparison_table": comparison_table,
        "crosswalk_section": crosswalk_section,
        "crosswalk_table": crosswalk_table,
        **cascade_blocks,
        "panes": panes,
        "nums": nums,
    }

    html = _TEMPLATE.render(**ctx)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    logger.info("Diagnostics report written to %s", output_path)
    return output_path
