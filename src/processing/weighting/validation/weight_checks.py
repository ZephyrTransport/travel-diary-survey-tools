"""Post-balancing weight sanity checks.

Compares survey weighted totals against PUMS-derived control targets
and verifies weight consistency across the table hierarchy.
"""

import logging

import polars as pl

from processing.weighting.controls.base import ControlLevel
from processing.weighting.controls.registry import CONTROLS
from processing.weighting.core.hierarchy import (
    LEVELS,
    Flow,
    levels_with_flow,
)
from processing.weighting.core.specs import ControlSpec, ControlTotals

logger = logging.getLogger(__name__)

_HIERARCHY_ABS_TOL = 0.01


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def weight_sanity_checks(
    tables: dict[str, pl.DataFrame],
    control_totals: ControlTotals,
    specs: list[ControlSpec],
    *,
    geo_col: str = "ctrl_geoid",
) -> None:
    """Run weight sanity checks and log a summary report."""
    hh = tables.get("households")
    per = tables.get("persons")
    if hh is None or per is None:
        logger.warning("Skipping weight sanity checks: households or persons table missing")
        return

    hh_ctrl = _first_control_at_level(specs, ControlLevel.HOUSEHOLD)
    per_ctrl = _first_control_at_level(specs, ControlLevel.PERSON)
    totals_df = control_totals.totals

    logger.info("── Weight sanity checks ──")

    if hh_ctrl and "hh_weight" in hh.columns:
        _compare_and_log(
            hh.filter(pl.col("hh_weight").is_not_null()),
            totals_df.filter(pl.col("control_name") == hh_ctrl),
            weight_col="hh_weight",
            base_weight_col="base_weight",
            geo_col=geo_col,
            label="Household",
        )

    if per_ctrl and "person_weight" in per.columns and geo_col in hh.columns:
        per_with_zone = per.join(
            hh.select("hh_id", geo_col, "base_weight"),
            on="hh_id",
            how="left",
        )
        _compare_and_log(
            per_with_zone.filter(pl.col("person_weight").is_not_null()),
            totals_df.filter(pl.col("control_name") == per_ctrl),
            weight_col="person_weight",
            base_weight_col="base_weight",
            geo_col=geo_col,
            label="Person",
        )

    _check_hierarchy(tables)


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------
def _first_control_at_level(
    specs: list[ControlSpec],
    level: ControlLevel,
) -> str | None:
    """Return the name of the first spec matching *level*, or None."""
    for s in specs:
        ctrl = CONTROLS.get(s.name)
        if ctrl is not None and ctrl.level == level:
            return s.name
    return None


def _compare_and_log(
    df: pl.DataFrame,
    target_df: pl.DataFrame,
    *,
    weight_col: str,
    base_weight_col: str,
    geo_col: str,
    label: str,
) -> None:
    """Compare summed survey weights to targets per zone and log results."""
    # Per-zone aggregation
    agg_exprs = [
        pl.col(weight_col).sum().alias("survey_total"),
        pl.col(weight_col).min().alias("wt_min"),
        pl.col(weight_col).max().alias("wt_max"),
        pl.col(weight_col).mean().alias("wt_mean"),
        pl.col(weight_col).median().alias("wt_median"),
        pl.col(base_weight_col).sum().alias("base_total"),
    ]
    survey_by_zone = df.group_by(geo_col).agg(agg_exprs)

    target_by_zone = (
        target_df.group_by("geo_id")
        .agg(pl.col("target_total").sum().alias("target_total"))
        .rename({"geo_id": geo_col})
    )

    comp = survey_by_zone.join(target_by_zone, on=geo_col, how="left").with_columns(
        pl.when(pl.col("target_total") != 0)
        .then((pl.col("survey_total") - pl.col("target_total")) / pl.col("target_total") * 100)
        .otherwise(0.0)
        .alias("pct_diff")
    )

    # Region-level stats
    w = df[weight_col].drop_nulls()
    region_survey = float(comp["survey_total"].sum())
    region_target = float(comp["target_total"].sum() or 0.0)
    region_base = float(comp["base_total"].sum())
    region_pct = (region_survey - region_target) / region_target * 100 if region_target else 0.0

    # Log: weight distribution headline
    logger.info(
        "  %s weights — min=%.1f  max=%.1f  mean=%.1f  median=%.1f",
        label,
        float(w.min()) if len(w) else 0.0,  # pyright: ignore[reportArgumentType]
        float(w.max()) if len(w) else 0.0,  # pyright: ignore[reportArgumentType]
        float(w.mean()) if len(w) else 0.0,  # pyright: ignore[reportArgumentType]
        float(w.median()) if len(w) else 0.0,  # pyright: ignore[reportArgumentType]
    )

    # Build a display table and let Polars handle column alignment
    display = comp.sort(geo_col).select(
        pl.col(geo_col).cast(pl.Utf8).alias("zone"),
        pl.col("base_total").round(0).cast(pl.Int64).alias("base"),
        pl.col("survey_total").round(0).cast(pl.Int64).alias("balanced"),
        (pl.col("target_total").fill_null(0).round(0).cast(pl.Int64)).alias("target"),
        (pl.col("pct_diff").fill_null(0.0).round(2)).alias("diff%"),
        pl.col("wt_min").round(1).alias("min"),
        pl.col("wt_max").round(1).alias("max"),
        pl.col("wt_mean").round(1).alias("mean"),
        pl.col("wt_median").round(1).alias("median"),
    )
    region_row = pl.DataFrame(
        {
            "zone": ["REGION"],
            "base": [round(region_base)],
            "balanced": [round(region_survey)],
            "target": [round(region_target)],
            "diff%": [round(region_pct, 2)],
            "min": [None],
            "max": [None],
            "mean": [None],
            "median": [None],
        }
    ).cast(display.schema)
    display = pl.concat([display, region_row])

    for line in str(display).splitlines():
        logger.info("  %s", line)


# Rows of a failing scope to name in the error before summarising the rest.
_MAX_REPORTED = 5


def _check_hierarchy(
    tables: dict[str, pl.DataFrame], usability_flag_col: str = "model_usable"
) -> None:
    """Verify that children sum to what their parents represent, or raise.

    Each DOWN edge is checked against the identity its rule actually maintains,
    read from the same [`HIERARCHY`]
    [processing.weighting.core.hierarchy.HIERARCHY] the
    propagation walks -- so the check cannot drift from the rule:

    * copy-and-conserve levels: ``sum(child_weight) == sum(parent_weight *
      n_children)`` over each conservation scope.
    * split levels (days): ``sum(child_weight) == parent_weight`` per parent --
      a person's usable days sum to exactly their person weight, the
      average-day convention.

    It is arithmetic we control, so any deviation past floating-point tolerance
    is a bug and fails loudly. Parents where *nothing* was usable have no
    denominator anywhere; they are reported as a shortfall rather than failed
    -- their weight is deliberately unrepresented below, never pooled across
    parents.

    Args:
        tables: Weighted canonical tables.
        usability_flag_col: Flag marking which records may carry weight.

    Raises:
        ValueError: If any scope's children do not sum to what its parents
            represent.
    """
    for level in levels_with_flow(Flow.DOWN):
        parent_name, child_name = level.parent, level.table
        join_key, child_wt = level.key, level.weight_col
        parent = tables.get(parent_name)  # type: ignore[arg-type]
        child = tables.get(child_name)
        if parent is None or child is None:
            continue
        parent_wt = LEVELS[parent_name].weight_col  # type: ignore[index]
        if parent_wt not in parent.columns or child_wt not in child.columns:
            continue

        # Check the identity at the scope the weight was conserved within.
        scope = level.scope if level.scope in child.columns else join_key

        usable = (
            pl.col(usability_flag_col).fill_null(value=False)
            if usability_flag_col in child.columns
            else pl.lit(value=True)
        )
        per_parent = (
            child.filter(pl.col(child_wt).is_not_null())
            .group_by(join_key)
            .agg(
                pl.col(scope).first().alias("_scope"),
                pl.col(child_wt).sum().alias("child_sum"),
                pl.len().alias("n_children"),
                usable.sum().alias("n_usable"),
            )
        )
        # A split level represents each parent once; a copy level once per child.
        expected = pl.col(parent_wt) if level.split else pl.col(parent_wt) * pl.col("n_children")
        merged = (
            parent.filter(pl.col(parent_wt).is_not_null())
            .select(join_key, parent_wt)
            .join(per_parent, on=join_key, how="inner")
            .with_columns(expected.alias("expected"))
        )
        if merged.is_empty():
            logger.info("  Hierarchy %s → %s: OK", parent_name, child_name)
            continue

        if level.split:
            # A split parent is represented once by its children, so a parent
            # carrying weight but owning no child rows at all (e.g. an
            # unsurveyable person has no days) is a genuine shortfall --
            # reported, not failed. Copy levels have nothing to represent.
            rowless = parent.filter(pl.col(parent_wt).fill_null(0.0) > 0).join(
                per_parent.select(join_key), on=join_key, how="anti"
            )
            if rowless.height:
                logger.info(
                    "  Hierarchy %s → %s: %d %s(s) carry weight but have no %s rows "
                    "(%.1f unrepresented below)",
                    parent_name,
                    child_name,
                    rowless.height,
                    join_key,
                    child_name,
                    float(rowless[parent_wt].sum()),
                )

        by_scope = merged.group_by("_scope").agg(
            pl.col("child_sum").sum(),
            pl.col("expected").sum(),
            pl.col("n_usable").sum(),
        )

        # Scopes that kept nothing have no denominator; report, do not fail.
        emptied = by_scope.filter(pl.col("n_usable") == 0)
        checked = by_scope.filter(pl.col("n_usable") > 0)
        if emptied.height:
            logger.info(
                "  Hierarchy %s → %s: %d %s kept no usable %s, leaving %.1f unrepresented",
                parent_name,
                child_name,
                emptied.height,
                scope,
                child_name,
                float(emptied["expected"].sum()),
            )

        failures = checked.filter(
            (pl.col("child_sum") - pl.col("expected")).abs() > _HIERARCHY_ABS_TOL
        )
        if failures.is_empty():
            logger.info("  Hierarchy %s → %s: OK (per %s)", parent_name, child_name, scope)
            continue

        rows = "\n".join(
            f"  {row['_scope']:<16} {row['child_sum']:>14.4f} {row['expected']:>14.4f}"
            for row in failures.head(_MAX_REPORTED).iter_rows(named=True)
        )
        more = (
            f"\n  ... and {failures.height - _MAX_REPORTED} more"
            if failures.height > _MAX_REPORTED
            else ""
        )
        identity = parent_wt if level.split else f"{parent_wt} x n_{child_name}"
        msg = (
            f"Weight cascade broken between {parent_name} and {child_name}: "
            f"{failures.height} {scope}(s) whose {child_wt} does not sum to "
            f"{identity}.\n"
            f"  {scope:<16} {'child_sum':>14} {'expected':>14}\n"
            f"{rows}{more}"
        )
        logger.error(msg)
        raise ValueError(msg)
