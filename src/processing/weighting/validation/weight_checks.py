"""Post-balancing weight sanity checks.

Compares survey weighted totals against PUMS-derived control targets
and verifies weight consistency across the table hierarchy.
"""

import logging
from collections.abc import Collection

import polars as pl

from processing.formatting.kept_records import (
    GATED_TABLES,
    keep_records,
    select_profile_columns,
)
from processing.usability import profile_of
from processing.weighting.controls.base import ControlLevel
from processing.weighting.controls.registry import CONTROLS
from processing.weighting.core.hierarchy import (
    LEVELS,
    Agg,
    Flow,
    Level,
    levels_with_flow,
    seed_col_for,
    weight_col_for,
    weight_columns_for,
)
from processing.weighting.core.specs import ControlSpec, ControlTotals

logger = logging.getLogger(__name__)

_HIERARCHY_ABS_TOL = 0.01

# Rows of a failing scope to name in the error before summarising the rest.
_MAX_REPORTED = 5

# The geography column the control totals are keyed on.
_GEO_COL = "zone_group_id"

# A table's total must survive the redistribution and the formatter to this
# relative precision; anything beyond floating-point drift is weight going missing.
_TOTAL_REL_TOL = 1e-6


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def weight_sanity_checks(
    tables: dict[str, pl.DataFrame],
    control_totals: ControlTotals,
    specs: list[ControlSpec],
    usability_flag_col: str,
    *,
    profile: str | None = None,
) -> None:
    """Run weight sanity checks: log the fit to the controls, raise on a broken identity.

    Args:
        tables: Weighted canonical tables.
        control_totals: The targets weights were balanced to.
        specs: Control specifications, for picking a household- and a
            person-level control to compare against.
        usability_flag_col: The flag the weighting was run under. The hierarchy
            identities only hold over the records that carried weight, so the
            checks have to read the same column the propagation did -- checking
            a different universe than was weighted would fail on correct output.
        profile: Usability profile whose columns to check, or None for the
            un-suffixed set. Names the columns only; *usability_flag_col* is
            still what decides the universe.
    """
    hh = tables.get("households")
    per = tables.get("persons")
    if hh is None or per is None:
        logger.warning("Skipping weight sanity checks: households or persons table missing")
        return

    hh_ctrl = _first_control_at_level(specs, ControlLevel.HOUSEHOLD)
    per_ctrl = _first_control_at_level(specs, ControlLevel.PERSON)
    totals_df = control_totals.totals

    hh_weight_col = weight_col_for("hh_weight", profile)
    person_weight_col = weight_col_for("person_weight", profile)
    base_weight_col = seed_col_for("base_weight", profile)

    logger.info("── Weight sanity checks ──%s", f" ({profile})" if profile else "")

    if hh_ctrl and hh_weight_col in hh.columns:
        _compare_and_log(
            hh.filter(pl.col(hh_weight_col).is_not_null()),
            totals_df.filter(pl.col("control_name") == hh_ctrl),
            weight_col=hh_weight_col,
            base_weight_col=base_weight_col,
            label="Household",
        )

    if per_ctrl and person_weight_col in per.columns and _GEO_COL in hh.columns:
        per_with_zone = per.join(
            hh.select("hh_id", _GEO_COL, base_weight_col),
            on="hh_id",
            how="left",
        )
        _compare_and_log(
            per_with_zone.filter(pl.col(person_weight_col).is_not_null()),
            totals_df.filter(pl.col("control_name") == per_ctrl),
            weight_col=person_weight_col,
            base_weight_col=base_weight_col,
            label="Person",
        )

    _check_hierarchy(tables, usability_flag_col, profile)
    _check_joint_sums(tables, usability_flag_col, profile)
    check_weight_placement(tables, usability_flag_col, profile)


def check_weight_placement(
    tables: dict[str, pl.DataFrame],
    usability_flag_col: str,
    profile: str | None = None,
    *,
    supplied_totals: dict[str, float] | None = None,
) -> None:
    """Verify every weight sits on a record its consumer will receive, or raise.

    Four things, each a way weight has gone missing before:

    1. No weight on a record the profile does not admit. The propagation zeroes
       those, so a non-zero here means a table the usability pass never reached.
    2. A weight above 0 on every record the profile does admit. A usable record
       with none (0 or null) would reach the model counting for nothing: a
       supplied weight file that does not cover the profile's universe, or a
       weighting that skipped part of it.
    3. Each supplied table still sums to what was joined, so redistributing
       onto the usable records moved the vendor's total rather than shrinking
       it. Checked only where the caller supplies totals (the vendor path).
    4. What the formatter will keep carries the whole total. [`keep_records`]
       [processing.formatting.kept_records.keep_records] is the formatter's
       own answer, so running it here and comparing totals proves the weighting
       and the formatter agree before any file is written. Skipped only when a
       table present carries no verdict, which is a partial call rather than
       a run.

    Args:
        tables: Weighted canonical tables.
        usability_flag_col: The flag the weighting was gated on.
        profile: Usability profile whose weight columns to read, or None for the
            un-suffixed set.
        supplied_totals: table -> the total that was joined from a weight file,
            for the tables that had one.

    Raises:
        ValueError: On the first table that fails any of the four.
    """
    weights = {
        table: col
        for table, col in weight_columns_for(profile).items()
        if table in tables and col in tables[table].columns
    }
    gated = [t for t in weights if t in GATED_TABLES and usability_flag_col in tables[t].columns]

    for table in gated:
        col = weights[table]
        misplaced = tables[table].filter(
            ~pl.col(usability_flag_col).fill_null(value=False)
            & pl.col(col).is_not_null()
            & (pl.col(col) != 0)
        )
        if misplaced.height:
            msg = (
                f"{misplaced.height} {table} record(s) not admitted by {usability_flag_col} "
                f"carry a non-zero {col} (total {misplaced[col].sum():.4f}). Weight on a "
                "record the consumer never receives is weight the output loses."
            )
            logger.error(msg)
            raise ValueError(msg)
        weightless = tables[table].filter(
            pl.col(usability_flag_col).fill_null(value=False) & ~(pl.col(col).fill_null(0) > 0)
        )
        if weightless.height:
            msg = (
                f"{weightless.height} {table} record(s) admitted by {usability_flag_col} "
                f"carry no {col} (0 or null). A usable record without a weight counts for "
                "nothing in the model; it should be unusable, or be weighted."
            )
            logger.error(msg)
            raise ValueError(msg)

    for table, supplied in (supplied_totals or {}).items():
        if table not in weights:
            continue
        placed = tables[table].select(pl.col(weights[table]).sum()).item() or 0.0
        if abs(placed - supplied) > _TOTAL_REL_TOL * max(abs(supplied), 1.0):
            msg = (
                f"{table}: {weights[table]} sums to {placed:.4f} but {supplied:.4f} was "
                f"supplied. The redistribution onto {usability_flag_col} records did not "
                "keep the total."
            )
            logger.error(msg)
            raise ValueError(msg)

    _check_kept_records_carry_everything(tables, usability_flag_col, weights)
    logger.info(
        "  Weight placement under %s: OK (%s)", usability_flag_col, ", ".join(sorted(weights))
    )


def _check_kept_records_carry_everything(
    tables: dict[str, pl.DataFrame],
    usability_flag_col: str,
    weights: dict[str, str],
) -> None:
    """Run the formatter's own record selection and require no weight left behind.

    Skipped, with a debug line, when a gated table carries no verdict: that is
    a partial call rather than a run.

    Raises:
        ValueError: Naming the table and the weight that would not reach it.
    """
    unflagged = [
        t for t in GATED_TABLES if t in tables and usability_flag_col not in tables[t].columns
    ]
    if unflagged:
        logger.debug(
            "Skipping the kept-records total check: %s carry no %s",
            ", ".join(unflagged),
            usability_flag_col,
        )
        return
    # The formatter reads the profile's own joint ids under the bare names; so
    # must the check, or a trip in a group this profile dissolved looks dangling.
    gated_tables = {t: df for t, df in tables.items() if t in GATED_TABLES}
    named = profile_of(usability_flag_col)
    if named is not None:
        gated_tables = select_profile_columns(
            gated_tables, named, only=("joint_tour_id", "joint_trip_id")
        )
    kept, _ = keep_records(gated_tables, usability_flag_col)
    for table, col in weights.items():
        if table not in kept:
            continue
        before = tables[table].select(pl.col(col).sum()).item() or 0.0
        after = kept[table].select(pl.col(col).sum()).item() or 0.0
        if abs(before - after) > _TOTAL_REL_TOL * max(abs(before), 1.0):
            msg = (
                f"{table}: {col} sums to {before:.4f} but the records a consumer of "
                f"{usability_flag_col} keeps carry {after:.4f}. The weighting put "
                f"{before - after:.4f} on records the formatter will leave out."
            )
            logger.error(msg)
            raise ValueError(msg)


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
    survey_by_zone = df.group_by(_GEO_COL).agg(agg_exprs)
    target_by_zone = target_df.group_by(_GEO_COL).agg(pl.col("target_total").sum())

    comp = survey_by_zone.join(target_by_zone, on=_GEO_COL, how="left").with_columns(
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
        *(float(v) if len(w) else 0.0 for v in (w.min(), w.max(), w.mean(), w.median())),  # pyright: ignore[reportArgumentType]
    )

    # Build a display table and let Polars handle column alignment
    display = comp.sort(_GEO_COL).select(
        pl.col(_GEO_COL).cast(pl.Utf8).alias("zone"),
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


def _failure_table(
    failures: pl.DataFrame, key: str, values: tuple[str, str], labels: tuple[str, str, str]
) -> str:
    """A header, the first ``_MAX_REPORTED`` failing rows aligned under it, then a count."""
    lines = [f"  {labels[0]:<16} {labels[1]:>14} {labels[2]:>14}"]
    lines += [
        f"  {row[key]:<16} {row[values[0]]:>14.4f} {row[values[1]]:>14.4f}"
        for row in failures.head(_MAX_REPORTED).iter_rows(named=True)
    ]
    if failures.height > _MAX_REPORTED:
        lines.append(f"  ... and {failures.height - _MAX_REPORTED} more")
    return "\n".join(lines)


def _check_joint_sums(
    tables: dict[str, pl.DataFrame], usability_flag_col: str, profile: str | None = None
) -> None:
    """Verify each SUM grouping equals its members' combined weight, or raise.

    The joint levels carry person-trips rather than events (see [`Agg.SUM`]
    [processing.weighting.core.hierarchy.Agg]), which buys a
    conservation identity the mean levels cannot have::

        joint_trip_weight == sum(linked_trip_weight of its member trips)

    Unusable groupings are excluded: [`_aggregate_up`]
    [processing.weighting.core.propagation] zeroes them regardless of
    what their members carry, so they are not expected to reconcile.

    Args:
        tables: Weighted canonical tables.
        usability_flag_col: Flag marking which records may carry weight.
        profile: Usability profile whose weight columns to read, or None for the
            un-suffixed set.

    Raises:
        ValueError: If any grouping's weight is not its members' total.
    """
    for level in levels_with_flow(Flow.UP):
        if level.agg is not Agg.SUM:
            continue
        target, source = tables.get(level.table), tables.get(level.parent)  # type: ignore[arg-type]
        if target is None or source is None:
            continue
        src_weight = LEVELS[level.parent].weight_col_for(profile)  # type: ignore[index]
        target_wt = level.weight_col_for(profile)
        if target_wt not in target.columns or src_weight not in source.columns:
            continue
        if level.key not in source.columns:
            continue

        # Summed by the same column the propagation summed by, so a tour that
        # left its joint tour for this profile is not expected in the total.
        member_key = level.member_key_for(usability_flag_col, source.columns)
        members = (
            source.filter(pl.col(member_key).is_not_null() & pl.col(src_weight).is_not_null())
            .group_by(member_key)
            .agg(pl.col(src_weight).sum().alias("member_sum"))
            .rename({member_key: level.key})
        )
        checked = target.filter(pl.col(target_wt).is_not_null())
        if usability_flag_col in checked.columns:
            checked = checked.filter(pl.col(usability_flag_col).fill_null(value=False))

        merged = checked.select(level.key, target_wt).join(members, on=level.key, how="inner")
        if merged.is_empty():
            logger.info("  Joint sum %s → %s: OK (nothing to check)", level.parent, level.table)
            continue

        failures = merged.filter(
            (pl.col(target_wt) - pl.col("member_sum")).abs() > _HIERARCHY_ABS_TOL
        )
        if failures.is_empty():
            logger.info(
                "  Joint sum %s → %s: OK (%d groupings)", level.parent, level.table, merged.height
            )
            continue

        table = _failure_table(
            failures, level.key, (target_wt, "member_sum"), (level.key, target_wt, "members' total")
        )
        msg = (
            f"Joint weight is not its members' total: {failures.height} "
            f"{level.table} record(s) whose {target_wt} does not equal "
            f"sum({src_weight}) over their member {level.parent}.\n{table}"
        )
        logger.error(msg)
        raise ValueError(msg)


def check_persons_carry_their_household(
    tables: dict[str, pl.DataFrame], profile: str | None = None
) -> None:
    """Raise unless household weight x number of persons = the sum of its persons.

    A household stands for every one of its persons, so its persons together
    carry its weight once each. The supplied-weights path keeps a dropped
    person's share inside the household, so the identity is exact; a dropped
    household carries 0, and so must its persons.

    Args:
        tables: Weighted canonical tables.
        profile: Usability profile whose weight columns to read, or None for the
            un-suffixed set.

    Raises:
        ValueError: Naming the households whose persons do not sum to them.
    """
    households, persons = tables.get("households"), tables.get("persons")
    hh_wt = weight_col_for("hh_weight", profile)
    person_wt = weight_col_for("person_weight", profile)
    if households is None or persons is None:
        return
    if hh_wt not in households.columns or person_wt not in persons.columns:
        return

    per_hh = persons.group_by("hh_id").agg(
        pl.col(person_wt).fill_null(0.0).sum().alias("_persons"), pl.len().alias("_n")
    )
    failures = (
        households.select("hh_id", pl.col(hh_wt).fill_null(0.0).alias("_hh"))
        .join(per_hh, on="hh_id", how="left")
        .with_columns(pl.col("_persons", "_n").fill_null(0))
        .filter((pl.col("_persons") - pl.col("_hh") * pl.col("_n")).abs() > _HIERARCHY_ABS_TOL)
        .sort("hh_id")
    )
    if failures.is_empty():
        logger.info(
            "  Persons carry their household (%s): OK (%d households)",
            person_wt,
            households.height,
        )
        return

    msg = (
        f"{failures.height} household(s) whose {hh_wt} x number of persons does not equal "
        f"the sum of their {person_wt}. Sample hh_id: "
        f"{failures['hh_id'].head(_MAX_REPORTED).to_list()}."
    )
    logger.error(msg)
    raise ValueError(msg)


def check_days_carry_their_person(
    tables: dict[str, pl.DataFrame], profile: str | None = None
) -> None:
    """Raise unless every person's days sum to that person's weight.

    The supplied-weights path gives each day its person's final weight, in
    proportion to the supplier's day weights, so the identity is exact and has
    no exemptions: a person carrying weight that no day carries -- no usable
    day the supplier weighted, or no day at all -- is weight the day table has
    lost. Holding it person by person also holds the day total to the person
    total.

    Args:
        tables: Weighted canonical tables.
        profile: Usability profile whose weight columns to read, or None for the
            un-suffixed set.

    Raises:
        ValueError: Naming the persons whose days do not sum to them.
    """
    persons, person_days = tables.get("persons"), tables.get("person_days")
    person_wt = weight_col_for("person_weight", profile)
    day_wt = weight_col_for("person_day_weight", profile)
    if persons is None or person_days is None:
        return
    if person_wt not in persons.columns or day_wt not in person_days.columns:
        return

    per_person = person_days.group_by("person_id").agg(
        pl.col(day_wt).fill_null(0.0).sum().alias("_days")
    )
    merged = persons.select("person_id", pl.col(person_wt).fill_null(0.0).alias("_person")).join(
        per_person, on="person_id", how="full", coalesce=True
    )
    failures = (
        merged.with_columns(pl.col("_person", "_days").fill_null(0.0))
        .filter((pl.col("_days") - pl.col("_person")).abs() > _HIERARCHY_ABS_TOL)
        .sort("person_id")
    )
    if failures.is_empty():
        logger.info("  Days carry their person (%s): OK (%d persons)", day_wt, merged.height)
        return

    msg = (
        f"{failures.height} person(s) whose {day_wt} does not sum to their {person_wt}: "
        f"{float(failures['_person'].sum()):.4f} on those persons, "
        f"{float(failures['_days'].sum()):.4f} on their days. "
        f"Sample person_id: {failures['person_id'].head(_MAX_REPORTED).to_list()}."
    )
    logger.error(msg)
    raise ValueError(msg)


def _check_hierarchy(
    tables: dict[str, pl.DataFrame],
    usability_flag_col: str,
    profile: str | None = None,
    *,
    only: Collection[str] | None = None,
) -> None:
    """Verify that each level's weights sum to what the level above represents, or raise.

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
        profile: Usability profile whose weight columns to read, or None for the
            un-suffixed set.
        only: Child tables whose edge to check; every DOWN edge by default. The
            supplied-weights step passes the levels it derived, since a level
            supplied as a file follows its own rule.

    Raises:
        ValueError: If any scope's children do not sum to what its parents
            represent.
    """
    for level in levels_with_flow(Flow.DOWN):
        if only is not None and level.table not in only:
            continue
        parent_name, child_name = level.parent, level.table
        join_key, child_wt = level.key, level.weight_col_for(profile)
        parent = tables.get(parent_name)  # type: ignore[arg-type]
        child = tables.get(child_name)
        if parent is None or child is None:
            continue
        parent_wt = LEVELS[parent_name].weight_col_for(profile)  # type: ignore[index]
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
            _log_rowless_parents(level, parent, per_parent, parent_wt)

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

        table = _failure_table(
            failures, "_scope", ("child_sum", "expected"), (scope, f"{child_wt} sum", "expected")
        )
        identity = parent_wt if level.split else f"{parent_wt} x n_{child_name}"
        msg = (
            f"Weight cascade broken between {parent_name} and {child_name}: "
            f"{failures.height} {scope}(s) whose {child_wt} does not sum to "
            f"{identity}.\n{table}"
        )
        logger.error(msg)
        raise ValueError(msg)


def _log_rowless_parents(
    level: Level, parent: pl.DataFrame, per_parent: pl.DataFrame, parent_wt: str
) -> None:
    """Log the records above a split level that carry weight but have no rows in it.

    A split level represents each record above it once, so one with weight but
    no rows below (e.g. an unsurveyable person has no days) is a genuine
    shortfall -- reported, not failed. Copy levels have nothing to represent.
    """
    rowless = parent.filter(pl.col(parent_wt).fill_null(0.0) > 0).join(
        per_parent.select(level.key), on=level.key, how="anti"
    )
    if rowless.height:
        logger.info(
            "  Hierarchy %s → %s: %d %s(s) carry weight but have no %s rows "
            "(%.1f unrepresented below)",
            level.parent,
            level.table,
            rowless.height,
            level.key,
            level.table,
            float(rowless[parent_wt].sum()),
        )
