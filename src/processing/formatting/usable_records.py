"""Select the records a formatter may see, by reading a verdict rather than making one.

A formatter does not decide which records exist. `cascade_usability` decides
that once, for every consumer, and stamps it as a usability profile; this reads
the profile the config names and keeps what it admits.

That is the whole of it, and the reason is arithmetic rather than tidiness. The
weighting sums over the records a profile admits, so anything the formatter
removed *afterwards* would leave a weight standing for records that are not in
the output. Filtering on the same verdict the weighting used, and checking that
the records kept carry every weight that came in, makes the two agree.

Which records survive is decided by
[`keep_records`][processing.formatting.kept_records.keep_records], which the
weighting also runs, so both read the same answer.

Weights and joint ids are read for the profile this consumer names, so its
universe, its weights and its joint tours are the same universe. Where a record
still carries no weight it is reported, not removed: a zero means its scope
kept no usable record to share the parent's claim among, a null means no
weight was ever estimated for it, and both are recoverable in a way that
deleting the record is not.
"""

import logging

import polars as pl

from data_canon.codebook.tours import TourDataQuality
from processing.formatting.kept_records import (
    GATED_TABLES,
    WEIGHT_COLUMNS,
    keep_records,
    select_profile_columns,
)
from processing.usability import usable_col_for

logger = logging.getLogger(__name__)

# Each table's own id, to name the records a failing check found.
_ID_COLUMNS = {
    "households": "hh_id",
    "persons": "person_id",
    "person_days": "person_day_id",
    "tours": "tour_id",
    "linked_trips": "linked_trip_id",
    "unlinked_trips": "unlinked_trip_id",
    "joint_trips": "joint_trip_id",
    "joint_tours": "joint_tour_id",
}

# Records named in an error before the rest are only counted.
_MAX_NAMED = 10


def _log_ledger(
    excluded: dict[str, int],
    kept: dict[str, pl.DataFrame],
    usability_flag_col: str,
) -> None:
    """Log one row per table: what the profile excluded, and what survived."""
    rows = [name for name in GATED_TABLES if name in kept]
    if not rows:
        return
    lines = [
        f"Records kept for the model, gated on {usability_flag_col}:",
        "",
        f"  {'table':<16}{'not ' + usability_flag_col:>22}{'kept':>12}",
        *(f"  {name:<16}{-excluded.get(name, 0):>22,}{kept[name].height:>12,}" for name in rows),
    ]
    logger.info("\n".join(lines))


def keep_usable(
    tables: dict[str, pl.DataFrame],
    usability_profile: str,
) -> dict[str, pl.DataFrame]:
    """Keep the records *usability_profile* admits, across every table.

    Args:
        tables: Canonical tables, keyed by name.
        usability_profile: The usability profile this consumer reads. It names
            both the verdict to gate on (``usable_<profile>``) and the weights to
            read (``hh_weight_<profile>``), so one setting settles both.

    Returns:
        The same mapping, filtered, with this profile's weights under the base
        column names. Tables absent from the input stay absent.

    Raises:
        ValueError: If a gated table carries no verdict, which means
            `cascade_usability` did not run or stamped a different profile.
    """
    tables = select_profile_columns(tables, usability_profile)
    usability_flag_col = usable_col_for(usability_profile)

    kept, excluded = keep_records(tables, usability_flag_col)
    _check_totals_kept(tables, kept, usability_flag_col)
    _log_ledger(excluded, kept, usability_flag_col)
    _log_exclusion_summary(tables.get("tours"), tables.get("linked_trips"), usability_flag_col)
    _warn_zero_weight(kept, usability_flag_col)
    return kept


def _check_totals_kept(
    before: dict[str, pl.DataFrame],
    kept: dict[str, pl.DataFrame],
    usability_flag_col: str,
) -> None:
    """Raise if any record the gate drops carries weight, naming the records.

    The weighting placed every weight on a record this profile admits, and
    checked so. This is the same identity from the other side, checked record
    by record: anything the gate leaves out must carry nothing. A total alone
    says weight was lost; the ids say where.

    Raises:
        ValueError: Naming the table, how much went missing, and which records.
    """
    for name, col in WEIGHT_COLUMNS.items():
        if name not in before or name not in kept or col not in before[name].columns:
            continue
        dropped = before[name].filter(
            ~pl.col(usability_flag_col).fill_null(value=False) & (pl.col(col).fill_null(0.0) != 0.0)
        )
        if dropped.is_empty():
            continue
        total_in = before[name].select(pl.col(col).sum()).item() or 0.0
        total_out = kept[name].select(pl.col(col).sum()).item() or 0.0
        id_col = _ID_COLUMNS[name]
        msg = (
            f"{name}: {col} sums to {total_in:.4f} on the way in and {total_out:.4f} on "
            f"the records {usability_flag_col} keeps; {total_in - total_out:.4f} sat on "
            f"records this consumer never receives. {dropped.height} {name} not admitted "
            f"by {usability_flag_col} carry {col} (sample {id_col}: "
            f"{dropped[id_col].head(_MAX_NAMED).to_list()}). The weighting and the cascade "
            "disagree on who is in the model."
        )
        logger.error(msg)
        raise ValueError(msg)


def _warn_zero_weight(tables: dict[str, pl.DataFrame], usability_flag_col: str) -> None:
    """Report records this consumer can use that carry no weight.

    Zero and null are different findings and have different fixes, so they are
    reported apart:

    * **zero** -- the record has nothing to expand by. Both weighting steps stop
      the run on a usable record without a weight, so a zero here means the
      weights did not come from either step: supplied some other way, or edited
      after it.
    * **null** -- no weight was ever estimated. Either the target zones could
      not place the household, so it belonged to no zone group, or no
      weighting step ran at all.

    Neither is an error and neither is a drop: expanding to nothing is recoverable,
    deleting the record is not.
    """
    for name, weight_col in WEIGHT_COLUMNS.items():
        df = tables.get(name)
        if df is None or weight_col not in df.columns:
            continue

        n_null = df.filter(pl.col(weight_col).is_null()).height
        n_zero = df.filter(pl.col(weight_col) <= 0).height

        if n_zero:
            logger.warning(
                "%d %s admitted by %s carry a zero %s. They stay in the output with "
                "nothing to expand by. Both weighting steps stop the run on a usable "
                "record without a weight, so these weights came from neither.",
                n_zero,
                name,
                usability_flag_col,
                weight_col,
            )
        if n_null:
            logger.warning(
                "%d %s admitted by %s have no %s at all: either the target zones "
                "could not place the household, so it was in no zone group, or no "
                "weighting step ran.",
                n_null,
                name,
                usability_flag_col,
                weight_col,
            )


def _completeness_split(df: pl.DataFrame) -> tuple[int, int]:
    """Return (already_incomplete, newly_unusable) counts from the ``survey_complete`` flag.

    ``already_incomplete`` are records flagged incomplete (household / person /
    day cascade) — they were already excluded from the weighted model.
    ``newly_unusable`` are otherwise-complete records the profile still rejects.
    When the ``survey_complete`` column is absent (e.g. tests), everything is treated as
    newly unusable.
    """
    total = df.height
    if "survey_complete" not in df.columns:
        return 0, total
    incomplete = df.filter(~pl.col("survey_complete").fill_null(value=False)).height
    return incomplete, total - incomplete


def _exclusion_grid(title: str, df: pl.DataFrame, denom: int) -> list[str]:
    """Build the lines of one grid (tours or member trips).

    ``new %`` is the newly-unusable share of all records — the genuinely new loss.
    """
    header = f"  {title:<14}{'excluded':>9}{'incomplete':>13}{'newly unusable':>16}{'new %':>8}"

    def row(name: str, incomplete: int, newly_unusable: int) -> str:
        total = incomplete + newly_unusable
        pct = newly_unusable * 100 / denom if denom > 0 else 0
        return f"  {name:<14}{total:>9,}{incomplete:>13,}{newly_unusable:>16,}{pct:>7.1f}%"

    lines = [header]
    for label in ("invalid", "partial/open"):
        incomplete, newly_unusable = _completeness_split(df.filter(pl.col("_reason") == label))
        if incomplete + newly_unusable == 0:
            continue
        lines.append(row(label, incomplete, newly_unusable))
    incomplete, newly_unusable = _completeness_split(df)
    lines.append(row("total", incomplete, newly_unusable))
    return lines


def _log_exclusion_summary(
    tours: pl.DataFrame | None,
    linked_trips: pl.DataFrame | None,
    usability_flag_col: str,
) -> None:
    """Log what the profile excluded, split by reason and by how new the loss is.

    Reporting only: the reason labels describe a verdict already reached, they do
    not contribute to it. Splitting already-incomplete from otherwise-complete
    records is what makes the number readable — a large exclusion of records the
    survey never collected is not the same finding as a small one of complete
    records the model cannot use.
    """
    if tours is None or usability_flag_col not in tours.columns:
        return

    excluded = tours.filter(~pl.col(usability_flag_col).fill_null(value=False))
    if excluded.is_empty():
        return

    if "tour_data_quality" in excluded.columns:
        is_valid = pl.col("tour_data_quality") == TourDataQuality.VALID.value
        reason = pl.when(~is_valid).then(pl.lit("invalid")).otherwise(pl.lit("partial/open"))
    else:
        reason = pl.lit("partial/open")
    excluded = excluded.with_columns(reason.alias("_reason"))

    lines = [
        f"Tours and member trips excluded by {usability_flag_col}.",
        '  "incomplete" = survey_complete is false (the survey did not collect the '
        'record fully); "newly unusable" = complete records the profile still rejects.',
        "",
        *_exclusion_grid("TOURS", excluded, tours.height),
    ]
    if linked_trips is not None and "tour_id" in linked_trips.columns:
        excluded_trips = linked_trips.join(
            excluded.select("tour_id", "_reason"), on="tour_id", how="inner"
        )
        lines += ["", *_exclusion_grid("MEMBER TRIPS", excluded_trips, linked_trips.height)]

    logger.info("\n".join(lines))
