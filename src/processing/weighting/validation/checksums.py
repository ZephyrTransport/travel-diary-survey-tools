"""Incidence-sum checksums for the weighting pipeline.

Two complementary checks share a common reporting backend:

``check_recode_nulls``
    Runs on a recoded DataFrame *before* aggregation.  Detects records
    where a control column evaluated to null (a gap in the recode
    mapping).  Logs a **warning** by default; raises with ``strict=True``.

``check_incidence_sums``
    Runs on the household-level incidence table *after* aggregation (and
    after fractional imputation for survey data).  For each non-structural
    control it verifies that member-column sums match the expected
    structural total (``p_total`` for person controls, ``h_total`` for
    household controls).  Mismatches always raise ``ValueError``.
"""

import logging
from collections.abc import Iterator

import polars as pl

from processing.weighting.controls.base import ControlLevel, ControlTarget
from processing.weighting.controls.registry import CONTROLS

logger = logging.getLogger(__name__)


# Max number of failures to show in logs (with summary count if more)
_MAX_ROWS = 5


# -- shared helpers ------------------------------------------------------------


def _resolve(
    targets: list[str],
    level: ControlLevel | None = None,
) -> Iterator[tuple[str, ControlTarget]]:
    """Yield ``(name, ctrl)`` for known targets, optionally filtered by level."""
    for name in targets:
        ctrl = CONTROLS.get(name)
        if ctrl is not None and (level is None or ctrl.level == level):
            yield name, ctrl


def _emit(
    rows: list[tuple],
    *,
    source_label: str,
    header: str,
    col_header: str,
    row_fmt: str,
    raise_: bool,
    total: int | None = None,
    n_distinct: int | None = None,
) -> None:
    """Format *rows*, log them, and optionally raise ``ValueError``."""
    if not rows:
        return
    lines = [f"{header} ({source_label}):", col_header]
    lines.extend(row_fmt.format(*row) for row in rows[:_MAX_ROWS])
    n = len(rows)
    if n > _MAX_ROWS:
        lines.append(f"  ... and {n - _MAX_ROWS} more")
    # When n_distinct is provided, report unique record count (not pair count)
    count = n_distinct if n_distinct is not None else n
    if total is not None and total > 0:
        pct = count / total * 100
        lines.append(f"\n{count} / {total} ({pct:.1f}%) record(s) with null recode(s).")
    else:
        lines.append(f"\n{count} total failure(s).")
    msg = "\n".join(lines)
    if raise_:
        logger.error(msg)
        raise ValueError(msg)
    logger.warning(msg)


# -- public API ---------------------------------------------


def check_recode_nulls(
    df: pl.DataFrame,
    targets: list[str],
    *,
    level: ControlLevel,
    id_col: str,
    source_label: str,
    strict: bool = False,
) -> None:
    """Check for records whose control recode evaluated to null.

    Args:
        df: Recoded table (households or persons).
        targets: Control registry names to check.
        level: Which level of controls to check (``HOUSEHOLD`` or
            ``PERSON``).
        id_col: Record identifier column (e.g. ``"hh_id"``,
            ``"person_id"``).
        source_label: Human label for log messages (e.g. ``"PUMS"`` or
            ``"survey"``).
        strict: If ``True``, raise ``ValueError`` on null recodes instead
            of just logging a warning.  PUMS recodes should always be
            strict (every person must land in exactly one category);
            survey recodes may tolerate nulls until imputation is
            implemented.
    """
    rows: list[tuple[str | int, str]] = []
    null_ids: set[str | int] = set()
    for name, _ctrl in _resolve(targets, level):
        if name not in df.columns:
            continue
        ids = df.filter(pl.col(name).is_null())[id_col].to_list()
        rows.extend((rid, name) for rid in ids)
        null_ids.update(ids)

    if not rows:
        logger.info(
            "Null recode check (%s, %s): all records classified",
            level.value,
            source_label,
        )
        return

    # Report distinct records, not (record, control) pairs
    _emit(
        rows,
        source_label=source_label,
        header=f"Null recode values ({level.value})",
        col_header=f"  {id_col:<16} {'control':<20}",
        row_fmt="  {:<16} {:<20}",
        raise_=strict,
        total=len(df),
        n_distinct=len(null_ids),
    )


def check_incidence_sums(
    seed: pl.DataFrame,
    targets: list[str],
    *,
    source_label: str,
    tolerance: float = 0.0,
) -> None:
    """Raise on incidence-column mismatches vs structural totals.

    For person-level controls, ``sum({ctrl}__*)`` must equal ``p_total``.
    For household-level controls, ``sum({ctrl}__*)`` must equal ``h_total``
    (always 1).  Both overcounts and undercounts are checked.

    After fractional imputation, values may be non-integer — set
    *tolerance* to a small float (e.g. ``0.01``) to allow for
    floating-point drift.

    Args:
        seed: Household-level incidence table with structural columns
            (``p_total``, ``h_total``) and ``{ctrl}__{member}`` columns.
        targets: Control registry names.
        source_label: Label for log messages.
        tolerance: Maximum acceptable absolute deviation from the
            expected sum.  Use ``0.0`` for exact integer match (PUMS),
            ``0.01`` for post-imputation survey data.

    Raises:
        ValueError: If any household has an incidence-sum mismatch.
    """
    hh_id_col = "hh_id" if "hh_id" in seed.columns else "SERIALNO"
    rows: list[tuple[int | str, str, float, float]] = []

    # --- Person-level controls (expected = p_total) ----------------------
    p_total_available = "p_total" in seed.columns
    for name, _ctrl in _resolve(targets, ControlLevel.PERSON):
        if _ctrl.structural:
            continue
        inc_cols = [c for c in seed.columns if c.startswith(f"{name}__")]
        if not inc_cols:
            continue
        if not p_total_available:
            logger.warning(
                "Skipping person-control checksum for '%s': "
                "p_total column missing from incidence table "
                "(add 'p_total' to targets to enable this check)",
                name,
            )
            continue

        check = seed.select(
            pl.col(hh_id_col),
            pl.sum_horizontal(inc_cols).cast(pl.Float64).alias("actual"),
            pl.col("p_total").cast(pl.Float64).alias("expected"),
        ).filter((pl.col("actual") - pl.col("expected")).abs() > tolerance)

        rows.extend(
            (row[hh_id_col], name, row["actual"], row["expected"])
            for row in check.iter_rows(named=True)
        )

    # --- Household-level controls (expected = 1.0 per HH) ---------------
    for name, _ctrl in _resolve(targets, ControlLevel.HOUSEHOLD):
        if _ctrl.structural:
            continue
        inc_cols = [c for c in seed.columns if c.startswith(f"{name}__")]
        if not inc_cols:
            continue

        check = seed.select(
            pl.col(hh_id_col),
            pl.sum_horizontal(inc_cols).cast(pl.Float64).alias("actual"),
            pl.lit(1.0).alias("expected"),
        ).filter((pl.col("actual") - pl.col("expected")).abs() > tolerance)

        rows.extend(
            (row[hh_id_col], name, row["actual"], row["expected"])
            for row in check.iter_rows(named=True)
        )

    if not rows:
        logger.info(
            "Incidence checksum (%s): all controls pass",
            source_label,
        )
        return

    _emit(
        rows,
        source_label=source_label,
        header="Incidence sum failures",
        col_header=f"  {hh_id_col:<16} {'control':<20} {'actual':>8} {'expected':>8}",
        row_fmt="  {:<16} {:<20} {:>8.2f} {:>8.2f}",
        raise_=True,
    )
