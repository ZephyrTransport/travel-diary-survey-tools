"""Incidence-table construction and control-total aggregation.

Unified operations that apply identically to both survey and PUMS datasets
after recoding:

1. **Pivot helpers** — :func:`pivot_hh_controls` and
   :func:`pivot_person_controls` convert encoded control columns into
   the ``{ctrl}__{member}`` incidence layout at HH and person levels.
2. **Incidence table** — :func:`build_incidence_table` orchestrates the
   helpers into a single household-level table for the balancer.
3. **Aggregate** — :func:`aggregate_control_totals` sums the pivoted
   PUMS incidence into per-zone target totals.

Category merging lives in :mod:`processing.weighting.data_prep.merges`.
"""

import logging

import polars as pl

from processing.weighting.controls.base import ControlLevel
from processing.weighting.controls.registry import resolve_targets
from processing.weighting.core.specs import ControlTotals, IncidenceBundle

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Incidence pivot
# ---------------------------------------------------------------------------


def pivot_hh_controls(
    hh_recoded: pl.DataFrame,
    targets: list[str],
    hh_id_col: str = "hh_id",
) -> pl.DataFrame:
    """Pivot recoded household controls into ``{ctrl}__{member}`` indicator columns.

    Each non-structural household control becomes a set of 0/1 columns
    (e.g. ``h_size__size_1``, ``h_income__low``).  Structural controls
    (like ``h_total``) are carried through as-is.

    Parameters
    ----------
    hh_recoded : pl.DataFrame
        Recoded households with encoded control columns.
    targets : list[str]
        Control registry names.
    hh_id_col : str
        Household identifier column.

    Returns:
    -------
    pl.DataFrame
        ``[hh_id_col, h_ctrl__member_1, h_ctrl__member_2, ...]``
    """
    hh_ctrls = resolve_targets(targets, ControlLevel.HOUSEHOLD)
    encoded_cols = [c.name for c in hh_ctrls if c.name in hh_recoded.columns]

    result = hh_recoded.select([hh_id_col])

    if encoded_cols:
        hh_data = hh_recoded.select([hh_id_col, *encoded_cols])
        result = result.join(hh_data, on=hh_id_col, how="left")

        for ctrl in hh_ctrls:
            if ctrl.name not in result.columns or ctrl.structural:
                continue
            for value, member_name in ctrl.valid_members:
                col_name = f"{ctrl.name}__{member_name.lower()}"
                result = result.with_columns(
                    (pl.col(ctrl.name) == value).cast(pl.Int32).fill_null(0).alias(col_name)
                )
            result = result.drop(ctrl.name)

    return result


def pivot_person_controls(
    per_recoded: pl.DataFrame,
    targets: list[str],
    hh_id_col: str = "hh_id",
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Pivot recoded person controls into per-person indicators and per-HH counts.

    Each non-structural person control becomes a set of 0/1 columns per
    person (e.g. ``p_gender__male``).  Structural controls (``p_total``)
    become a literal 1 per person.

    Returns both the per-person pivot (useful for fractional imputation)
    and the per-household aggregation (used in the incidence table).

    Parameters
    ----------
    per_recoded : pl.DataFrame
        Recoded persons with encoded control columns and *hh_id_col*.
    targets : list[str]
        Control registry names.
    hh_id_col : str
        Household identifier column.

    Returns:
    -------
    tuple[pl.DataFrame, pl.DataFrame]
        ``(person_pivot, person_agg)``

        *person_pivot* — one row per person with 0/1 indicator columns.
        *person_agg* — one row per household with integer count columns
        (sum of person_pivot grouped by household).
    """
    p_ctrls = resolve_targets(targets, ControlLevel.PERSON)

    # Determine which person-level ID column is available
    if "person_id" in per_recoded.columns:
        person_id_col = "person_id"
    elif "SPORDER" in per_recoded.columns:
        person_id_col = "SPORDER"
    else:
        person_id_col = None

    base_cols = [hh_id_col]
    if person_id_col and person_id_col not in base_cols:
        base_cols.append(person_id_col)
    person_pivot = per_recoded.select(base_cols)

    member_cols: list[str] = []

    for ctrl in p_ctrls:
        if ctrl.name not in per_recoded.columns:
            continue
        if ctrl.structural:
            # Each person counts as 1; HH aggregation will sum to household size
            person_pivot = person_pivot.with_columns(pl.lit(1).alias(ctrl.name))
            member_cols.append(ctrl.name)
        else:
            for value, member_name in ctrl.valid_members:
                col_name = f"{ctrl.name}__{member_name.lower()}"
                person_pivot = person_pivot.with_columns(
                    (per_recoded[ctrl.name] == value).cast(pl.Int32).fill_null(0).alias(col_name)
                )
                member_cols.append(col_name)

    # Aggregate to household level
    agg_exprs = [pl.col(c).sum() for c in member_cols]
    person_agg = person_pivot.group_by(hh_id_col).agg(agg_exprs)

    return person_pivot, person_agg


def build_incidence_table(
    hh_recoded: pl.DataFrame,
    per_recoded: pl.DataFrame,
    targets: list[str],
    *,
    hh_id_col: str = "hh_id",
    extra_cols: list[str] | None = None,
) -> IncidenceBundle:
    """Build household-level incidence table with fully-pivoted control columns.

    This is the unified pivoter for both survey and PUMS data.  After
    recoding, both datasets share the same control-column schema
    (``h_size``, ``h_income``, ``p_gender``, …).  This function pivots
    those encoded columns into the incidence layout expected by the
    balancer:

    - HH controls as 0/1 indicators: ``h_size__size_1``, …
    - Person controls as per-household counts: ``p_gender__male``, …
    - Structural controls remain unpivoted: ``h_total``, ``p_total``.

    Non-structural controls use the naming ``{control_name}__{member_name}``.

    Parameters
    ----------
    hh_recoded : pl.DataFrame
        Recoded households (survey or PUMS).
    per_recoded : pl.DataFrame
        Recoded persons (survey or PUMS).
    targets : list[str]
        Control registry names used for recoding.
    hh_id_col : str
        Household identifier column (``"hh_id"`` for survey,
        ``"SERIALNO"`` for PUMS).
    extra_cols : list[str] | None
        Additional columns from *hh_recoded* to carry forward
        (e.g. ``["WGTP", "PUMA"]`` for PUMS).

    Returns:
    -------
    IncidenceBundle
        Bundle containing the combined incidence table, per-person pivot,
        and household-only pivot.
    """
    # Base columns (id + extras like WGTP, PUMA)
    base_cols = [hh_id_col]
    for col in extra_cols or []:
        if col in hh_recoded.columns and col not in base_cols:
            base_cols.append(col)
    incidence = hh_recoded.select(base_cols)

    # HH-level controls → 0/1 indicators
    hh_pivot = pivot_hh_controls(hh_recoded, targets, hh_id_col)
    hh_pivot_cols = [c for c in hh_pivot.columns if c != hh_id_col]
    if hh_pivot_cols:
        incidence = incidence.join(hh_pivot, on=hh_id_col, how="left")

    # Person-level controls → per-HH counts
    person_pivot, person_agg = pivot_person_controls(per_recoded, targets, hh_id_col)
    person_agg_cols = [c for c in person_agg.columns if c != hh_id_col]
    if person_agg_cols:
        incidence = incidence.join(person_agg, on=hh_id_col, how="left").with_columns(
            [pl.col(c).fill_null(0) for c in person_agg_cols]
        )

    logger.info(
        "Incidence table: %d households, %d columns", len(incidence), len(incidence.columns)
    )
    return IncidenceBundle(
        incidence=incidence,
        person_pivot=person_pivot,
        household_pivot=hh_pivot,
    )


# ---------------------------------------------------------------------------
# Control-total aggregation from incidence tables
# ---------------------------------------------------------------------------


def aggregate_control_totals(
    pums_incidence: pl.DataFrame,
    targets: list[str],
    *,
    weight_col: str = "WGTP",
    geo_col: str = "ctrl_geoid",
) -> ControlTotals:
    """Aggregate PUMS incidence into per-zone control totals.

    Reads the pivoted ``{control}__{member}`` columns and structural
    columns directly from the incidence table, multiplies each by the
    household weight, and sums by geography.

    Parameters
    ----------
    pums_incidence : pl.DataFrame
        Zone-allocated PUMS incidence table.  Must have *geo_col*,
        *weight_col*, and the ``{control}__{member}`` / structural columns.
    targets : list[str]
        Control registry names.
    weight_col : str
        Weight column (default ``"WGTP"``).
    geo_col : str
        Geography column (default ``"ctrl_geoid"``).
    """
    ctrl_instances = resolve_targets(targets)

    all_totals: list[pl.DataFrame] = []

    for ctrl in ctrl_instances:
        if ctrl.structural:
            col = ctrl.name
            if col not in pums_incidence.columns:
                continue
            totals = (
                pums_incidence.group_by(geo_col)
                .agg(
                    (pl.col(col).cast(pl.Float64) * pl.col(weight_col)).sum().alias("target_total")
                )
                .with_columns(
                    pl.col(geo_col).cast(pl.Utf8).alias("geo_id"),
                    pl.lit(ctrl.name).alias("control_name"),
                    pl.lit(ctrl.valid_members[0][1].lower()).alias("category"),
                )
                .select("geo_id", "control_name", "category", "target_total")
            )
            all_totals.append(totals)
        else:
            prefix = f"{ctrl.name}__"
            ctrl_cols = sorted(c for c in pums_incidence.columns if c.startswith(prefix))
            for col in ctrl_cols:
                member = col[len(prefix) :]
                totals = (
                    pums_incidence.group_by(geo_col)
                    .agg(
                        (pl.col(col).cast(pl.Float64) * pl.col(weight_col))
                        .sum()
                        .alias("target_total")
                    )
                    .with_columns(
                        pl.col(geo_col).cast(pl.Utf8).alias("geo_id"),
                        pl.lit(ctrl.name).alias("control_name"),
                        pl.lit(member).alias("category"),
                    )
                    .select("geo_id", "control_name", "category", "target_total")
                )
                all_totals.append(totals)

    if not all_totals:
        msg = "No control columns found in PUMS incidence table."
        raise ValueError(msg)

    combined = pl.concat(all_totals)
    geo_ids = combined["geo_id"].unique().sort().to_list()

    id_col = "SERIALNO" if "SERIALNO" in pums_incidence.columns else pums_incidence.columns[0]
    pums_hh_count = pums_incidence[id_col].n_unique()
    pums_person_count = 0
    if "p_total" in pums_incidence.columns:
        pums_person_count = int(pums_incidence["p_total"].sum())

    return ControlTotals(
        totals=combined,
        pums_hh_count=pums_hh_count,
        pums_person_count=pums_person_count,
        geo_ids=geo_ids,
    )
