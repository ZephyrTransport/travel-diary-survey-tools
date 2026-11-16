"""Incidence-table construction and control-total aggregation.

Unified operations that apply identically to both survey and PUMS datasets
after recoding:

1. **Pivot helpers** — :func:`pivot_hh_controls` and
   :func:`pivot_person_controls` convert encoded control columns into
   the ``{ctrl}__{member}`` incidence layout at HH and person levels.
2. **Incidence table** — :func:`build_incidence_table` orchestrates the
   helpers into a single household-level table for the balancer.
3. **Aggregate** — :func:`aggregate_control_totals` sums the pivoted
   PUMS incidence into per-zone target totals, households on household
   weights and persons on person weights.

Category merging lives in :mod:`processing.weighting.data_prep.merges`.
"""

import logging
from dataclasses import replace

import polars as pl

from processing.weighting.controls.base import ControlLevel, ControlTarget
from processing.weighting.controls.registry import resolve_targets
from processing.weighting.core.specs import ControlTotals, IncidenceBundle

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Incidence pivot
# ---------------------------------------------------------------------------


def _indicators(code: pl.Expr | pl.Series, ctrl: ControlTarget) -> list:
    """One 0/1 ``{ctrl}__{member}`` column per category of *ctrl*, read off *code*."""
    return [
        (code == value).cast(pl.Int32).fill_null(0).alias(f"{ctrl.name}__{member.lower()}")
        for value, member in ctrl.valid_members
    ]


def control_columns(ctrl: ControlTarget, columns: list[str]) -> dict[str, str]:
    """The incidence columns holding *ctrl*, each mapped to its category name.

    A structural control is one unpivoted column under its own name; any other
    is one ``{ctrl}__{member}`` column per category present, merged ones included.
    """
    if ctrl.structural:
        return {ctrl.name: ctrl.valid_members[0][1].lower()} if ctrl.name in columns else {}
    prefix = f"{ctrl.name}__"
    return {c: c[len(prefix) :] for c in sorted(columns) if c.startswith(prefix)}


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

    result = hh_recoded.select(hh_id_col, *encoded_cols)
    for ctrl in hh_ctrls:
        if ctrl.name in encoded_cols and not ctrl.structural:
            result = result.with_columns(_indicators(pl.col(ctrl.name), ctrl)).drop(ctrl.name)
    return result


def pivot_person_controls(
    per_recoded: pl.DataFrame,
    targets: list[str],
    hh_id_col: str = "hh_id",
    extra_cols: list[str] | None = None,
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
    extra_cols : list[str] | None
        Additional columns from *per_recoded* to carry onto the person pivot
        (e.g. ``["PWGTP", "PUMA"]`` for PUMS). Never aggregated.

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
    base_cols += [c for c in extra_cols or [] if c in per_recoded.columns and c not in base_cols]
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
            indicators = _indicators(per_recoded[ctrl.name], ctrl)
            person_pivot = person_pivot.with_columns(indicators)
            member_cols += [s.name for s in indicators]

    # Aggregate to household level
    agg_exprs = [pl.col(c).sum() for c in member_cols]
    person_agg = person_pivot.group_by(hh_id_col).agg(agg_exprs)

    return person_pivot, person_agg


def pivot_allocated_controls(incidence: pl.DataFrame, targets: list[str]) -> pl.DataFrame:
    """Add the ``{ctrl}__{member}`` columns of controls recoded after allocation.

    *incidence* is the crosswalk-allocated PUMS incidence, one row per record
    and zone; each copy gets its own zone's category.
    """
    for ctrl in resolve_targets(targets, ControlLevel.HOUSEHOLD):
        if not ctrl.pums_after_allocation:
            continue
        incidence = incidence.with_columns(_indicators(ctrl.pums_expr(), ctrl))
    return incidence


def build_incidence_table(
    hh_recoded: pl.DataFrame,
    per_recoded: pl.DataFrame,
    targets: list[str],
    *,
    hh_id_col: str = "hh_id",
    extra_cols: list[str] | None = None,
    person_extra_cols: list[str] | None = None,
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
    person_extra_cols : list[str] | None
        Additional columns from *per_recoded* to carry onto the person pivot
        (e.g. ``["PWGTP", "PUMA"]`` for PUMS).

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
    person_pivot, person_agg = pivot_person_controls(
        per_recoded, targets, hh_id_col, extra_cols=person_extra_cols
    )
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
    pums_person_incidence: pl.DataFrame,
    targets: list[str],
    *,
    geo_col: str = "zone_group_id",
) -> ControlTotals:
    """Aggregate PUMS incidence into per-zone control totals.

    Household targets sum the household incidence times ``WGTP``; person targets
    sum the person incidence (one row per person) times ``PWGTP``. Which weights
    those are is set beforehand by ``pums_weights`` in ``clean_pums``.

    Parameters
    ----------
    pums_incidence : pl.DataFrame
        Zone-allocated PUMS household incidence. Must have *geo_col*, ``WGTP``
        and the household ``{control}__{member}`` / structural columns.
    pums_person_incidence : pl.DataFrame
        Zone-allocated PUMS person incidence. Must have *geo_col*, ``PWGTP``
        and the person ``{control}__{member}`` / structural columns.
    targets : list[str]
        Control registry names.
    geo_col : str
        Geography column (default ``"zone_group_id"``).
    """
    sources = {
        ControlLevel.HOUSEHOLD: (pums_incidence, "WGTP"),
        ControlLevel.PERSON: (pums_person_incidence, "PWGTP"),
    }
    all_totals: list[pl.DataFrame] = []

    for ctrl in resolve_targets(targets):
        source, weight_col = sources[ctrl.level]
        for col, member in control_columns(ctrl, source.columns).items():
            totals = (
                source.group_by(geo_col)
                .agg(
                    (pl.col(col).cast(pl.Float64) * pl.col(weight_col)).sum().alias("target_total")
                )
                .with_columns(
                    pl.col(geo_col).cast(pl.Utf8).alias("zone_group_id"),
                    pl.lit(ctrl.name).alias("control_name"),
                    pl.lit(member).alias("category"),
                )
                .select("zone_group_id", "control_name", "category", "target_total")
            )
            all_totals.append(totals)

    if not all_totals:
        msg = "No control columns found in PUMS incidence table."
        raise ValueError(msg)

    combined = pl.concat(all_totals)
    zone_group_ids = combined["zone_group_id"].unique().sort().to_list()

    return ControlTotals(
        totals=combined,
        pums_hh_count=pums_incidence["SERIALNO"].n_unique(),
        pums_person_count=pums_person_incidence.select("SERIALNO", "SPORDER").n_unique(),
        zone_group_ids=zone_group_ids,
    )


def drop_lone_target_zones(totals: ControlTotals, crosswalk_df: pl.DataFrame) -> ControlTotals:
    """Drop ``h_target_zone`` in zone groups made of one target zone.

    There its target is the zone's household total, already ``h_total``.
    """
    lone = (
        crosswalk_df.group_by("zone_group_id")
        .agg(pl.col("target_zone_id").n_unique())
        .filter(pl.col("target_zone_id") == 1)["zone_group_id"]
    )
    keep = ~(
        (pl.col("control_name") == "h_target_zone") & pl.col("zone_group_id").is_in(lone.implode())
    )
    return replace(totals, totals=totals.totals.filter(keep))
