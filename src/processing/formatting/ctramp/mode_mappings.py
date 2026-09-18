"""CT-RAMP mode and transit-submode mappings."""

import polars as pl

from data_canon.codebook.ctramp import CTRAMPModeType
from data_canon.codebook.trips import AccessEgressMode, Mode, ModeType, TNCType

# Transit submode ranks (ordered by CT-RAMP hierarchy: COM > HVY > EXP > LRF > LOC)
# When a transit tour/trip uses multiple submodes, the highest-ranked submode wins.
TRANSIT_SUBMODE_NONE = 0
TRANSIT_SUBMODE_LOCAL = 1
TRANSIT_SUBMODE_LRF = 2  # light rail / ferry
TRANSIT_SUBMODE_EXPRESS = 3
TRANSIT_SUBMODE_HEAVY = 4
TRANSIT_SUBMODE_COMMUTER = 5

# Detailed Mode -> transit submode rank. Modes not listed have no transit submode.
MODE_TO_TRANSIT_SUBMODE = {
    # Local bus
    Mode.BUS_LOCAL.value: TRANSIT_SUBMODE_LOCAL,
    Mode.BUS_LOCAL_PUBLIC.value: TRANSIT_SUBMODE_LOCAL,
    Mode.BUS_OTHER.value: TRANSIT_SUBMODE_LOCAL,
    Mode.PARATRANSIT.value: TRANSIT_SUBMODE_LOCAL,
    # Light rail / streetcar / ferry
    Mode.LIGHT_RAIL.value: TRANSIT_SUBMODE_LRF,
    Mode.MUNI_METRO.value: TRANSIT_SUBMODE_LRF,
    Mode.STREETCAR.value: TRANSIT_SUBMODE_LRF,
    Mode.FERRY.value: TRANSIT_SUBMODE_LRF,
    Mode.WATER.value: TRANSIT_SUBMODE_LRF,
    Mode.BOAT.value: TRANSIT_SUBMODE_LRF,
    # Express / bus rapid transit
    Mode.BUS_EXPRESS.value: TRANSIT_SUBMODE_EXPRESS,
    Mode.BUS_BRT.value: TRANSIT_SUBMODE_EXPRESS,
    # Heavy rail
    Mode.BART.value: TRANSIT_SUBMODE_HEAVY,
    # Commuter rail
    Mode.RAIL.value: TRANSIT_SUBMODE_COMMUTER,
    Mode.RAIL_OTHER.value: TRANSIT_SUBMODE_COMMUTER,
    Mode.RAIL_INTERCITY.value: TRANSIT_SUBMODE_COMMUTER,
}


def aggregate_transit_submode(unlinked_trips: pl.DataFrame, group_col: str) -> pl.DataFrame:
    """Aggregate the highest transit submode and the TNC type within each group.

    Uses the detailed ``mode_1``-``mode_4`` columns on unlinked trips to detect
    the transit submode (local bus, light rail/ferry, express bus, heavy rail,
    commuter rail) and returns the highest-ranked submode present per group.
    Also aggregates ``tnc_type`` across the group's TNC segments: ``POOLED`` wins
    if any TNC segment is pooled, otherwise the lowest ``tnc_type`` present
    (``null`` if the group has no TNC segments).

    Args:
        unlinked_trips: Canonical unlinked trips DataFrame containing ``group_col``,
            the detailed ``mode_1``-``mode_4`` columns, and optionally ``mode_type``
            and ``tnc_type``.
        group_col: Column to group by (e.g. ``tour_id`` or ``joint_tour_id``).

    Returns:
        DataFrame with columns ``[group_col, transit_submode, tnc_type]`` where
        ``transit_submode`` is the highest submode rank (0 if none) and
        ``tnc_type`` is the aggregated TNC service type (null if no TNC segments).
    """
    mode_cols = [c for c in ("mode_1", "mode_2", "mode_3", "mode_4") if c in unlinked_trips.columns]
    if not mode_cols or group_col not in unlinked_trips.columns:
        return pl.DataFrame(
            {group_col: [], "transit_submode": [], "tnc_type": []},
            schema={
                group_col: unlinked_trips.schema.get(group_col, pl.Int64),
                "transit_submode": pl.Int64,
                "tnc_type": pl.Int64,
            },
        )

    submode = (
        unlinked_trips.select([group_col, *mode_cols])
        .filter(pl.col(group_col).is_not_null())
        .unpivot(index=group_col, on=mode_cols, variable_name="_mode_slot", value_name="_mode")
        .drop_nulls("_mode")
        .with_columns(
            pl.col("_mode")
            .replace_strict(MODE_TO_TRANSIT_SUBMODE, default=TRANSIT_SUBMODE_NONE)
            .alias("_submode_rank")
        )
        .group_by(group_col)
        .agg(pl.col("_submode_rank").max().alias("transit_submode"))
    )

    # Aggregate TNC type across the group's TNC segments (POOLED wins, else min).
    if "tnc_type" in unlinked_trips.columns and "mode_type" in unlinked_trips.columns:
        tnc = (
            unlinked_trips.filter(
                pl.col(group_col).is_not_null() & (pl.col("mode_type") == ModeType.TNC.value)
            )
            .group_by(group_col)
            .agg(
                pl.when((pl.col("tnc_type") == TNCType.POOLED.value).any())
                .then(pl.lit(TNCType.POOLED.value))
                .otherwise(pl.col("tnc_type").min())
                .alias("tnc_type")
            )
        )
        submode = submode.join(tnc, on=group_col, how="full", coalesce=True)
    else:
        submode = submode.with_columns(pl.lit(None, dtype=pl.Int64).alias("tnc_type"))

    return submode


def ctramp_mode_expression(
    mode_type: pl.Expr,
    num_travelers: pl.Expr,
    access_mode: pl.Expr | None = None,
    egress_mode: pl.Expr | None = None,
    transit_submode: pl.Expr | None = None,
    tnc_type: pl.Expr | None = None,
) -> pl.Expr:
    """Map canonical mode_type to CTRAMP mode integer code.

    Args:
        mode_type: Polars expression for canonical mode_type
            (from ModeType enum)
        num_travelers: Polars expression for number of travelers in vehicle
        access_mode: Optional polars expression for access mode (AccessEgressMode enum)
        egress_mode: Optional polars expression for egress mode (AccessEgressMode enum)
        transit_submode: Optional polars expression for the transit submode rank
            (see ``TRANSIT_SUBMODE_*`` and :func:`aggregate_transit_submode`). When
            provided, transit trips are mapped to the matching CT-RAMP submode code
            (local/express/light rail-ferry/heavy rail/commuter rail) instead of
            always defaulting to local bus.
        tnc_type: Optional polars expression for determining TNC type

    Returns:
        Polars expression resolving to CTRAMPModeType integer code (21 codes)

    Notes:
        - Walk=7, Bike=8
        - Transit walk-access codes: LOC=9, LRF=10, EXP=11, HVY=12, COM=13
        - Transit drive-access codes: LOC=14, LRF=15, EXP=16, HVY=17, COM=18
          Uses access_mode/egress_mode to detect drive-to-transit and
          transit_submode to pick the submode; defaults to local bus.
        - Personal vehicle by occupancy: DA=1, SR2=3, SR3=5 (non-toll)
        - TNC by service type: Regualr/Premium =20, Shared=21
        - Taxi=19
        - School bus treated as SR3=5
        - Unknown modes default to DA=1
    """
    # Walk mode
    walk_expr = pl.when(mode_type == ModeType.WALK.value).then(pl.lit(CTRAMPModeType.WALK.value))

    # Bike and micromobility modes
    bike_modes = [
        ModeType.BIKE.value,
        ModeType.BIKESHARE.value,
        ModeType.SCOOTERSHARE.value,
    ]
    bike_expr = walk_expr.when(mode_type.is_in(bike_modes)).then(pl.lit(CTRAMPModeType.BIKE.value))

    # Transit modes - check for drive-to-transit via access/egress modes
    # Default to walk-local bus-walk (WLK_LOC_WLK=9)
    # If drove to transit (access or egress by car), use DRV_LOC_WLK=14
    transit_modes = [
        ModeType.TRANSIT.value,
        ModeType.FERRY.value,
        ModeType.SHUTTLE.value,
    ]
    # Define drive access/egress modes from canonical AccessEgressMode enum
    drove_access_egress = [
        AccessEgressMode.TNC.value,
        AccessEgressMode.CAR_HOUSEHOLD.value,
        AccessEgressMode.CAR_OTHER.value,
        AccessEgressMode.DROPOFF_HOUSEHOLD.value,
        AccessEgressMode.DROPOFF_OTHER.value,
    ]

    if access_mode is not None and egress_mode is not None:
        # Check if either access or egress involved driving
        drove_to_transit = access_mode.is_in(drove_access_egress) | egress_mode.is_in(
            drove_access_egress
        )
    else:
        drove_to_transit = None

    # Pick the walk-access and drive-access transit codes based on submode rank.
    # When no submode is available, default to local bus (WLK_LOC_WLK / DRV_LOC_WLK).
    if transit_submode is not None:
        walk_transit_code = (
            pl.when(transit_submode == TRANSIT_SUBMODE_COMMUTER)
            .then(pl.lit(CTRAMPModeType.WLK_COM_WLK.value))
            .when(transit_submode == TRANSIT_SUBMODE_HEAVY)
            .then(pl.lit(CTRAMPModeType.WLK_HVY_WLK.value))
            .when(transit_submode == TRANSIT_SUBMODE_LRF)
            .then(pl.lit(CTRAMPModeType.WLK_LRF_WLK.value))
            .when(transit_submode == TRANSIT_SUBMODE_EXPRESS)
            .then(pl.lit(CTRAMPModeType.WLK_EXP_WLK.value))
            .otherwise(pl.lit(CTRAMPModeType.WLK_LOC_WLK.value))
        )
        drive_transit_code = (
            pl.when(transit_submode == TRANSIT_SUBMODE_COMMUTER)
            .then(pl.lit(CTRAMPModeType.DRV_COM_WLK.value))
            .when(transit_submode == TRANSIT_SUBMODE_HEAVY)
            .then(pl.lit(CTRAMPModeType.DRV_HVY_WLK.value))
            .when(transit_submode == TRANSIT_SUBMODE_LRF)
            .then(pl.lit(CTRAMPModeType.DRV_LRF_WLK.value))
            .when(transit_submode == TRANSIT_SUBMODE_EXPRESS)
            .then(pl.lit(CTRAMPModeType.DRV_EXP_WLK.value))
            .otherwise(pl.lit(CTRAMPModeType.DRV_LOC_WLK.value))
        )
    else:
        walk_transit_code = pl.lit(CTRAMPModeType.WLK_LOC_WLK.value)
        drive_transit_code = pl.lit(CTRAMPModeType.DRV_LOC_WLK.value)

    if drove_to_transit is not None:
        transit_mode_code = (
            pl.when(drove_to_transit).then(drive_transit_code).otherwise(walk_transit_code)
        )
    else:
        # No access/egress info available, default to walk-to-transit
        transit_mode_code = walk_transit_code

    transit_expr = bike_expr.when(mode_type.is_in(transit_modes)).then(transit_mode_code)

    # School bus - treat as SR3
    school_bus_expr = transit_expr.when(mode_type == ModeType.SCHOOL_BUS.value).then(
        pl.lit(CTRAMPModeType.SR3.value)
    )

    # Taxi - specific code
    taxi_expr = school_bus_expr.when(mode_type == ModeType.TAXI.value).then(
        pl.lit(CTRAMPModeType.TAXI.value)
    )

    # TNC - distinguish between single (TNC=20) and shared (TNC2=21)
    tnc_occupancy = (
        pl.when(tnc_type == TNCType.POOLED.value)
        .then(pl.lit(CTRAMPModeType.TNC2.value))
        .otherwise(pl.lit(CTRAMPModeType.TNC.value))
    )
    tnc_expr = taxi_expr.when(mode_type == ModeType.TNC.value).then(tnc_occupancy)

    # Personal vehicle (CAR, CARSHARE) - distinguish by occupancy (non-toll)
    auto_modes = [
        ModeType.CAR.value,
        ModeType.CARSHARE.value,
    ]
    auto_occupancy_segmentation = (
        pl.when(num_travelers == 1)
        .then(pl.lit(CTRAMPModeType.DA.value))
        .when(num_travelers == 2)  # noqa: PLR2004
        .then(pl.lit(CTRAMPModeType.SR2.value))
        .otherwise(pl.lit(CTRAMPModeType.SR3.value))
    )
    auto_expr = tnc_expr.when(mode_type.is_in(auto_modes)).then(auto_occupancy_segmentation)

    # Default to drive alone (DA=1) for OTHER, LONG_DISTANCE, MISSING, and any unknown modes
    return auto_expr.otherwise(pl.lit(CTRAMPModeType.DA.value))
