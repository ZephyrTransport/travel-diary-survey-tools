"""Unit tests for CT-RAMP transit submode and mode code mapping."""

import polars as pl
import pytest

from data_canon.codebook.ctramp import CTRAMPModeType
from data_canon.codebook.trips import AccessEgressMode, Mode, ModeType
from processing.formatting.ctramp.mode_mappings import (
    TRANSIT_SUBMODE_FERRY,
    TRANSIT_SUBMODE_LR,
    aggregate_transit_submode,
    ctramp_mode_expression,
)


def _map_transit(submode: int, access_mode: int) -> int:
    df = pl.DataFrame(
        {
            "mode_type": [ModeType.TRANSIT.value],
            "num_travelers": [1],
            "access_mode": [access_mode],
            "egress_mode": [AccessEgressMode.WALK.value],
            "transit_submode": [submode],
            "tnc_type": [None],
        },
        schema_overrides={"tnc_type": pl.Int64},
    )
    expr = ctramp_mode_expression(
        pl.col("mode_type"),
        pl.col("num_travelers"),
        access_mode=pl.col("access_mode"),
        egress_mode=pl.col("egress_mode"),
        transit_submode=pl.col("transit_submode"),
        tnc_type=pl.col("tnc_type"),
    )
    return df.select(expr.alias("mode")).item()


@pytest.mark.parametrize(
    ("submode", "access_mode", "expected"),
    [
        (TRANSIT_SUBMODE_FERRY, AccessEgressMode.WALK.value, CTRAMPModeType.WLK_FERRY_WLK),
        (TRANSIT_SUBMODE_FERRY, AccessEgressMode.CAR_HOUSEHOLD.value, CTRAMPModeType.DRV_FERRY_WLK),
        (TRANSIT_SUBMODE_LR, AccessEgressMode.WALK.value, CTRAMPModeType.WLK_LRF_WLK),
        (TRANSIT_SUBMODE_LR, AccessEgressMode.CAR_HOUSEHOLD.value, CTRAMPModeType.DRV_LRF_WLK),
    ],
)
def test_ferry_and_light_rail_map_to_separate_codes(submode, access_mode, expected):
    """Ferry gets its own walk/drive codes instead of sharing light rail's."""
    assert _map_transit(submode, access_mode) == expected.value


def test_ferry_outranks_light_rail_but_not_heavy_rail():
    """A tour mixing light rail and ferry is ferry; ferry plus BART is heavy rail."""
    trips = pl.DataFrame(
        {
            "tour_id": [1, 1, 2, 2],
            "mode_1": [
                Mode.LIGHT_RAIL.value,
                Mode.FERRY.value,
                Mode.FERRY.value,
                Mode.BART.value,
            ],
        }
    )
    result = aggregate_transit_submode(trips, "tour_id").sort("tour_id")
    assert result["transit_submode"].to_list()[0] == TRANSIT_SUBMODE_FERRY
    assert result["transit_submode"].to_list()[1] > TRANSIT_SUBMODE_FERRY
