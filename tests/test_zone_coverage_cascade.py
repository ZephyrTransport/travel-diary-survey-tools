"""A tour has a zone only if every leg of it does, not just its endpoints.

A tour's own ``o``/``d`` are its anchor and primary destination, so they say
nothing about the legs between. CT-RAMP needs a zone for every trip it writes,
so ``zone_coverage`` is an ALL over the tour's trips, like ``survey_complete``.
"""

import polars as pl
import pytest

from processing.usability import stamp_usable
from tests.fixtures.cascade import household, make_profile, stamp

ZONE = "taz"


def _tables(trip_zones: list[tuple[int | None, int | None]]) -> dict[str, pl.DataFrame]:
    """One person's tour with the given trip endpoints; the tour's own endpoints have zones."""
    n = len(trip_zones)
    tables = household(1, hh_id=[1], **{f"o_{ZONE}": [100], f"d_{ZONE}": [100]})
    tables["households"] = tables["households"].with_columns(pl.lit(100).alias(f"home_{ZONE}"))
    tables["linked_trips"] = pl.DataFrame(
        {
            "linked_trip_id": list(range(1, n + 1)),
            "tour_id": [10] * n,
            "person_day_id": [1] * n,
            "survey_complete": [True] * n,
            f"o_{ZONE}": [z[0] for z in trip_zones],
            f"d_{ZONE}": [z[1] for z in trip_zones],
        },
        schema_overrides={f"o_{ZONE}": pl.Int64, f"d_{ZONE}": pl.Int64},
    )
    return tables


def _verdict(trip_zones, *, coverage: str) -> bool:
    profile = make_profile("u", zone_coverage=coverage)
    return bool(stamp(_tables(trip_zones), profile)["tours"]["usable_u"][0])


@pytest.mark.parametrize(
    ("trip_zones", "coverage", "usable"),
    [
        pytest.param([(100, 200), (200, 100)], ZONE, True, id="all_legs_with_zones_is_usable"),
        # The case the endpoints alone admit, and CT-RAMP rejects for a null trip TAZ.
        pytest.param([(100, None), (None, 100)], ZONE, False, id="a_middle_leg_with_no_zone"),
        pytest.param([(100, -1), (-1, 100)], ZONE, False, id="the_missing_sentinel_is_no_zone"),
        pytest.param([(100, None), (None, 100)], "none", True, id="a_profile_asking_no_zones"),
    ],
)
def test_a_leg_outside_the_area_costs_the_tour(trip_zones, coverage, usable):
    """The tour is usable only if every leg has a zone, when the profile asks for one."""
    assert _verdict(trip_zones, coverage=coverage) is usable


@pytest.mark.parametrize("table", ["tours", "linked_trips"])
def test_missing_zone_columns_are_named(table):
    """Passing them silently would admit every leg the zone step never placed."""
    tables = _tables([(100, 200), (200, 100)])
    tables[table] = tables[table].drop(f"o_{ZONE}", f"d_{ZONE}")
    tables = stamp(tables)
    with pytest.raises(ValueError, match=f"{table} carries no o_{ZONE}, d_{ZONE}"):
        stamp_usable(tables, make_profile("u", zone_coverage=ZONE))
