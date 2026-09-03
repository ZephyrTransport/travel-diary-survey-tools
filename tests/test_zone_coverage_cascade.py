"""A tour is addressable only if every leg of it is, not just its endpoints.

``zone_coverage`` asks whether a consumer's zone system can place a record. For a
tour, the obvious reading is its own ``o``/``d`` -- but those are its anchor and
its primary destination, so they say nothing about the legs between. A tour can
leave home in the model area, stop somewhere outside it, and return: endpoints
fine, one leg unplaceable.

CT-RAMP needs a zone for every trip it writes, so that tour is unusable to it. On
BATS 2023 the endpoints-only reading left 360 such tours carrying 986
unplaceable trips, which reached the output as null ``orig_taz``/``dest_taz`` and
failed validation.

The rule is therefore an ALL over the tour's trips -- the same shape as
``complete``, which is also a property of the legs rather than the ends.
"""

import polars as pl

from data_canon.codebook.tours import TourCategory, TourDataQuality
from processing.completeness import UsabilityProfile, compute_usability

ZONE = "taz"
VALID, COMPLETE = TourDataQuality.VALID.value, TourCategory.COMPLETE.value


def _tables(trip_zones: list[tuple[int | None, int | None]]) -> dict[str, pl.DataFrame]:
    """One household, one person, one day, one tour, with the given trip endpoints.

    The tour's own endpoints are always addressable, so only the legs can fail.
    """
    n = len(trip_zones)
    return {
        "households": pl.DataFrame({"hh_id": [1], f"home_{ZONE}": [100], "complete": [True]}),
        "persons": pl.DataFrame(
            {"person_id": [1], "hh_id": [1], "complete": [True], "surveyable": [True]}
        ),
        "days": pl.DataFrame(
            {
                "day_id": [1],
                "person_id": [1],
                "hh_id": [1],
                "travel_date": ["2023-01-02"],
                "complete": [True],
            }
        ),
        "tours": pl.DataFrame(
            {
                "tour_id": [1],
                "day_id": [1],
                "person_id": [1],
                "hh_id": [1],
                "complete": [True],
                "tour_data_quality": [VALID],
                "tour_category": [COMPLETE],
                f"o_{ZONE}": [100],
                f"d_{ZONE}": [100],
            }
        ).with_columns(pl.lit(None, dtype=pl.Int64).alias("parent_tour_id")),
        "linked_trips": pl.DataFrame(
            {
                "linked_trip_id": list(range(1, n + 1)),
                "tour_id": [1] * n,
                "day_id": [1] * n,
                "complete": [True] * n,
                f"o_{ZONE}": [z[0] for z in trip_zones],
                f"d_{ZONE}": [z[1] for z in trip_zones],
            },
            schema_overrides={f"o_{ZONE}": pl.Int64, f"d_{ZONE}": pl.Int64},
        ),
    }


def _verdict(trip_zones, *, coverage: str) -> bool:
    tables = _tables(trip_zones)
    profile = UsabilityProfile("u", "primary_home", "all_members", zone_coverage=coverage)
    compute_usability(tables, profile=profile)
    return bool(tables["tours"]["u"][0])


class TestALegOutsideTheAreaCostsTheTour:
    """The ALL over legs, which the endpoints-only rule missed."""

    def test_all_legs_addressable_is_usable(self):
        """The baseline, or the test below proves nothing."""
        assert _verdict([(100, 200), (200, 100)], coverage=ZONE) is True

    def test_an_unaddressable_middle_leg_makes_the_tour_unusable(self):
        """Endpoints are fine; one stop is outside the area.

        This is the case the endpoints-only rule admitted, and CT-RAMP then
        rejected for a null trip TAZ.
        """
        assert _verdict([(100, None), (None, 100)], coverage=ZONE) is False

    def test_the_missing_sentinel_counts_as_unaddressable(self):
        """-1 is written as a missing zone as well as null."""
        assert _verdict([(100, -1), (-1, 100)], coverage=ZONE) is False


class TestCoverageIsStillOptional:
    """Nothing changes for a profile that asks nothing of geography."""

    def test_a_profile_asking_nothing_of_geography_admits_it(self):
        """The term is inert where it is not asked for."""
        assert _verdict([(100, None), (None, 100)], coverage="none") is True
