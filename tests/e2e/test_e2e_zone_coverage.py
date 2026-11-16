"""Out-of-region travel is gated by the usability profile, not dropped by a formatter.

Household 27 lives outside the zone file; household 28 lives in region and commutes out
of it on one of its two dates, which separates the household term from the tour term.
"""

import polars as pl
import pytest

GATED = "usable_ctramp"  # the ctramp profile, whose zone_coverage is taz
UNGATED = "usable_analysis"  # zone_coverage: none

OUT_OF_REGION_HH = 27
COMMUTES_OUT_HH = 28


def _for_hh(frame: pl.DataFrame, hh_id: int) -> pl.DataFrame:
    return frame.filter(pl.col("hh_id") == hh_id)


class TestTheFixtureActuallyHasRecordsWithoutZones:
    """Guards the guard: every assertion below is vacuous without this."""

    def test_a_household_has_no_home_zone(self, full_tables):
        home = _for_hh(full_tables["households"], OUT_OF_REGION_HH)
        assert home.height == 1, "fixture household 27 is missing"
        assert home["home_taz"].null_count() == 1, (
            "household 27 was expected to fall outside the zone file, but the zone "
            "join placed it -- the snap distance or the fixture coordinates moved"
        )

    def test_a_tour_has_an_end_with_no_zone(self, full_tables):
        tours = _for_hh(full_tables["tours"], COMMUTES_OUT_HH)
        assert tours["d_taz"].null_count() >= 1, (
            "household 28 was expected to travel out of region on one date"
        )


class TestAHouseholdWithNoHomeZoneTakesItsTravelWithIt:
    """The household term, which the upward cascade cannot deliver on its own."""

    @pytest.mark.parametrize("table", ["households", "persons", "person_days", "tours"])
    def test_nothing_it_owns_is_usable(self, full_tables, table):
        rows = _for_hh(full_tables[table], OUT_OF_REGION_HH)
        assert rows.height > 0, f"{table} has no rows for household 27"

        usable = rows.filter(pl.col(GATED).fill_null(value=False))

        assert usable.height == 0, (
            f"{usable.height} {table} row(s) of a household with no home zone are still "
            f"{GATED}; the home-zone term is not reaching this level"
        )


class TestTravellingOutOfRegionGatesTheDayNotThePerson:
    """The tour term alone, on a household whose home keeps its zone."""

    def test_the_household_itself_is_still_usable(self, full_tables):
        """Otherwise this case says nothing the household case did not."""
        household = _for_hh(full_tables["households"], COMMUTES_OUT_HH)

        assert household[GATED].all(), "household 28's home should keep its zone"

    def test_the_out_of_region_day_is_gated(self, full_tables):
        person_days = _for_hh(full_tables["person_days"], COMMUTES_OUT_HH)
        assert person_days.height == 2, "household 28 should have two diary dates"

        assert person_days[GATED].sum() == 1, (
            f"expected exactly one of household 28's days to survive {GATED}, "
            f"got {person_days[GATED].to_list()}"
        )

    def test_the_person_survives_on_their_other_day(self, full_tables):
        """A gated day costs the day, not the traveller."""
        persons = _for_hh(full_tables["persons"], COMMUTES_OUT_HH)

        assert persons[GATED].all(), "the person kept a usable day and should stay usable"


class TestCoverageIsPerProfile:
    """A profile asking nothing of geography admits what another one excludes."""

    @pytest.mark.parametrize("hh_id", [OUT_OF_REGION_HH, COMMUTES_OUT_HH])
    def test_a_profile_asking_nothing_of_geography_admits_them(self, full_tables, hh_id):
        person_days = _for_hh(full_tables["person_days"], hh_id)
        assert person_days[UNGATED].any(), (
            f"{UNGATED} sets zone_coverage: none, so geography must not exclude "
            f"household {hh_id} from it"
        )
