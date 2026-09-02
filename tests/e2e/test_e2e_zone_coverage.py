"""Out-of-region travel is gated by the profile, not dropped by a formatter.

A record the model's zone system cannot address is real data that one consumer
cannot use -- which is what a usability profile says. Deciding it here rather
than inside a formatter means the weighting sums over the same records that
reach the output, so a joint group's ``num_participants`` matches the members
its weight was summed over instead of whatever survived a later filter.

The fixture supplies two cases because the two terms fail together on any
household that simply lives out of region, and a test that never separates them
cannot show the household term does any work. Household 27 is outside the zone
file entirely; household 28 lives in region and commutes out of it on one of its
two dates.

The household term is the one worth the trouble. The cascade reduces *upward* --
days to persons to households -- so a household-level fact reaches its
descendants only because it is joined into their verdicts. Get that wrong and an
unaddressable household keeps perfectly usable tours hanging off a household the
consumer cannot write at all.
"""

import polars as pl
import pytest

GATED = "ctramp_usable"  # zone_coverage: taz
UNGATED = "analysis_usable"  # zone_coverage: none

OUT_OF_REGION_HH = 27
COMMUTES_OUT_HH = 28


@pytest.fixture(scope="module")
def tables(full_result) -> dict[str, pl.DataFrame]:
    return (
        full_result.as_dict_non_null()
        if hasattr(full_result, "as_dict_non_null")
        else dict(full_result)
    )


def _for_hh(frame: pl.DataFrame, hh_id: int) -> pl.DataFrame:
    return frame.filter(pl.col("hh_id") == hh_id)


class TestTheFixtureActuallyHasUnaddressableRecords:
    """Guards the guard: every assertion below is vacuous without this."""

    def test_a_household_has_no_home_zone(self, tables):
        home = _for_hh(tables["households"], OUT_OF_REGION_HH)
        assert home.height == 1, "fixture household 27 is missing"
        assert home["home_taz"].null_count() == 1, (
            "household 27 was expected to fall outside the zone file, but the zone "
            "join placed it -- the snap distance or the fixture coordinates moved"
        )

    def test_a_tour_has_an_unaddressable_end(self, tables):
        tours = _for_hh(tables["tours"], COMMUTES_OUT_HH)
        assert tours["d_taz"].null_count() >= 1, (
            "household 28 was expected to travel out of region on one date"
        )


class TestAnUnaddressableHouseholdTakesItsTravelWithIt:
    """The household term, which the upward cascade cannot deliver on its own."""

    @pytest.mark.parametrize("table", ["households", "persons", "days", "tours"])
    def test_nothing_it_owns_is_usable(self, tables, table):
        rows = _for_hh(tables[table], OUT_OF_REGION_HH)
        assert rows.height > 0, f"{table} has no rows for household 27"

        usable = rows.filter(pl.col(GATED).fill_null(value=False))

        assert usable.height == 0, (
            f"{usable.height} {table} row(s) of an unaddressable household are still "
            f"{GATED}; the home-zone term is not reaching this level"
        )


class TestTravellingOutOfRegionGatesTheDayNotThePerson:
    """The tour term alone, on a household that stays addressable."""

    def test_the_household_itself_is_still_usable(self, tables):
        """Otherwise this case says nothing the household case did not."""
        household = _for_hh(tables["households"], COMMUTES_OUT_HH)

        assert household[GATED].all(), "household 28 should remain addressable"

    def test_the_out_of_region_day_is_gated(self, tables):
        days = _for_hh(tables["days"], COMMUTES_OUT_HH)
        assert days.height == 2, "household 28 should have two diary dates"

        assert days[GATED].sum() == 1, (
            f"expected exactly one of household 28's days to survive {GATED}, "
            f"got {days[GATED].to_list()}"
        )

    def test_the_person_survives_on_their_other_day(self, tables):
        """A gated day costs the day, not the traveller."""
        persons = _for_hh(tables["persons"], COMMUTES_OUT_HH)

        assert persons[GATED].all(), "the person kept a usable day and should stay usable"


class TestCoverageIsPerProfile:
    """Coverage is asked per consumer, not once for everyone.

    The axis exists because zone systems differ, so a profile asking nothing of
    geography must still admit what another one excludes.
    """

    @pytest.mark.parametrize("hh_id", [OUT_OF_REGION_HH, COMMUTES_OUT_HH])
    def test_a_profile_asking_nothing_of_geography_admits_them(self, tables, hh_id):
        days = _for_hh(tables["days"], hh_id)
        if UNGATED not in days.columns:
            pytest.skip(f"{UNGATED} is not stamped in this run")

        assert days[UNGATED].any(), (
            f"{UNGATED} sets zone_coverage: none, so geography must not exclude "
            f"household {hh_id} from it"
        )
