"""A member whose group did not survive becomes an individual traveller, not a ghost.

CT-RAMP requires ``num_participants >= 2``: a party of one is not a party. The
cascade enforces that upstream -- a joint grouping is usable only while two of
its members are -- so by the time a formatter runs, no under-quorum group is
admitted.

What the formatter still has to handle is the member left behind. Rejecting a
group does not reject its one usable member, and that member still carries the
group's id. Downstream the id resolves to nothing and the record falls through
both doors: individual tours take only tours whose ``joint_tour_id`` is null,
and the joint tables are built from the group rows that survived. The traveller
would vanish from the output altogether.

So the gate clears those ids. That repairs a reference; it does not make a
decision. Both groupings need it, because a joint trip is not merely a part of a
joint tour -- members can travel together on tours that were never joint, so a
trip-level group can exist where no ``joint_tour_id`` does.
"""

import polars as pl

from processing.formatting.usable_records import keep_usable

FLAG = "ctramp_usable"


def _tours(rows: list[tuple[int, int | None, bool]]) -> pl.DataFrame:
    """Rows of (tour_id, joint_tour_id, usable)."""
    return pl.DataFrame(
        {
            "tour_id": [r[0] for r in rows],
            "joint_tour_id": [r[1] for r in rows],
            FLAG: [r[2] for r in rows],
        },
        schema_overrides={"joint_tour_id": pl.Int64},
    )


def _linked_trips(rows: list[tuple[int, int, int | None, bool]]) -> pl.DataFrame:
    """Rows of (linked_trip_id, tour_id, joint_trip_id, usable)."""
    return pl.DataFrame(
        {
            "linked_trip_id": [r[0] for r in rows],
            "tour_id": [r[1] for r in rows],
            "joint_trip_id": [r[2] for r in rows],
            FLAG: [r[3] for r in rows],
        },
        schema_overrides={"joint_trip_id": pl.Int64},
    )


def _groups(name: str, rows: list[tuple[int, bool]]) -> pl.DataFrame:
    return pl.DataFrame(
        {name: [r[0] for r in rows], FLAG: [r[1] for r in rows]},
        schema_overrides={name: pl.Int64},
    )


class TestAJointTripIsItsOwnGrouping:
    """The case a rule keyed on the tour structurally cannot see."""

    def test_a_survivor_of_a_rejected_trip_group_is_demoted(self):
        """Its joint_tour_id is null, so only a trip-level rule reaches it.

        The second member leaves because its *tour* is unusable, which is the
        only way a member can depart. A tour's ``complete`` is the ALL over its
        member trips, so an incomplete trip makes its own tour incomplete and
        therefore unusable -- a usable tour never holds an unusable trip. On
        BATS 2023 that holds for all 324,965 linked trips, zero exceptions.
        """
        kept = keep_usable(
            {
                "tours": _tours([(1, None, True), (2, None, False)]),
                "linked_trips": _linked_trips([(1, 1, 900, True), (2, 2, 900, False)]),
                "joint_trips": _groups("joint_trip_id", [(900, False)]),
            },
            FLAG,
        )

        assert kept["joint_trips"].height == 0, "the under-quorum group must not be emitted"
        assert kept["linked_trips"]["joint_trip_id"].to_list() == [None], (
            "the surviving member still pointed at a group that is gone"
        )

    def test_the_survivors_travel_is_kept(self):
        """Demotion, not deletion: the trip happened, it was just not joint."""
        kept = keep_usable(
            {
                "tours": _tours([(1, None, True), (2, None, False)]),
                "linked_trips": _linked_trips([(1, 1, 900, True), (2, 2, 900, False)]),
                "joint_trips": _groups("joint_trip_id", [(900, False)]),
            },
            FLAG,
        )

        assert kept["linked_trips"]["linked_trip_id"].to_list() == [1]


class TestTheTourGroupingIsSeparate:
    """Neither grouping's rule stands in for the other's."""

    def test_a_survivor_of_a_rejected_tour_group_is_demoted(self):
        """Otherwise it is in neither the individual nor the joint tour table."""
        kept = keep_usable(
            {
                "tours": _tours([(1, 500, True), (2, 500, False)]),
                "joint_tours": _groups("joint_tour_id", [(500, False)]),
            },
            FLAG,
        )

        assert kept["joint_tours"].height == 0
        assert kept["tours"]["joint_tour_id"].to_list() == [None]


class TestNoRecordOutlivesItsParent:
    """The cascade's verdicts do not imply one another downward.

    A household is usable only on a date where *every* member was; a person needs
    just one usable day of their own. So a person can pass while their household
    does not. Keeping that person hands CT-RAMP a record with no household to
    read a home zone from, which arrives as a null HomeTAZ -- 1,512 rows of it on
    BATS 2023.
    """

    def test_a_person_does_not_outlive_their_household(self):
        """The shape that reached CT-RAMP as a null HomeTAZ."""
        kept = keep_usable(
            {
                "households": pl.DataFrame({"hh_id": [1, 2], FLAG: [True, False]}),
                "persons": pl.DataFrame(
                    {"person_id": [10, 20], "hh_id": [1, 2], FLAG: [True, True]}
                ),
            },
            FLAG,
        )

        assert kept["persons"]["person_id"].to_list() == [10], (
            "a usable person in an unusable household has no home to report"
        )

    def test_removal_propagates_down_the_hierarchy(self):
        """Dropping a household drops its persons, and their days with them."""
        kept = keep_usable(
            {
                "households": pl.DataFrame({"hh_id": [1, 2], FLAG: [True, False]}),
                "persons": pl.DataFrame(
                    {"person_id": [10, 20], "hh_id": [1, 2], FLAG: [True, True]}
                ),
                "days": pl.DataFrame(
                    {
                        "day_id": [100, 200],
                        "person_id": [10, 20],
                        "hh_id": [1, 2],
                        FLAG: [True, True],
                    }
                ),
            },
            FLAG,
        )

        assert kept["days"]["day_id"].to_list() == [100]


class TestTheIdIsClearedEverywhereItIsCarried:
    """The ids are denormalised, and clearing only some of them is worse than none.

    ``joint_tour_id`` sits on tours, linked_trips and unlinked_trips. Clear it on
    tours alone and a trip is left claiming a joint tour while carrying no joint
    trip -- exactly what ``_validate_joint_tours_are_wholly_joint`` refuses,
    because such a leg reaches neither output file: the individual trip file
    excludes joint tours, and the joint trip file needs a ``joint_trip_id``.

    On BATS 2023 that omission stranded 64 trips across 24 joint tours.
    """

    def test_a_demoted_tour_clears_the_id_on_its_trips_too(self):
        """The exact shape that failed on real data."""
        kept = keep_usable(
            {
                "tours": _tours([(1, 500, True), (2, 500, False)]),
                "linked_trips": pl.DataFrame(
                    {
                        "linked_trip_id": [10, 20],
                        "tour_id": [1, 2],
                        "joint_tour_id": [500, 500],
                        "joint_trip_id": [900, 900],
                        FLAG: [True, False],
                    },
                    schema_overrides={"joint_tour_id": pl.Int64, "joint_trip_id": pl.Int64},
                ),
                "joint_tours": _groups("joint_tour_id", [(500, False)]),
                "joint_trips": _groups("joint_trip_id", [(900, False)]),
            },
            FLAG,
        )

        trips = kept["linked_trips"]
        assert trips["joint_tour_id"].to_list() == [None], (
            "a trip still naming a demoted joint tour reaches neither output file"
        )
        assert trips["joint_trip_id"].to_list() == [None], (
            "a tour that is no longer joint cannot keep joint trips on it"
        )

    def test_a_demoted_tour_clears_the_id_on_unlinked_trips(self):
        """Unlinked trips carry the tour id as well, and are written out too."""
        kept = keep_usable(
            {
                "tours": _tours([(1, 500, True), (2, 500, False)]),
                "unlinked_trips": pl.DataFrame(
                    {
                        "unlinked_trip_id": [100, 200],
                        "tour_id": [1, 2],
                        "joint_tour_id": [500, 500],
                        FLAG: [True, False],
                    },
                    schema_overrides={"joint_tour_id": pl.Int64},
                ),
                "joint_tours": _groups("joint_tour_id", [(500, False)]),
            },
            FLAG,
        )

        assert kept["unlinked_trips"]["joint_tour_id"].to_list() == [None]


class TestAnAdmittedGroupIsLeftAlone:
    """Repair applies to broken references only."""

    def test_members_of_a_surviving_group_keep_their_id(self):
        """The cascade admitted it, so two members are usable and it stays joint."""
        kept = keep_usable(
            {
                "tours": _tours([(1, None, True), (2, None, True)]),
                "linked_trips": _linked_trips([(1, 1, 900, True), (2, 2, 900, True)]),
                "joint_trips": _groups("joint_trip_id", [(900, True)]),
            },
            FLAG,
        )

        assert kept["joint_trips"].height == 1
        assert kept["linked_trips"]["joint_trip_id"].to_list() == [900, 900]

    def test_an_individual_trip_is_untouched(self):
        """A null group id was never a reference to repair."""
        kept = keep_usable(
            {
                "tours": _tours([(1, None, True)]),
                "linked_trips": _linked_trips([(1, 1, None, True)]),
                "joint_trips": _groups("joint_trip_id", []),
            },
            FLAG,
        )

        assert kept["linked_trips"]["joint_trip_id"].to_list() == [None]
