"""Choosing the member trip that stands for a joint group's location.

Canonical joint tables carry no coordinates -- they record which trips travelled
together, not where. CT-RAMP needs one origin and one destination per joint
record, so the choice is made here, and these pin what it chooses.
"""

import polars as pl

from processing.formatting.ctramp.joint_representative import (
    representative_person_per_tour,
    select_representative_members,
)

TWO = 2


def _members(rows: list[tuple[int, int, int | None, float, float]]) -> pl.DataFrame:
    """(person_id, joint_trip_id, joint_tour_id, o_lat, d_lat) member trips."""
    return pl.DataFrame(
        {
            "person_id": [r[0] for r in rows],
            "joint_trip_id": [r[1] for r in rows],
            "joint_tour_id": [r[2] for r in rows],
            "o_lat": [r[3] for r in rows],
            "o_lon": [-122.4 + r[3] for r in rows],
            "d_lat": [r[4] for r in rows],
            "d_lon": [-122.5 + r[4] for r in rows],
        },
        schema_overrides={"joint_tour_id": pl.Int64},
    )


class TestChoosingTheMember:
    """Which member of a single joint trip is picked."""

    def test_a_pair_is_settled_by_person(self):
        """Two members are equidistant from their own midpoint, so person_id decides."""
        members = _members([(102, 500, 900, 37.500, 37.900), (101, 500, 900, 37.600, 37.800)])

        rep = select_representative_members(members)

        assert rep["person_id"].to_list() == [101]
        assert rep["o_lat"].to_list() == [37.600]  # not the 37.55 midpoint

    def test_a_pair_ties_even_when_the_arithmetic_is_untidy(self):
        """The pair's tie must survive floating point, or the tie-break never runs.

        Two members are equidistant from their own midpoint exactly, but not in
        binary: subtracting the midpoint can leave one a fraction of an ulp
        closer. Unrounded, that noise decides the commonest case instead of
        ``person_id``.
        """
        members = _members([(101, 500, 900, 39.0100, 40.0100), (102, 500, 900, 39.0000, 40.0000)])

        rep = select_representative_members(members)

        assert rep["person_id"].to_list() == [101]

    def test_three_members_take_the_one_nearest_the_centre(self):
        """With no tie the most central member wins, over a lower person number."""
        members = _members(
            [
                (101, 500, 900, 37.0000, 38.0000),
                (102, 500, 900, 37.0005, 38.0005),
                (103, 500, 900, 37.0100, 38.0100),
            ]
        )

        rep = select_representative_members(members)

        assert rep["person_id"].to_list() == [102]

    def test_both_ends_come_from_the_same_member(self):
        """A per-end rule would compose an origin and destination nobody paired.

        Person 102 is nearest the centre at the origin and 103 at the
        destination, but 101 is closest over the two together -- so the row is
        person 101's real trip, not a splice of the other two.
        """
        members = _members(
            [
                (101, 500, 900, 37.0000, 38.0000),
                (102, 500, 900, 37.0010, 38.0100),
                (103, 500, 900, 37.0100, 38.0010),
            ]
        )

        rep = select_representative_members(members)

        assert rep["o_lat"].to_list() == [37.0000]
        assert rep["d_lat"].to_list() == [38.0000]

    def test_the_representative_is_a_real_member_trip(self):
        """Every returned coordinate belongs to one of the member trips."""
        members = _members(
            [
                (101, 500, 900, 37.0000, 38.0000),
                (102, 500, 900, 37.0005, 38.0005),
                (103, 500, 900, 37.0100, 38.0100),
            ]
        )

        rep = select_representative_members(members)

        matched = members.join(rep.select(members.columns), on=members.columns)
        assert len(matched) == 1

    def test_the_choice_does_not_depend_on_row_order(self):
        """Same group, reversed rows, same representative."""
        members = _members(
            [
                (101, 500, 900, 37.0000, 38.0000),
                (102, 500, 900, 37.0005, 38.0005),
                (103, 500, 900, 37.0100, 38.0100),
            ]
        )

        assert select_representative_members(members)["person_id"].to_list() == (
            select_representative_members(members.reverse())["person_id"].to_list()
        )


class TestOneMemberPerJointTour:
    """The choice is settled per joint tour, not per leg.

    Choosing per leg lets a tour take its outbound from one member and its
    inbound from another. Where those two reported home a few metres apart
    across a zone boundary, the tour then leaves one zone and returns to a
    different one -- a tour that never gets home.
    """

    @staticmethod
    def _tour_where_legs_disagree() -> pl.DataFrame:
        """Two legs of one joint tour whose per-leg medoids are different people.

        Outbound: 102 is the middle of 101/102/103. Inbound: 103 is. Left to
        themselves the legs would pick different members.
        """
        return _members(
            [
                # outbound leg
                (101, 500, 900, 37.0000, 38.0000),
                (102, 500, 900, 37.0050, 38.0050),
                (103, 500, 900, 37.0200, 38.0200),
                # inbound leg, returning to the same places
                (101, 501, 900, 38.0200, 37.0200),
                (102, 501, 900, 38.0000, 37.0000),
                (103, 501, 900, 38.0050, 37.0050),
            ]
        )

    def test_every_leg_of_a_tour_uses_one_member(self):
        """Both legs come back with the same person."""
        rep = select_representative_members(self._tour_where_legs_disagree())

        assert len(rep) == TWO  # one row per leg
        assert rep["person_id"].n_unique() == 1

    def test_the_legs_would_otherwise_have_disagreed(self):
        """The premise: judged alone, the two legs pick different members.

        Without this the previous test would pass on a fixture that never
        exercised the rule.
        """
        members = self._tour_where_legs_disagree().with_columns(
            pl.lit(None, dtype=pl.Int64).alias("joint_tour_id")
        )

        per_leg = select_representative_members(members)

        assert per_leg["person_id"].n_unique() == TWO

    def test_separate_tours_choose_separately(self):
        """Two joint tours in one household are independent groups."""
        members = _members(
            [
                (101, 500, 900, 37.0000, 38.0000),
                (102, 500, 900, 37.0005, 38.0005),
                (101, 600, 901, 39.0100, 40.0100),
                (102, 600, 901, 39.0000, 40.0000),
            ]
        )

        rep = select_representative_members(members).sort("joint_trip_id")

        # Both are pairs, so both fall to the tie-break rather than to each other.
        assert rep["person_id"].to_list() == [101, 101]
        assert rep["joint_tour_id"].to_list() == [900, 901]


class TestTheTourAndItsTripsAgree:
    """The joint tour and its joint trips are two views of one outing."""

    def test_the_tour_takes_the_same_member_as_its_trips(self):
        """Both files resolve to one member, so neither describes half of each.

        The tour file used to take whichever member tour sorted first, which on
        BATS 2023 disagreed with the trip file's representative for 148 joint
        tours -- placing 10 of them in a different zone from their own trips.
        """
        members = _members(
            [
                (101, 500, 900, 37.0000, 38.0000),
                (102, 500, 900, 37.0050, 38.0050),
                (103, 500, 900, 37.0200, 38.0200),
                (101, 501, 900, 38.0200, 37.0200),
                (102, 501, 900, 38.0000, 37.0000),
                (103, 501, 900, 38.0050, 37.0050),
            ]
        )

        per_tour = representative_person_per_tour(members)
        per_leg = select_representative_members(members)

        assert len(per_tour) == 1
        assert per_tour["person_id"].to_list() == per_leg["person_id"].unique().to_list()

    def test_no_representative_without_joint_tours(self):
        """Joint trips outside any joint tour contribute no tour-level choice."""
        members = _members([(101, 500, None, 37.0, 38.0), (102, 500, None, 37.1, 38.1)])

        assert representative_person_per_tour(members).is_empty()
