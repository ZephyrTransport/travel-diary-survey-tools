"""A joint group that loses members must stop being joint, at both levels.

CT-RAMP requires ``num_participants >= 2``: a party of one is not a party. The
tour filter can strip members from a group, so quorum has to be re-checked after
it -- and on both groupings, because a joint *trip* is not merely a part of a
joint tour.

Only the tour level was checked. A joint trip whose members travelled together
on tours that were not themselves joint has no ``joint_tour_id`` at all, so a
rule keyed on the tour never saw it; and even inside a surviving joint tour, one
leg can lose a member while the tour keeps two. On BATS 2023 that left 2,380 of
26,514 joint-trip groups with a single surviving member trip, every one of them
emitted with ``num_participants = 1``.

A group that falls below quorum is demoted rather than deleted: the survivor's
travel is real, it is simply no longer joint.
"""

import polars as pl

from processing.completeness import MIN_JOINT_PARTICIPANTS
from processing.formatting.ctramp.filters import (
    _drop_lone_joint_tours,
    _drop_lone_joint_trips,
)


def _tours(joint_tour_ids: list[int | None]) -> pl.DataFrame:
    n = len(joint_tour_ids)
    return pl.DataFrame(
        {
            "tour_id": list(range(1, n + 1)),
            "person_id": list(range(1, n + 1)),
            "joint_tour_id": joint_tour_ids,
        }
    )


def _linked_trips(rows: list[tuple[int, int | None, int | None]]) -> pl.DataFrame:
    """Rows of (tour_id, joint_tour_id, joint_trip_id)."""
    return pl.DataFrame(
        {
            "linked_trip_id": list(range(1, len(rows) + 1)),
            "tour_id": [r[0] for r in rows],
            "joint_tour_id": [r[1] for r in rows],
            "joint_trip_id": [r[2] for r in rows],
        },
        schema_overrides={"joint_tour_id": pl.Int64, "joint_trip_id": pl.Int64},
    )


def _joint_trips(ids: list[int]) -> pl.DataFrame:
    return pl.DataFrame({"joint_trip_id": ids}, schema={"joint_trip_id": pl.Int64})


class TestAJointTripIsItsOwnGrouping:
    """The case the tour-level rule structurally cannot see."""

    def test_a_lone_joint_trip_with_no_joint_tour_is_demoted(self):
        """joint_tour_id is null, so a rule keyed on the tour never reaches it.

        This is the shape that reached CT-RAMP as num_participants = 1.
        """
        tours = _tours([None, None])
        trips = _linked_trips([(1, None, 900)])  # one member left in group 900

        _, trips_out, joint_out = _drop_lone_joint_trips(tours, trips, _joint_trips([900]))

        assert trips_out["joint_trip_id"].to_list() == [None]
        assert joint_out.height == 0

    def test_a_lone_leg_inside_a_surviving_joint_tour_is_demoted(self):
        """The tour keeps quorum while one of its trips does not."""
        tours = _tours([50, 50])
        trips = _linked_trips(
            [
                (1, 50, 900),  # group 900 keeps both members
                (2, 50, 900),
                (1, 50, 901),  # group 901 is down to one
            ]
        )

        _, trips_out, joint_out = _drop_lone_joint_trips(tours, trips, _joint_trips([900, 901]))

        assert joint_out["joint_trip_id"].to_list() == [900]
        assert trips_out["joint_trip_id"].to_list() == [900, 900, None]

    def test_a_group_at_exactly_quorum_survives(self):
        """The boundary itself, so the rule is not off by one."""
        assert MIN_JOINT_PARTICIPANTS == 2
        tours = _tours([50, 50])
        trips = _linked_trips([(1, 50, 900), (2, 50, 900)])

        _, trips_out, joint_out = _drop_lone_joint_trips(tours, trips, _joint_trips([900]))

        assert joint_out["joint_trip_id"].to_list() == [900]
        assert trips_out["joint_trip_id"].to_list() == [900, 900]


class TestDemotionNotDeletion:
    """The survivor's travel happened; only its joint-ness did not."""

    def test_the_surviving_trip_is_kept(self):
        """Its travel happened; only the joint label was wrong."""
        tours = _tours([None])
        trips = _linked_trips([(1, None, 900)])

        _, trips_out, _ = _drop_lone_joint_trips(tours, trips, _joint_trips([900]))

        assert trips_out.height == 1
        assert trips_out["tour_id"].to_list() == [1]

    def test_unrelated_groups_are_untouched(self):
        """A quorum failure in one group must not disturb another."""
        tours = _tours([50, 50])
        trips = _linked_trips([(1, 50, 900), (2, 50, 900), (1, 50, 901)])

        _, trips_out, joint_out = _drop_lone_joint_trips(tours, trips, _joint_trips([900, 901]))

        assert trips_out.filter(pl.col("joint_trip_id") == 900).height == 2
        assert joint_out.height == 1


class TestNothingToDo:
    """Quiet on data that needs no change."""

    def test_all_groups_at_quorum_are_left_alone(self):
        """Correct data must pass through untouched."""
        tours = _tours([50, 50])
        trips = _linked_trips([(1, 50, 900), (2, 50, 900)])

        tours_out, trips_out, joint_out = _drop_lone_joint_trips(tours, trips, _joint_trips([900]))

        assert tours_out.equals(tours)
        assert trips_out.equals(trips)
        assert joint_out.height == 1

    def test_no_joint_trips_at_all(self):
        """A project without joint travel is not an error."""
        tours = _tours([None])
        trips = _linked_trips([(1, None, None)])

        _, trips_out, joint_out = _drop_lone_joint_trips(tours, trips, _joint_trips([]))

        assert trips_out.height == 1
        assert joint_out.height == 0


class TestTheTourRuleIsSeparate:
    """The other grouping, checked on its own terms.

    A joint tour falls below quorum when the tour filter drops enough of its
    member tours. That is a different question from whether any individual leg
    kept two travellers, which is why the two rules run independently rather
    than one gating the other.
    """

    def test_a_joint_tour_with_one_survivor_is_demoted(self):
        """Its ids are nulled, so it is emitted as an individual tour."""
        tours = _tours([50])  # only one member tour of group 50 survived
        trips = _linked_trips([(1, 50, None)])

        tours_out, trips_out, _ = _drop_lone_joint_tours(tours, trips, _joint_trips([]))

        assert tours_out["joint_tour_id"].to_list() == [None]
        assert trips_out["joint_tour_id"].to_list() == [None]

    def test_a_joint_tour_at_quorum_survives(self):
        """Two participants is a party; the boundary must not be off by one."""
        tours = _tours([50, 50])
        trips = _linked_trips([(1, 50, None), (2, 50, None)])

        tours_out, _, _ = _drop_lone_joint_tours(tours, trips, _joint_trips([]))

        assert tours_out["joint_tour_id"].to_list() == [50, 50]

    def test_the_survivors_tour_is_kept(self):
        """Demoted, not deleted -- the travel happened."""
        tours = _tours([50])
        trips = _linked_trips([(1, 50, None)])

        tours_out, trips_out, _ = _drop_lone_joint_tours(tours, trips, _joint_trips([]))

        assert tours_out.height == 1
        assert trips_out.height == 1

    def test_it_leaves_joint_trips_alone(self):
        """The trip rule owns that grouping; this one must not pre-empt it.

        A joint trip whose tour keeps quorum is none of the tour rule's business,
        and one whose tour does not is still the trip rule's to judge.
        """
        tours = _tours([50, 50])
        trips = _linked_trips([(1, 50, 900), (2, 50, 900)])

        _, trips_out, joint_out = _drop_lone_joint_tours(tours, trips, _joint_trips([900]))

        assert trips_out["joint_trip_id"].to_list() == [900, 900]
        assert joint_out["joint_trip_id"].to_list() == [900]
