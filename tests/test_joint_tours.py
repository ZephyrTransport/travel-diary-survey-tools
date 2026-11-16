"""Joint tour identification.

A tour is joint when every trip of it is joint with the same stable group of
two or more people; a solo leg, a dropped-off member, or a second occasion
each change that.
"""

import polars as pl
import pytest

from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    SchoolType,
    Student,
)
from data_canon.codebook.trips import Purpose
from processing.joint_trips import detect_joint_trips
from processing.joint_trips.joint_trip_configs import JointTripConfig
from processing.tours.joint_tour_helpers import (
    _validate_joint_tours_have_joint_trips,
    identify_joint_tours,
)
from tests.fixtures.tour_pipeline import (
    HOME,
    SHOP,
    at,
    days_not_asked,
    leg,
    link,
    locate_and_extract_tours,
    unlinked_trips,
)

HH_ID = 23000075
P1 = 23000075001
P2 = 23000075002
P3 = 23000075003
PERSON_IDS = [P1, P2, P3]
DAY_IDS = [2300007500101, 2300007500201, 2300007500301]
SCHOOL = (37.81, -122.43)

PERSONS = pl.DataFrame(
    {
        "person_id": PERSON_IDS,
        "hh_id": [HH_ID] * 3,
        "age": [
            AgeCategory.AGE_35_TO_44.value,
            AgeCategory.AGE_35_TO_44.value,
            AgeCategory.AGE_5_TO_15.value,
        ],
        "employment": [
            Employment.EMPLOYED_FULLTIME.value,
            Employment.EMPLOYED_PARTTIME.value,
            Employment.UNEMPLOYED_NOT_LOOKING.value,
        ],
        "student": [
            Student.NONSTUDENT.value,
            Student.NONSTUDENT.value,
            Student.FULLTIME_INPERSON.value,
        ],
        "school_type": [None, None, SchoolType.ELEMENTARY.value],
        "work_lat": [37.85, 37.82, None],
        "work_lon": [-122.45, -122.48, None],
        "school_lat": [None, None, SCHOOL[0]],
        "school_lon": [None, None, SCHOOL[1]],
    }
)
HOUSEHOLDS = pl.DataFrame({"hh_id": [HH_ID], "home_lat": [HOME[0]], "home_lon": [HOME[1]]})


def _member_leg(member: int, *args, num_travelers: int) -> dict:
    """A 2 km leg by household member *member* (0-based)."""
    return leg(
        *args,
        person_id=PERSON_IDS[member],
        person_day_id=DAY_IDS[member],
        hh_id=HH_ID,
        num_travelers=num_travelers,
        distance_meters=2000.0,
    )


def _tours_from(legs: list[dict]) -> pl.DataFrame:
    """Run the whole link -> joint trips -> tours path and return the tours."""
    linked = link(unlinked_trips(legs))
    joint = detect_joint_trips(
        linked_trips=linked["linked_trips"],
        person_days=days_not_asked(linked["linked_trips"]),
        households=HOUSEHOLDS,
        matching=JointTripConfig(
            method="buffer",
            time_threshold_minutes=5.0,
            space_threshold_meters=50.0,
        ),
    )
    return locate_and_extract_tours(
        PERSONS,
        HOUSEHOLDS,
        linked["unlinked_trips"],
        joint["linked_trips"],
        joint_trips=joint["joint_trips"],
    )["tours"]


def _out_and_back(n_people: int, stagger_hours: int = 0) -> list[dict]:
    """*n_people* household members each go home -> shop -> home, 15 minutes each way.

    Unstaggered, every member's legs share times and endpoints, so the buffer
    detector sees one group on each leg; staggering leaves nobody together.
    """
    home, grocery = Purpose.HOME, Purpose.GROCERY
    legs = []
    for member in range(n_people):
        hour = 10 + stagger_hours * member
        for o, d, op, dp, h in (
            (HOME, SHOP, home, grocery, hour),
            (SHOP, HOME, grocery, home, hour + 1),
        ):
            legs.append(_member_leg(member, o, d, op, dp, at(h), at(h, 15), num_travelers=n_people))
    return legs


class TestFullyJointTour:
    """Test tours where all trips involve the same group of people."""

    @pytest.mark.parametrize(
        "n_people", [pytest.param(2, id="two_people"), pytest.param(3, id="three_people")]
    )
    def test_a_whole_tour_taken_together_is_one_joint_tour(self, n_people):
        """Each traveller keeps their own tour; the group shares one joint id."""
        tours = _tours_from(_out_and_back(n_people))

        assert len(tours) == n_people, "One tour per person"

        joint_tours = tours.filter(pl.col("joint_tour_id").is_not_null())
        assert len(joint_tours) == n_people, "Every tour should be marked joint"
        assert joint_tours["joint_tour_id"].n_unique() == 1, "All of them share one id"

        # joint_tour_id is the household id plus a two-digit sequence
        assert int(joint_tours["joint_tour_id"][0]) // 100 == HH_ID


def test_three_start_one_drops_off():
    """Three leave home; the child stays at school; the parents go on to shop and home.

    The stable group is the people on *every* joint trip of the tour, so the two
    parents are joint and the child is not.
    """
    k12, grocery, home = Purpose.K12_SCHOOL, Purpose.GROCERY, Purpose.HOME
    legs = []
    for parent in (0, 1):
        legs += [
            _member_leg(parent, HOME, SCHOOL, home, k12, at(8), at(8, 15), num_travelers=3),
            _member_leg(parent, SCHOOL, SHOP, k12, grocery, at(8, 30), at(9), num_travelers=2),
            _member_leg(parent, SHOP, HOME, grocery, home, at(9, 30), at(10), num_travelers=2),
        ]
    legs += [
        _member_leg(2, HOME, SCHOOL, home, k12, at(8), at(8, 15), num_travelers=3),
        _member_leg(2, SCHOOL, SHOP, k12, k12, at(16), at(16, 15), num_travelers=1),
        _member_leg(2, SHOP, HOME, k12, home, at(16, 15), at(16, 30), num_travelers=1),
    ]

    tours = _tours_from(legs)

    parent_joint = tours.filter(
        pl.col("person_id").is_in([P1, P2]) & pl.col("joint_tour_id").is_not_null()
    )
    assert len(parent_joint) == 2, "Both parents should have joint tours"
    assert parent_joint["joint_tour_id"].n_unique() == 1, "Parents share one joint_tour_id"

    child_tours = tours.filter(pl.col("person_id") == P3)
    assert not child_tours.is_empty(), "The child still has a tour of their own"
    assert child_tours["joint_tour_id"].null_count() == len(child_tours)


def _build_joint_tour_frames(
    trips: list[tuple[int, int, int, int | None]],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Minimal linked_trips and tours from (linked_trip_id, person_id, tour_id, joint_trip_id)."""
    linked_trips = pl.DataFrame(
        {
            "linked_trip_id": [t[0] for t in trips],
            "person_id": [t[1] for t in trips],
            "tour_id": [t[2] for t in trips],
            "joint_trip_id": [t[3] for t in trips],
            "hh_id": [HH_ID] * len(trips),
        },
        schema_overrides={"joint_trip_id": pl.Int64},
    )
    tours = pl.DataFrame({"tour_id": sorted({t[2] for t in trips})})
    return linked_trips, tours


class TestJointTourOccasions:
    """Test that joint_tour_id distinguishes occasions, not participant groups."""

    def test_same_pair_two_occasions_get_different_ids(self):
        """The same pair travelling twice should get one joint_tour_id per occasion.

        joint_tour_id is keyed on the set of joint_trip_ids (the occasion), so the
        same stable group on a second outing is a distinct joint tour rather than a
        continuation of the first.
        """
        # Occasion 1 = joint trips 1001/1002; occasion 2 = joint trips 1003/1004.
        linked_trips, tours = _build_joint_tour_frames(
            [
                (1, P1, 101, 1001),
                (2, P1, 101, 1002),
                (3, P2, 201, 1001),
                (4, P2, 201, 1002),
                (5, P1, 102, 1003),
                (6, P1, 102, 1004),
                (7, P2, 202, 1003),
                (8, P2, 202, 1004),
            ]
        )

        _, tours_out = identify_joint_tours(linked_trips, tours)

        ids = dict(zip(tours_out["tour_id"], tours_out["joint_tour_id"], strict=True))
        assert all(v is not None for v in ids.values()), "All four tours should be joint"

        # Partners on the same occasion share an id...
        assert ids[101] == ids[201], "Occasion 1 partners should share a joint_tour_id"
        assert ids[102] == ids[202], "Occasion 2 partners should share a joint_tour_id"

        # ...but the two occasions are distinct joint tours.
        assert ids[101] != ids[102], (
            "The same pair on a second occasion should get a different joint_tour_id"
        )

    def test_singleton_joint_tour_id_is_nulled(self):
        """A joint_tour_id surviving for only one person is reset to null.

        P2's tour has a non-joint trip, so it fails the all-trips-joint filter. That
        leaves P1's tour as the only member of the occasion; a joint tour needs 2+
        participants, so the id is dropped rather than left as a singleton.
        """
        linked_trips, tours = _build_joint_tour_frames(
            [
                (1, P1, 101, 1001),
                (2, P1, 101, 1002),
                (3, P2, 201, 1001),
                (4, P2, 201, 1002),
                (5, P2, 201, None),  # non-joint trip disqualifies P2's tour
            ]
        )

        _, tours_out = identify_joint_tours(linked_trips, tours)

        assert tours_out["joint_tour_id"].null_count() == len(tours_out), (
            "A joint_tour_id held by a single person should be nulled"
        )


class TestNoJointTours:
    """Test cases where no joint tours should be identified."""

    @pytest.mark.parametrize(
        "trips",
        [
            pytest.param(
                [
                    (1, P1, 101, None),
                    (2, P1, 101, None),
                    (3, P2, 201, None),
                    (4, P2, 201, None),
                ],
                id="nobody_shared_a_trip_all_day",
            ),
            pytest.param(
                [
                    (1, P1, 101, 1001),
                    (2, P1, 101, None),
                    (3, P2, 201, 1002),
                    (4, P2, 201, None),
                ],
                id="every_tour_has_a_solo_leg",
            ),
        ],
    )
    def test_no_joint_trips(self, trips):
        """Two people out on their own: nothing for the tours to intersect on."""
        linked_trips, tours = _build_joint_tour_frames(trips)

        _, tours_out = identify_joint_tours(linked_trips, tours)

        assert tours_out["joint_tour_id"].null_count() == len(tours_out)

    def test_the_whole_pipeline_survives_a_day_with_no_joint_trips(self):
        """Two members shopping five hours apart never travel together.

        Tour extraction has its own branch for joint detection finding nothing.
        """
        tours = _tours_from(_out_and_back(2, stagger_hours=5))

        assert len(tours) == 2
        assert tours["joint_tour_id"].null_count() == 2


class TestAJointTourHoldsAJointTrip:
    """``joint_tour_id`` claims shared travel, so some trip must be shared.

    Deliberately weaker than the rule ids are currently assigned under: a tour
    is admitted today only when *every* trip is joint, but a survey can record a
    half-shared tour -- a parent dropping a child, then driving on to work -- and
    widening the id to cover those should not have to revisit this check.
    """

    @staticmethod
    def _frame(joint_trip_ids: list[int | None], joint_tour_ids: list[int | None]) -> pl.DataFrame:
        n = len(joint_trip_ids)
        return pl.DataFrame(
            {
                "linked_trip_id": list(range(1, n + 1)),
                "person_id": [P1] * n,
                "tour_id": [101] * n,
                "joint_trip_id": joint_trip_ids,
                "joint_tour_id": joint_tour_ids,
            },
            schema_overrides={"joint_trip_id": pl.Int64, "joint_tour_id": pl.Int64},
        )

    def test_a_solo_leg_stops_the_tour_being_joint(self):
        """Today's stricter rule: a tour with one non-joint leg is left individual."""
        linked_trips, tours = _build_joint_tour_frames(
            [
                (1, P1, 101, 1001),
                (2, P1, 101, None),  # solo leg
                (3, P2, 201, 1001),
                (4, P2, 201, 1002),
            ]
        )

        _, tours_out = identify_joint_tours(linked_trips, tours)

        assert tours_out.filter(pl.col("joint_tour_id").is_not_null()).is_empty()

    def test_the_guard_rejects_a_joint_tour_with_no_joint_trip(self):
        """The tripwire, on a frame the public path cannot currently produce."""
        broken = self._frame([None, None], [9001, 9001])

        with pytest.raises(ValueError, match="hold no joint trip"):
            _validate_joint_tours_have_joint_trips(broken)

    @pytest.mark.parametrize(
        ("joint_trip_ids", "joint_tour_ids"),
        [
            pytest.param(
                [1001, None],
                [9001, 9001],
                id="a_partly_shared_tour_is_allowed_through",
            ),
            pytest.param(
                [1001, 1002],
                [9001, 9001],
                id="the_guard_passes_when_every_leg_is_joint",
            ),
            pytest.param([None], [None], id="a_trip_with_no_joint_tour_is_not_policed"),
        ],
    )
    def test_the_guard_admits_the_rest(self, joint_trip_ids, joint_tour_ids):
        """One shared leg is enough for canonical, and a solo trip is not policed.

        CT-RAMP cannot represent a half-shared tour and rejects it in its own
        formatter, but the survey can record one and canonical should not
        forbid it.
        """
        _validate_joint_tours_have_joint_trips(self._frame(joint_trip_ids, joint_tour_ids))
