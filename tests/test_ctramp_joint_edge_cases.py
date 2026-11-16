"""Edge cases in how joint travel reaches the CT-RAMP output.

CT-RAMP represents joint travel only as joint tours; a survey detects sharing per
trip. These tests pin how the formatter bridges the two, and that the individual
and joint files partition travel rather than overlap it, which is what makes their
different weight conventions safe to add up.
"""

import polars as pl
import pytest

from data_canon.codebook.ctramp import CTRAMPModeType
from data_canon.codebook.tours import TourDirection
from data_canon.codebook.trips import PurposeCategory
from tests.fixtures.ctramp_joint import (
    MEMBER_WEIGHTS,
    format_all,
    joint_trips_table,
    mixed_tours,
    shared_tour,
    tours_frame,
    trips_for,
    two_person_household,
)

MIN_JOINT = 2


def _format_shared(config, purpose=PurposeCategory.SOCIALREC, **tour_kwargs):
    """Format a two-member shared tour of ``purpose`` with one round trip per member."""
    households, persons = two_person_household()
    tours = shared_tour(purpose, **tour_kwargs)
    trips = trips_for(tours)
    return format_all(tours, trips, persons, households, config), tours, trips


def _two_joint_tours(*, first_purpose: PurposeCategory, second_purpose: PurposeCategory):
    """Joint tours 5001 and 5002 in one household, each shared by both members."""
    return tours_frame(
        [
            {
                "tour_id": 1001 + 2 * n + i,
                "person_id": 101 + i,
                "person_num": 1 + i,
                "tour_num": 1 + n,
                "joint_tour_id": 5001 + n,
                "purpose": purpose,
                "weight": MEMBER_WEIGHTS[i],
            }
            for n, purpose in enumerate((first_purpose, second_purpose))
            for i in range(2)
        ]
    )


class TestSharingWithoutAJointTour:
    """Trip-level sharing that never became a joint tour."""

    def test_shared_trips_on_individual_tours_stay_individual(self, standard_config):
        """Shared legs on two individual tours stay in the individual files, not dropped."""
        households, persons = two_person_household()
        tours = shared_tour(PurposeCategory.SHOP, joint_tour_id=None)
        trips = trips_for(tours, joint_trip_ids=(801, 802))

        out = format_all(tours, trips, persons, households, standard_config)

        assert len(out["individual_trips"]) == len(trips)
        assert out["joint_tours"].is_empty()
        assert out["joint_trips"].is_empty()


def test_an_admissible_purpose_survives_as_joint(standard_config):
    """A same-purpose non-mandatory group stays joint (the purpose rules live in test_joint_ids)."""
    out, _, _ = _format_shared(standard_config)

    assert out["individual_tours"].is_empty()
    assert len(out["joint_tours"]) == 1
    assert not out["joint_trips"].is_empty()


class TestPartitionInvariants:
    """The individual and joint files partition travel; an overlap would inflate every total."""

    def test_a_mixed_household_splits_its_tours_between_the_files(self, standard_config):
        """With both files non-empty, every canonical tour is written exactly once."""
        households, persons = two_person_household()
        tours = mixed_tours()

        out = format_all(tours, trips_for(tours), persons, households, standard_config)

        individual_ids = set(tours.filter(pl.col("joint_tour_id").is_null())["tour_id"])
        assert set(out["individual_tours"]["_tour_id_canonical"]) == individual_ids
        assert len(out["joint_tours"]) == tours["joint_tour_id"].drop_nulls().n_unique()

    def test_every_joint_trip_resolves_to_a_written_joint_tour(self, standard_config):
        """No joint trip names a tour_id missing from the joint tour file."""
        out, _, _ = _format_shared(standard_config)

        assert not out["joint_trips"].is_empty()
        orphans = (
            out["joint_trips"]
            .select("hh_id", "tour_id")
            .unique()
            .join(out["joint_tours"], on=["hh_id", "tour_id"], how="anti")
        )
        assert orphans.is_empty()


class TestJointTourNumbering:
    """Joint tours are numbered 0-based per household, separately in the tour and trip files."""

    def test_a_dissolved_joint_tour_does_not_shift_the_survivor(self, standard_config):
        """A joint tour the cascade dissolved (null id) is not ranked; the survivor is 0."""
        households, persons = two_person_household()
        tours = _two_joint_tours(
            first_purpose=PurposeCategory.SOCIALREC, second_purpose=PurposeCategory.SOCIALREC
        ).with_columns(pl.when(pl.col("joint_tour_id") != 5001).then(pl.col("joint_tour_id")))

        out = format_all(tours, trips_for(tours), persons, households, standard_config)

        assert out["joint_tours"]["tour_id"].to_list() == [0]
        assert out["joint_trips"]["tour_id"].unique().to_list() == [0]

    def test_a_joint_tour_missing_from_the_trip_table_does_not_renumber_the_rest(
        self, standard_config
    ):
        """Joint trips lost to the TAZ filter must not shift the tour a trip points at.

        The two joint tours have different purposes, so each joint trip is checked
        against its tour's purpose rather than only for a matching id.
        """
        households, persons = two_person_household()
        tours = _two_joint_tours(
            first_purpose=PurposeCategory.SOCIALREC, second_purpose=PurposeCategory.SHOP
        )
        trips = trips_for(tours)
        # Joint tour 5001 keeps its tours and member trips but loses its joint trips.
        surviving = joint_trips_table(trips).filter(pl.col("joint_trip_id") // 10 != 5001)

        out = format_all(
            tours, trips, persons, households, standard_config, joint_trips_canonical=surviving
        )

        resolved = (
            out["joint_trips"]
            .select("hh_id", "tour_id", "tour_purpose")
            .unique()
            .join(
                out["joint_tours"]
                .select("hh_id", "tour_id", "tour_purpose")
                .rename({"tour_purpose": "_tour_file_purpose"}),
                on=["hh_id", "tour_id"],
                how="left",
            )
        )
        assert resolved["_tour_file_purpose"].to_list() == resolved["tour_purpose"].to_list()


@pytest.mark.parametrize(
    ("num_travelers", "expected_mode"),
    [
        pytest.param(2, CTRAMPModeType.SR2.value, id="no_companion"),
        pytest.param(3, CTRAMPModeType.SR3.value, id="unsurveyed_companion"),
    ],
)
def test_an_unsurveyed_companion_counts_in_the_mode_not_the_participants(
    num_travelers, expected_mode, standard_config
):
    """num_participants counts surveyed members (it scales the weight); the mode counts all."""
    out, _, _ = _format_shared(standard_config, num_travelers=num_travelers)

    assert out["joint_trips"]["num_participants"].unique().to_list() == [MIN_JOINT]
    assert out["joint_trips"]["trip_mode"].unique().to_list() == [expected_mode]


class TestJointWeightExpansion:
    """``num_participants / sampleRate`` returns the person-trips and person-tours."""

    def test_joint_trip_expansion_returns_the_person_trips(self, standard_config):
        """Each leg expands to the summed member weight, not twice the mean."""
        out, _, trips = _format_shared(standard_config)

        expanded = out["joint_trips"]["num_participants"] / out["joint_trips"]["sampleRate"]
        assert expanded.sum() == pytest.approx(trips["linked_trip_weight"].sum())
        assert expanded.to_list() == pytest.approx([sum(MEMBER_WEIGHTS)] * 2)

    def test_joint_tour_expansion_returns_the_person_tours(self, standard_config):
        """The same round trip on the tour side."""
        households, persons = two_person_household()
        tours = shared_tour(PurposeCategory.SOCIALREC)
        joint_tours_canonical = tours.group_by("joint_tour_id").agg(
            pl.col("hh_id").first(), pl.col("tour_weight").sum().alias("joint_tour_weight")
        )

        out = format_all(
            tours,
            trips_for(tours),
            persons,
            households,
            standard_config,
            joint_tours_canonical=joint_tours_canonical,
        )

        expanded = MIN_JOINT / out["joint_tours"]["sampleRate"]
        assert expanded.sum() == pytest.approx(tours["tour_weight"].sum())

    def test_a_zero_weight_joint_record_has_no_sample_rate(self, standard_config):
        """A rate of 1/0 is not a number; null says "no rate" without poisoning sums."""
        out, _, _ = _format_shared(standard_config, weights=(0.0, 0.0))

        assert out["joint_trips"]["joint_trip_weight"].to_list() == [0.0, 0.0]
        assert out["joint_trips"]["sampleRate"].null_count() == len(out["joint_trips"])


class TestPartlySharedToursAreRejectedHere:
    """CT-RAMP cannot express a half-shared tour, so this formatter refuses one."""

    def test_a_joint_tour_with_an_unshared_leg_is_rejected(self, standard_config):
        """An unshared leg of a joint tour would reach neither trip file, so it raises."""
        households, persons = two_person_household()
        tours = shared_tour(PurposeCategory.SOCIALREC)
        trips = trips_for(tours).with_columns(
            pl.when(pl.col("tour_direction") == TourDirection.OUTBOUND.value).then(
                pl.col("joint_trip_id")
            )
        )

        with pytest.raises(ValueError, match=r"partly-shared tour.*joint_tours: fully_joint"):
            format_all(tours, trips, persons, households, standard_config)

    def test_a_frame_with_no_joint_trips_at_all_is_not_judged(self, standard_config):
        """Absent trip-level sharing is missing information, not a partial tour."""
        households, persons = two_person_household()
        tours = shared_tour(PurposeCategory.SOCIALREC)
        trips = trips_for(tours).with_columns(pl.lit(None, dtype=pl.Int64).alias("joint_trip_id"))

        out = format_all(tours, trips, persons, households, standard_config)

        assert not out["joint_tours"].is_empty()
        assert out["joint_trips"].is_empty()
