"""Unit tests for CT-RAMP formatter.

Tests formatting, field corrections, and end-to-end transformation from
canonical survey data to CT-RAMP model format.
"""

from datetime import datetime, time
from typing import get_args

import polars as pl
import pytest

from data_canon.codebook.ctramp import (
    CTRAMPPersonType,
)
from data_canon.codebook.households import IncomeBroad
from data_canon.codebook.persons import (
    AgeCategory,
)
from data_canon.codebook.tours import TourCategory, TourDataQuality, TourDirection
from data_canon.codebook.trips import PurposeCategory
from data_canon.models.ctramp import (
    HouseholdCTRAMPModel,
    IndividualTourCTRAMPModel,
    IndividualTripCTRAMPModel,
    JointTourCTRAMPModel,
    JointTripCTRAMPModel,
    PersonCTRAMPModel,
)
from processing.formatting.ctramp.ctramp_config import CTRAMPConfig
from processing.formatting.ctramp.filters import _drop_invalid_tours
from processing.formatting.ctramp.format_ctramp import format_ctramp
from processing.formatting.ctramp.format_households import format_households
from processing.formatting.ctramp.format_joint_trips import format_joint_trip
from processing.formatting.ctramp.format_persons import format_persons
from processing.formatting.ctramp.format_tours import (
    format_individual_tour,
    format_joint_tour,
)
from processing.formatting.ctramp.format_trips import (
    format_individual_trip,
)
from tests.fixtures import (
    create_family_household,
    create_household,
    create_linked_trip,
    create_person,
    create_retired_household,
    create_single_adult_household,
    create_tour,
    create_university_student_household,
    days_for_persons,
    empty_joint_tours,
    empty_joint_trips,
    empty_linked_trips,
    empty_tours,
    empty_unlinked_trips,
    get_tour_schema,
)


def get_required_non_null_fields(model):
    """Get field names that are required and don't allow None.

    Args:
        model: Pydantic BaseModel class

    Returns:
        List of field names that are required (no | None in type)
    """
    required = []
    for name, field_info in model.model_fields.items():
        # Check if None is allowed in the type annotation
        # get_args returns empty tuple for non-generic types
        type_args = get_args(field_info.annotation)
        # If type_args is not empty and None is in the args, skip it
        if type_args and type(None) in type_args:
            continue  # Skip optional fields (have | None)
        required.append(name)
    return required


@pytest.fixture
def standard_config():
    """Standard test configuration with explicit parameters."""
    return CTRAMPConfig(
        income_low_threshold=30000,  # $30k ($2000, MTC)
        income_med_threshold=60000,  # $60k ($2000, MTC)
        income_high_threshold=100000,  # $100k ($2000, MTC)
        income_survey_year_to_ctramp_year=0.5319148936,
        age_adult=4,  # AGE_18_TO_24 = category 4 (18+ are adults)
    )


class TestEndToEndFormatting:
    """Tests for end-to-end CT-RAMP formatting."""

    def test_single_adult_household(self, standard_config):
        """Test formatting of single adult household."""
        (
            households,
            persons,
        ) = create_single_adult_household()

        result = format_ctramp(
            persons,
            households,
            linked_trips=empty_linked_trips(),
            tours=empty_tours(),
            joint_trips=empty_joint_trips(),
            unlinked_trips=empty_unlinked_trips(),
            joint_tours=empty_joint_tours(),
            days=days_for_persons(persons),
            income_low_threshold=standard_config.income_low_threshold,
            income_med_threshold=standard_config.income_med_threshold,
            income_high_threshold=standard_config.income_high_threshold,
            income_survey_year_to_ctramp_year=standard_config.income_survey_year_to_ctramp_year,
        )

        households_ctramp = result["households_ctramp"]
        persons_ctramp = result["persons_ctramp"]

        assert len(households_ctramp) == 1
        assert len(persons_ctramp) == 1
        # CT-RAMP ids are person-day encoded: hh_id * 100 + day_num.
        assert households_ctramp["hh_id"][0] == 101
        assert persons_ctramp["type"][0] == CTRAMPPersonType.FULL_TIME_WORKER.label

    def test_family_household(self, standard_config):
        """Test formatting of family household with multiple person types."""
        households, persons = create_family_household()

        result = format_ctramp(
            persons,
            households,
            linked_trips=empty_linked_trips(),
            tours=empty_tours(),
            joint_trips=empty_joint_trips(),
            unlinked_trips=empty_unlinked_trips(),
            joint_tours=empty_joint_tours(),
            days=days_for_persons(persons),
            income_low_threshold=standard_config.income_low_threshold,
            income_med_threshold=standard_config.income_med_threshold,
            income_high_threshold=standard_config.income_high_threshold,
            income_survey_year_to_ctramp_year=standard_config.income_survey_year_to_ctramp_year,
        )

        households_ctramp = result["households_ctramp"]
        persons_ctramp = result["persons_ctramp"]

        assert len(households_ctramp) == 1
        assert len(persons_ctramp) == 4

        # Check person types
        person_types = persons_ctramp["type"].to_list()
        assert CTRAMPPersonType.FULL_TIME_WORKER.label in person_types
        assert CTRAMPPersonType.PART_TIME_WORKER.label in person_types
        assert CTRAMPPersonType.STUDENT_DRIVING_AGE.label in person_types
        assert CTRAMPPersonType.STUDENT_NON_DRIVING_AGE.label in person_types

    def test_retired_household(self, standard_config):
        """Test formatting of retired household."""
        households, persons = create_retired_household()

        result = format_ctramp(
            persons,
            households,
            linked_trips=empty_linked_trips(),
            tours=empty_tours(),
            joint_trips=empty_joint_trips(),
            unlinked_trips=empty_unlinked_trips(),
            joint_tours=empty_joint_tours(),
            days=days_for_persons(persons),
            income_low_threshold=standard_config.income_low_threshold,
            income_med_threshold=standard_config.income_med_threshold,
            income_high_threshold=standard_config.income_high_threshold,
            income_survey_year_to_ctramp_year=standard_config.income_survey_year_to_ctramp_year,
        )

        persons_ctramp = result["persons_ctramp"]

        assert len(persons_ctramp) == 2
        assert all(pt == CTRAMPPersonType.RETIRED.label for pt in persons_ctramp["type"].to_list())

    def test_university_student_household(self, standard_config):
        """Test formatting of university student household."""
        (
            households,
            persons,
        ) = create_university_student_household()

        result = format_ctramp(
            persons,
            households,
            linked_trips=empty_linked_trips(),
            tours=empty_tours(),
            joint_trips=empty_joint_trips(),
            unlinked_trips=empty_unlinked_trips(),
            joint_tours=empty_joint_tours(),
            days=days_for_persons(persons),
            income_low_threshold=standard_config.income_low_threshold,
            income_med_threshold=standard_config.income_med_threshold,
            income_high_threshold=standard_config.income_high_threshold,
            income_survey_year_to_ctramp_year=standard_config.income_survey_year_to_ctramp_year,
        )

        persons_ctramp = result["persons_ctramp"]

        assert len(persons_ctramp) == 1
        assert persons_ctramp["type"][0] == CTRAMPPersonType.UNIVERSITY_STUDENT.label

    def test_drop_missing_taz(self, standard_config):
        """Test filtering households without valid TAZ."""
        households = pl.DataFrame(
            [
                create_household(hh_id=1, home_taz=100),
                create_household(hh_id=2, home_taz=None),
                create_household(hh_id=3, home_taz=-1),
            ]
        )

        persons = pl.DataFrame(
            [
                create_person(person_id=101, hh_id=1),
                create_person(person_id=201, hh_id=2),
                create_person(person_id=301, hh_id=3),
            ]
        )

        result = format_ctramp(
            persons,
            households,
            linked_trips=empty_linked_trips(),
            tours=empty_tours(),
            joint_trips=empty_joint_trips(),
            unlinked_trips=empty_unlinked_trips(),
            joint_tours=empty_joint_tours(),
            days=days_for_persons(persons),
            income_low_threshold=standard_config.income_low_threshold,
            income_med_threshold=standard_config.income_med_threshold,
            income_high_threshold=standard_config.income_high_threshold,
            income_survey_year_to_ctramp_year=standard_config.income_survey_year_to_ctramp_year,
            drop_missing_taz=True,
        )

        households_ctramp = result["households_ctramp"]
        persons_ctramp = result["persons_ctramp"]

        # Only household 1 should remain (CT-RAMP id = hh_id * 100 + day_num).
        assert len(households_ctramp) == 1
        assert len(persons_ctramp) == 1
        assert households_ctramp["hh_id"][0] == 101
        assert persons_ctramp["hh_id"][0] == 101

    def test_keep_missing_taz_when_disabled(self, standard_config):
        """Test keeping households without TAZ when filtering is disabled."""
        households = pl.DataFrame(
            [
                create_household(hh_id=1, home_taz=100),
                create_household(hh_id=2, home_taz=None),
            ]
        )

        persons = pl.DataFrame(
            [
                create_person(person_id=101, hh_id=1),
                create_person(person_id=201, hh_id=2),
            ]
        )

        result = format_ctramp(
            persons,
            households,
            linked_trips=empty_linked_trips(),
            tours=empty_tours(),
            joint_trips=empty_joint_trips(),
            unlinked_trips=empty_unlinked_trips(),
            joint_tours=empty_joint_tours(),
            days=days_for_persons(persons),
            income_low_threshold=standard_config.income_low_threshold,
            income_med_threshold=standard_config.income_med_threshold,
            income_high_threshold=standard_config.income_high_threshold,
            income_survey_year_to_ctramp_year=standard_config.income_survey_year_to_ctramp_year,
            drop_missing_taz=False,
        )

        households_ctramp = result["households_ctramp"]
        persons_ctramp = result["persons_ctramp"]

        # Both households should remain
        assert len(households_ctramp) == 2
        assert len(persons_ctramp) == 2


class TestDropInvalidTours:
    """Tests for dropping unusable tours in the CT-RAMP formatter.

    Drops tours that are not VALID or not COMPLETE, mirroring the DaySim
    formatter, so single-trip/loop tours (which carry a null tour_purpose) do
    not leak into CT-RAMP output as the OTHDISCR catch-all, and both outputs
    keep the same tours.
    """

    def _linked_trips(self, tour_ids: list[int]) -> pl.DataFrame:
        """One non-joint linked trip per tour id."""
        return pl.DataFrame(
            {
                "linked_trip_id": [t * 10 for t in tour_ids],
                "tour_id": tour_ids,
                "joint_trip_id": [None] * len(tour_ids),
            },
            schema={
                "linked_trip_id": pl.Int64,
                "tour_id": pl.Int64,
                "joint_trip_id": pl.Int64,
            },
        )

    def _empty_joint_trips(self) -> pl.DataFrame:
        return pl.DataFrame({"joint_trip_id": []}, schema={"joint_trip_id": pl.Int64})

    def test_drops_invalid_and_partial_and_cascades(self):
        """Non-VALID and non-COMPLETE tours (and their trips) are removed."""
        tours = pl.DataFrame(
            {
                "tour_id": [1, 2, 3, 4],
                "tour_data_quality": [
                    TourDataQuality.VALID.value,
                    TourDataQuality.NO_DESTINATION.value,  # invalid
                    TourDataQuality.VALID.value,
                    TourDataQuality.VALID.value,
                ],
                "tour_category": [
                    TourCategory.COMPLETE.value,
                    TourCategory.PARTIAL_BOTH.value,
                    TourCategory.COMPLETE.value,
                    TourCategory.PARTIAL_END.value,  # valid but partial
                ],
            }
        )
        tours_out, linked_out, _ = _drop_invalid_tours(
            tours, self._linked_trips([1, 2, 3, 4]), self._empty_joint_trips()
        )
        # Tour 2 (invalid) and tour 4 (valid-but-partial) dropped
        assert sorted(tours_out["tour_id"].to_list()) == [1, 3]
        assert sorted(linked_out["tour_id"].to_list()) == [1, 3]

    def test_all_valid_complete_keeps_everything(self):
        """VALID + COMPLETE tours are all kept."""
        tours = pl.DataFrame(
            {
                "tour_id": [1, 2],
                "tour_data_quality": [TourDataQuality.VALID.value] * 2,
                "tour_category": [TourCategory.COMPLETE.value] * 2,
            }
        )
        tours_out, linked_out, _ = _drop_invalid_tours(
            tours, self._linked_trips([1, 2]), self._empty_joint_trips()
        )
        assert sorted(tours_out["tour_id"].to_list()) == [1, 2]
        assert sorted(linked_out["tour_id"].to_list()) == [1, 2]

    def test_only_category_present_still_drops_partial(self):
        """A frame with tour_category but no tour_data_quality still drops partials."""
        tours = pl.DataFrame(
            {
                "tour_id": [1, 2],
                "tour_category": [TourCategory.COMPLETE.value, TourCategory.PARTIAL_END.value],
            }
        )
        tours_out, linked_out, _ = _drop_invalid_tours(
            tours, self._linked_trips([1, 2]), self._empty_joint_trips()
        )
        assert tours_out["tour_id"].to_list() == [1]
        assert linked_out["tour_id"].to_list() == [1]

    def test_cascades_to_joint_trips(self):
        """A joint trip belonging only to a dropped tour is removed."""
        linked_trips = pl.DataFrame(
            {
                "linked_trip_id": [10, 20],
                "tour_id": [1, 2],
                "joint_trip_id": [None, 500],
            },
            schema={
                "linked_trip_id": pl.Int64,
                "tour_id": pl.Int64,
                "joint_trip_id": pl.Int64,
            },
        )
        joint_trips = pl.DataFrame({"joint_trip_id": [500]}, schema={"joint_trip_id": pl.Int64})
        tours = pl.DataFrame(
            {
                "tour_id": [1, 2],
                "tour_data_quality": [
                    TourDataQuality.VALID.value,
                    TourDataQuality.NO_DESTINATION.value,
                ],
                "tour_category": [TourCategory.COMPLETE.value, TourCategory.PARTIAL_BOTH.value],
            }
        )

        _, _, joint_trips_out = _drop_invalid_tours(tours, linked_trips, joint_trips)
        # Joint trip 500 belonged only to dropped tour 2, so it is removed
        assert joint_trips_out["joint_trip_id"].to_list() == []

    def test_no_filter_columns_is_noop(self):
        """Tours with neither quality nor category column are left untouched."""
        tours = pl.DataFrame({"tour_id": [1, 2]})
        tours_out, linked_out, _ = _drop_invalid_tours(
            tours, self._linked_trips([1, 2]), self._empty_joint_trips()
        )
        assert sorted(tours_out["tour_id"].to_list()) == [1, 2]
        assert sorted(linked_out["tour_id"].to_list()) == [1, 2]


class TestColumnPresence:
    """Tests to ensure all required CT-RAMP columns are present."""

    def test_household_columns(self, standard_config):
        """Test that all required household columns are present."""
        households, persons = create_single_adult_household()
        tours = pl.DataFrame([], schema=get_tour_schema())
        result = format_households(households, persons, tours, standard_config)

        required_columns = get_required_non_null_fields(HouseholdCTRAMPModel)
        for col in required_columns:
            assert col in result.columns, f"Missing required column: {col}"

    def test_person_columns(self, standard_config):
        """Test that all required person columns are present."""
        _, persons = create_single_adult_household()
        result = format_persons(persons, pl.DataFrame(), standard_config)

        required_columns = get_required_non_null_fields(PersonCTRAMPModel)
        for col in required_columns:
            assert col in result.columns, f"Missing required column: {col}"

    def test_individual_tour_columns(self, standard_config):
        """Test that all required individual tour columns are present."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [create_tour(tour_id=1001, person_id=101, hh_id=1)],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10001,
                    tour_id=1001,
                    person_id=101,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10002, tour_id=1001, person_id=101, tour_direction=TourDirection.INBOUND
                ),
            ]
        )

        households_formatted = format_households(households, persons, tours, standard_config)
        result = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        required_columns = get_required_non_null_fields(IndividualTourCTRAMPModel)
        for col in required_columns:
            assert col in result.columns, f"Missing required column: {col}"

    def test_individual_trip_columns(self, standard_config):
        """Test that all required individual trip columns are present."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [create_tour(tour_id=1001, person_id=101, hh_id=1)],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10001,
                    tour_id=1001,
                    person_id=101,
                    tour_direction=TourDirection.OUTBOUND,
                    depart_time=datetime.combine(datetime(2024, 1, 1), time(8, 30)),
                ),
            ]
        )

        households_formatted = format_households(households, persons, tours, standard_config)
        tours_formatted = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )
        result = format_individual_trip(
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            tours_ctramp=tours_formatted,
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        required_columns = get_required_non_null_fields(IndividualTripCTRAMPModel)
        # parking_taz is optional in the model, so don't check for it
        for col in required_columns:
            assert col in result.columns, f"Missing required column: {col}"

    def test_joint_tour_columns(self, standard_config):
        """Test that all required joint tour columns are present."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame(
            [
                create_person(person_id=101, hh_id=1, person_num=1, age=AgeCategory.AGE_35_TO_44),
                create_person(person_id=102, hh_id=1, person_num=2, age=AgeCategory.AGE_5_TO_15),
            ]
        )
        tours = pl.DataFrame(
            [
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    joint_tour_id=9001,
                    num_travelers=2,
                    tour_purpose=PurposeCategory.SHOP,
                ),
                create_tour(
                    tour_id=1002,
                    person_id=102,
                    hh_id=1,
                    joint_tour_id=9001,
                    num_travelers=2,
                    tour_purpose=PurposeCategory.SHOP,
                ),
            ],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10001,
                    tour_id=1001,
                    person_id=101,
                    tour_direction=TourDirection.OUTBOUND,
                    joint_tour_id=9001,
                ),
                create_linked_trip(
                    trip_id=10002,
                    tour_id=1001,
                    person_id=101,
                    tour_direction=TourDirection.INBOUND,
                    joint_tour_id=9001,
                ),
            ]
        )

        households_formatted = format_households(households, persons, tours, standard_config)
        persons_formatted = format_persons(persons, pl.DataFrame(), standard_config)
        result = format_joint_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            joint_tours_canonical=pl.DataFrame(),
            persons_canonical=persons_formatted,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        required_columns = get_required_non_null_fields(JointTourCTRAMPModel)
        for col in required_columns:
            assert col in result.columns, f"Missing required column: {col}"

    def test_joint_trip_columns(self, standard_config):
        """Test that all required joint trip columns are present."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1, person_num=1)])
        tours = pl.DataFrame(
            [
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    joint_tour_id=9001,
                    tour_purpose=PurposeCategory.SHOP,
                ),
            ],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10001,
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    joint_tour_id=9001,
                    joint_trip_id=90001,
                    tour_direction=TourDirection.OUTBOUND,
                )
            ]
        )
        joint_trips = pl.DataFrame(
            [
                {
                    "joint_trip_id": 90001,
                    "joint_tour_id": 9001,
                    "hh_id": 1,
                    "num_joint_travelers": 1,
                }
            ]
        )

        households_formatted = format_households(households, persons, tours, standard_config)
        result = format_joint_trip(
            joint_trips_canonical=joint_trips,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            tours_canonical=tours,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        required_columns = get_required_non_null_fields(JointTripCTRAMPModel)
        for col in required_columns:
            assert col in result.columns, f"Missing required column: {col}"


class TestWeightsAndSampleRateIntegration:
    """Integration tests for weight fields and sampleRate calculation in CTRAMP output."""

    def test_household_weight_and_samplerate(self, standard_config):
        """Test hh_weight and sampleRate are output when weight exists."""
        households = pl.DataFrame(
            [
                create_household(hh_id=1, hh_weight=2.5),
                create_household(hh_id=2, hh_weight=4.0),
            ]
        )
        persons = pl.DataFrame(
            [
                create_person(person_id=101, hh_id=1),
                create_person(person_id=201, hh_id=2),
            ]
        )
        tours = pl.DataFrame([], schema=get_tour_schema())

        result = format_households(households, persons, tours, standard_config)

        # Verify weight column present
        assert "hh_weight" in result.columns
        assert "sampleRate" in result.columns

        # Verify weight values passed through
        assert result.filter(pl.col("hh_id") == 1)["hh_weight"][0] == 2.5
        assert result.filter(pl.col("hh_id") == 2)["hh_weight"][0] == 4.0

        # Verify sampleRate = 1/weight
        assert result.filter(pl.col("hh_id") == 1)["sampleRate"][0] == pytest.approx(1 / 2.5)
        assert result.filter(pl.col("hh_id") == 2)["sampleRate"][0] == pytest.approx(1 / 4.0)

    def test_household_samplerate_null_when_zero_weight(self, standard_config):
        """Test sampleRate is None when hh_weight is zero."""
        households = pl.DataFrame(
            [
                create_household(hh_id=1, hh_weight=0.0),
                create_household(hh_id=2, hh_weight=2.0),
            ]
        )
        persons = pl.DataFrame(
            [
                create_person(person_id=101, hh_id=1),
                create_person(person_id=201, hh_id=2),
            ]
        )
        tours = pl.DataFrame([], schema=get_tour_schema())

        result = format_households(households, persons, tours, standard_config)

        # Zero weight should result in None sampleRate
        assert result.filter(pl.col("hh_id") == 1)["sampleRate"][0] is None
        assert result.filter(pl.col("hh_id") == 2)["sampleRate"][0] == pytest.approx(0.5)

    def test_household_samplerate_null_when_null_weight(self, standard_config):
        """Test sampleRate is None when hh_weight is null."""
        households = pl.DataFrame(
            {
                "hh_id": [1, 2],
                "home_taz": [100, 200],
                "num_people": [1, 2],
                "num_vehicles": [1, 1],
                "num_workers": [1, 1],
                "income_bin": [IncomeBroad.INCOME_75TO100.value] * 2,
                "hh_weight": [None, 3.0],
                "home_lat": [37.7, 37.8],
                "home_lon": [-122.4, -122.5],
                "home_maz": [None, None],
                "home_walk_subzone": [None, None],
                "residence_type": [None, None],
                "residence_rent_own": [None, None],
            }
        )
        persons = pl.DataFrame(
            [
                create_person(person_id=101, hh_id=1),
                create_person(person_id=201, hh_id=2),
            ]
        )
        tours = pl.DataFrame([], schema=get_tour_schema())

        result = format_households(households, persons, tours, standard_config)

        # Null weight should result in None sampleRate
        assert result.filter(pl.col("hh_id") == 1)["sampleRate"][0] is None
        assert result.filter(pl.col("hh_id") == 2)["sampleRate"][0] == pytest.approx(1 / 3.0)

    def test_household_no_weight_columns_when_missing(self, standard_config):
        """Test hh_weight and sampleRate absent when not in input."""
        households = pl.DataFrame(
            {
                "hh_id": [1],
                "home_taz": [100],
                "num_people": [1],
                "num_vehicles": [1],
                "num_workers": [1],
                "income_bin": [IncomeBroad.INCOME_75TO100.value],
                "home_lat": [37.7],
                "home_lon": [-122.4],
                "home_maz": [None],
                "home_walk_subzone": [None],
                "residence_type": [None],
                "residence_rent_own": [None],
                # NO hh_weight column
            }
        )
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame([], schema=get_tour_schema())

        result = format_households(households, persons, tours, standard_config)

        # Weight columns should not be present
        assert "hh_weight" not in result.columns
        assert "sampleRate" not in result.columns

    def test_person_weight_and_samplerate(self, standard_config):
        """Test person_weight and sampleRate are output when weight exists."""
        persons = pl.DataFrame(
            [
                {**create_person(person_id=101, hh_id=1), "person_weight": 1.5},
                {**create_person(person_id=102, hh_id=1), "person_weight": 2.0},
            ]
        )
        tours = pl.DataFrame()

        result = format_persons(persons, tours, standard_config)

        # Verify weight column present
        assert "person_weight" in result.columns
        assert "sampleRate" in result.columns

        # Verify sampleRate = 1/weight
        assert result.filter(pl.col("person_id") == 101)["sampleRate"][0] == pytest.approx(1 / 1.5)
        assert result.filter(pl.col("person_id") == 102)["sampleRate"][0] == pytest.approx(1 / 2.0)

    def test_person_samplerate_null_when_zero_weight(self, standard_config):
        """Test person sampleRate is None when person_weight is zero."""
        persons = pl.DataFrame(
            [
                {**create_person(person_id=101, hh_id=1), "person_weight": 0.0},
                {**create_person(person_id=102, hh_id=1), "person_weight": 1.5},
            ]
        )
        tours = pl.DataFrame()

        result = format_persons(persons, tours, standard_config)

        assert result.filter(pl.col("person_id") == 101)["sampleRate"][0] is None
        assert result.filter(pl.col("person_id") == 102)["sampleRate"][0] == pytest.approx(1 / 1.5)

    def test_person_no_weight_columns_when_missing(self, standard_config):
        """Test person_weight and sampleRate absent when not in input."""
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)]).drop("person_weight")
        tours = pl.DataFrame()

        result = format_persons(persons, tours, standard_config)

        # Weight columns should not be present
        assert "person_weight" not in result.columns
        assert "sampleRate" not in result.columns

    def test_tour_weight_and_samplerate(self, standard_config):
        """Test tour_weight and sampleRate are output when weight exists."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [
                {
                    **create_tour(tour_id=1001, person_id=101, hh_id=1, tour_num=1),
                    "tour_weight": 3.0,
                },
                {
                    **create_tour(tour_id=1002, person_id=101, hh_id=1, tour_num=2),
                    "tour_weight": 5.0,
                },
            ],
            schema={**get_tour_schema(), "tour_weight": pl.Float64},
        )
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10001, tour_id=1001, tour_direction=TourDirection.OUTBOUND
                ),
                create_linked_trip(
                    trip_id=10002, tour_id=1001, tour_direction=TourDirection.INBOUND
                ),
                create_linked_trip(
                    trip_id=10003, tour_id=1002, tour_direction=TourDirection.OUTBOUND
                ),
                create_linked_trip(
                    trip_id=10004, tour_id=1002, tour_direction=TourDirection.INBOUND
                ),
            ]
        )

        households_formatted = format_households(households, persons, tours, standard_config)
        result = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        # Verify weight column present
        assert "tour_weight" in result.columns
        assert "sampleRate" in result.columns

        # Verify sampleRate = 1/weight (CTRAMP tour_id is 0-based: 0, 1)
        assert result.filter(pl.col("tour_id") == 0)["sampleRate"][0] == pytest.approx(1 / 3.0)
        assert result.filter(pl.col("tour_id") == 1)["sampleRate"][0] == pytest.approx(1 / 5.0)

    def test_tour_samplerate_null_when_zero_weight(self, standard_config):
        """Test tour sampleRate is None when tour_weight is zero."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [
                {
                    **create_tour(tour_id=1001, person_id=101, hh_id=1, tour_num=1),
                    "tour_weight": 0.0,
                },
                {
                    **create_tour(tour_id=1002, person_id=101, hh_id=1, tour_num=2),
                    "tour_weight": 2.5,
                },
            ],
            schema={**get_tour_schema(), "tour_weight": pl.Float64},
        )
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10001, tour_id=1001, tour_direction=TourDirection.OUTBOUND
                ),
                create_linked_trip(
                    trip_id=10002, tour_id=1001, tour_direction=TourDirection.INBOUND
                ),
                create_linked_trip(
                    trip_id=10003, tour_id=1002, tour_direction=TourDirection.OUTBOUND
                ),
                create_linked_trip(
                    trip_id=10004, tour_id=1002, tour_direction=TourDirection.INBOUND
                ),
            ]
        )

        households_formatted = format_households(households, persons, tours, standard_config)
        result = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        # CTRAMP tour_id is 0-based: 0, 1
        assert result.filter(pl.col("tour_id") == 0)["sampleRate"][0] is None
        assert result.filter(pl.col("tour_id") == 1)["sampleRate"][0] == pytest.approx(1 / 2.5)

    def test_tour_no_weight_columns_when_missing(self, standard_config):
        """Test tour_weight and sampleRate absent when not in input."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [create_tour(tour_id=1001, person_id=101, hh_id=1)],
            schema=get_tour_schema(),
        ).drop("tour_weight")
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10001, tour_id=1001, tour_direction=TourDirection.OUTBOUND
                ),
                create_linked_trip(
                    trip_id=10002, tour_id=1001, tour_direction=TourDirection.INBOUND
                ),
            ]
        )

        households_formatted = format_households(households, persons, tours, standard_config)
        result = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        # Weight columns should not be present
        assert "tour_weight" not in result.columns
        assert "sampleRate" not in result.columns

    def test_trip_weight_and_samplerate(self, standard_config):
        """Test trip_weight and sampleRate are output when linked_trip_weight exists."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [create_tour(tour_id=1001, person_id=101, hh_id=1)],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                {
                    **create_linked_trip(
                        linked_trip_id=10001, tour_id=1001, tour_direction=TourDirection.OUTBOUND
                    ),
                    "linked_trip_weight": 2.0,
                },
                {
                    **create_linked_trip(
                        linked_trip_id=10002, tour_id=1001, tour_direction=TourDirection.INBOUND
                    ),
                    "linked_trip_weight": 4.0,
                },
            ]
        )

        households_formatted = format_households(households, persons, tours, standard_config)
        tours_formatted = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )
        result = format_individual_trip(
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            tours_ctramp=tours_formatted,
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        # Verify trip_weight column present (renamed from linked_trip_weight)
        assert "trip_weight" in result.columns
        assert "sampleRate" in result.columns

        # Verify sampleRate = 1/weight
        # Sort by trip_weight to ensure predictable order
        result = result.sort("trip_weight")
        assert result["trip_weight"][0] == 2.0
        assert result["trip_weight"][1] == 4.0
        assert result["sampleRate"][0] == pytest.approx(1 / 2.0)
        assert result["sampleRate"][1] == pytest.approx(1 / 4.0)

    def test_trip_samplerate_null_when_zero_weight(self, standard_config):
        """Test trip sampleRate is None when linked_trip_weight is zero."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [create_tour(tour_id=1001, person_id=101, hh_id=1)],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                {
                    **create_linked_trip(
                        linked_trip_id=10001, tour_id=1001, tour_direction=TourDirection.OUTBOUND
                    ),
                    "linked_trip_weight": 0.0,
                },
                {
                    **create_linked_trip(
                        linked_trip_id=10002, tour_id=1001, tour_direction=TourDirection.INBOUND
                    ),
                    "linked_trip_weight": 3.5,
                },
            ]
        )

        households_formatted = format_households(households, persons, tours, standard_config)
        tours_formatted = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )
        result = format_individual_trip(
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            tours_ctramp=tours_formatted,
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        # Sort by trip_weight to ensure predictable order (0 first, then 3.5)
        result = result.sort("trip_weight")
        assert result["trip_weight"][0] == 0.0
        assert result["sampleRate"][0] is None
        assert result["trip_weight"][1] == 3.5
        assert result["sampleRate"][1] == pytest.approx(1 / 3.5)

    def test_trip_no_weight_columns_when_missing(self, standard_config):
        """Test trip_weight and sampleRate absent when linked_trip_weight not in input."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [create_tour(tour_id=1001, person_id=101, hh_id=1)],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    linked_trip_id=10001, tour_id=1001, tour_direction=TourDirection.OUTBOUND
                ),
                create_linked_trip(
                    linked_trip_id=10002, tour_id=1001, tour_direction=TourDirection.INBOUND
                ),
            ]
        ).drop("linked_trip_weight")

        households_formatted = format_households(households, persons, tours, standard_config)
        tours_formatted = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )
        result = format_individual_trip(
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            tours_ctramp=tours_formatted,
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        # Weight columns should not be present
        assert "trip_weight" not in result.columns
        assert "sampleRate" not in result.columns

    def test_joint_tours_weight_fields(self, standard_config):
        """Test joint tours carry joint_tour_weight and derive sampleRate from it.

        Joint tours are their own entity, so their weight comes from the canonical
        joint-tours table as ``joint_tour_weight``. That weight is the SUM over
        participants, so ``sampleRate`` is the inverse of the member *mean* --
        ``n_member_tours / weight`` -- not 1/weight.
        """
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame(
            [
                create_person(person_id=101, hh_id=1, person_num=1),
                create_person(person_id=102, hh_id=1, person_num=2),
            ]
        )
        # Two participants, so the joint tour has two member tours and its summed
        # weight divides back to a per-tour rate by that count.
        tours = pl.DataFrame(
            [
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    joint_tour_id=5001,
                    tour_purpose=PurposeCategory.SOCIALREC,
                    num_travelers=2,
                ),
                create_tour(
                    tour_id=1002,
                    person_id=102,
                    hh_id=1,
                    joint_tour_id=5001,
                    tour_purpose=PurposeCategory.SOCIALREC,
                    num_travelers=2,
                ),
            ],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10001,
                    person_id=101,
                    tour_id=1001,
                    joint_tour_id=5001,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10002,
                    person_id=101,
                    tour_id=1001,
                    joint_tour_id=5001,
                    tour_direction=TourDirection.INBOUND,
                ),
                create_linked_trip(
                    trip_id=10003,
                    person_id=102,
                    person_num=2,
                    tour_id=1002,
                    joint_tour_id=5001,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10004,
                    person_id=102,
                    person_num=2,
                    tour_id=1002,
                    joint_tour_id=5001,
                    tour_direction=TourDirection.INBOUND,
                ),
            ]
        )

        joint_tours_canonical = pl.DataFrame(
            {
                "joint_tour_id": [5001],
                "joint_tour_weight": [5.0],
            }
        )

        households_formatted = format_households(households, persons, tours, standard_config)
        result = format_joint_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            joint_tours_canonical=joint_tours_canonical,
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        assert "joint_tour_weight" in result.columns
        assert "sampleRate" in result.columns
        assert result["joint_tour_weight"][0] == pytest.approx(5.0)
        assert result["sampleRate"][0] == pytest.approx(2 / 5.0)

    def test_joint_trips_weight_fields(self, standard_config):
        """Test joint trips preserve their explicit weight and derive sampleRate."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame(
            [
                create_person(person_id=101, hh_id=1, person_num=1),
                create_person(person_id=102, hh_id=1, person_num=2),
            ]
        )
        tours = pl.DataFrame(
            [
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    joint_tour_id=5001,
                    tour_purpose=PurposeCategory.SOCIALREC,
                    num_travelers=2,
                ),
            ],
            schema=get_tour_schema(),
        )
        # Two people travelling together: each joint trip has two member trips, so
        # the summed weight divides back to a per-person rate by that count.
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10001,
                    person_id=101,
                    tour_id=1001,
                    joint_tour_id=5001,
                    joint_trip_id=8001,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10002,
                    person_id=101,
                    tour_id=1001,
                    joint_tour_id=5001,
                    joint_trip_id=8002,
                    tour_direction=TourDirection.INBOUND,
                ),
                create_linked_trip(
                    trip_id=10003,
                    person_id=102,
                    person_num=2,
                    tour_id=1002,
                    joint_tour_id=5001,
                    joint_trip_id=8001,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10004,
                    person_id=102,
                    person_num=2,
                    tour_id=1002,
                    joint_tour_id=5001,
                    joint_trip_id=8002,
                    tour_direction=TourDirection.INBOUND,
                ),
            ]
        )

        # Create aggregated joint trips
        joint_trips = (
            trips.filter(pl.col("joint_trip_id").is_not_null())
            .group_by("joint_trip_id")
            .agg(
                [
                    pl.col("hh_id").first(),
                    pl.col("tour_id").first(),
                    pl.col("joint_tour_id").first(),
                    pl.col("o_purpose_category").first(),
                    pl.col("d_purpose_category").first(),
                    pl.col("o_lat").mean().alias("o_lat"),
                    pl.col("o_lon").mean().alias("o_lon"),
                    pl.col("d_lat").mean().alias("d_lat"),
                    pl.col("d_lon").mean().alias("d_lon"),
                    pl.col("o_taz").first().alias("o_taz"),
                    pl.col("d_taz").first().alias("d_taz"),
                    pl.col("mode_type").first(),
                    pl.col("depart_time").first(),
                    pl.col("arrive_time").first(),
                    pl.col("tour_direction").first(),
                    pl.col("num_travelers").max().alias("num_joint_travelers"),
                ]
            )
            .with_columns(
                pl.lit(4.0).alias("joint_trip_weight"),
            )
        )

        households_formatted = format_households(households, persons, tours, standard_config)
        result = format_joint_trip(
            joint_trips_canonical=joint_trips,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            tours_canonical=tours,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        assert result["joint_trip_weight"][0] == 4.0
        assert result["sampleRate"][0] == pytest.approx(0.5)
        # CT-RAMP's own expansion must reproduce the weight.
        assert result["num_participants"][0] / result["sampleRate"][0] == pytest.approx(4.0)
