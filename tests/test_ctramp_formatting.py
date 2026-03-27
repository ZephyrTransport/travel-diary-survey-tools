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
    FreeParkingChoice,
    JTFChoice,
    TourComposition,
    WFHChoice,
)
from data_canon.codebook.generic import BooleanYesNo
from data_canon.codebook.households import IncomeBroad
from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    Gender,
    Student,
)
from data_canon.codebook.tours import TourDirection
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
from processing.formatting.ctramp.format_ctramp import format_ctramp
from processing.formatting.ctramp.format_households import format_households
from processing.formatting.ctramp.format_persons import format_persons
from processing.formatting.ctramp.format_tours import (
    format_individual_tour,
    format_joint_tour,
)
from processing.formatting.ctramp.format_trips import (
    format_individual_trip,
    format_joint_trip,
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
    empty_joint_trips,
    empty_linked_trips,
    empty_tours,
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
        income_low_threshold=60000,  # $60k
        income_med_threshold=150000,  # $150k
        income_high_threshold=240000,  # $240k
        income_base_year_dollars=2023,
        age_adult=4,  # AGE_18_TO_24 = category 4 (18+ are adults)
    )


class TestFreeParkingChoice:
    """Tests for free parking choice in person formatting."""

    def test_free_parking_used(self, standard_config):
        """Test free parking choice when free parking is used."""
        persons = pl.DataFrame(
            [
                create_person(
                    commute_subsidy_use_3=BooleanYesNo.YES,
                    commute_subsidy_use_4=BooleanYesNo.NO,
                )
            ]
        )
        result = format_persons(persons, pl.DataFrame(), standard_config)
        assert result["fp_choice"][0] == FreeParkingChoice.PARK_FOR_FREE.value

    def test_discount_parking_used(self, standard_config):
        """Test free parking choice when discounted parking is used."""
        persons = pl.DataFrame(
            [
                create_person(
                    commute_subsidy_use_3=BooleanYesNo.NO,
                    commute_subsidy_use_4=BooleanYesNo.YES,
                )
            ]
        )
        result = format_persons(persons, pl.DataFrame(), standard_config)
        assert result["fp_choice"][0] == FreeParkingChoice.PARK_FOR_FREE.value

    def test_both_parking_subsidies_used(self, standard_config):
        """Test free parking choice when both parking subsidies are used."""
        persons = pl.DataFrame(
            [
                create_person(
                    commute_subsidy_use_3=BooleanYesNo.YES,
                    commute_subsidy_use_4=BooleanYesNo.YES,
                )
            ]
        )
        result = format_persons(persons, pl.DataFrame(), standard_config)
        assert result["fp_choice"][0] == FreeParkingChoice.PARK_FOR_FREE.value

    def test_no_parking_subsidy_used(self, standard_config):
        """Test no parking subsidy used."""
        persons = pl.DataFrame(
            [
                create_person(
                    commute_subsidy_use_3=BooleanYesNo.NO,
                    commute_subsidy_use_4=BooleanYesNo.NO,
                )
            ]
        )
        result = format_persons(persons, pl.DataFrame(), standard_config)
        assert result["fp_choice"][0] == FreeParkingChoice.PAY_TO_PARK.value

    def test_missing_values_treated_as_no_subsidy(self, standard_config):
        """Test that missing (995) values are treated as no subsidy."""
        persons = pl.DataFrame(
            [
                create_person(
                    commute_subsidy_use_3=BooleanYesNo.MISSING,
                    commute_subsidy_use_4=BooleanYesNo.MISSING,
                )
            ]
        )
        result = format_persons(persons, pl.DataFrame(), standard_config)
        assert result["fp_choice"][0] == FreeParkingChoice.PAY_TO_PARK.value


class TestHouseholdFormatting:
    """Tests for household formatting."""

    def test_basic_household_formatting(self, standard_config):
        """Test basic household formatting with all required fields."""
        households = pl.DataFrame(
            [
                create_household(
                    hh_id=1,
                    home_taz=100,
                    num_people=2,
                    num_vehicles=1,
                    num_workers=1,
                    income_bin=IncomeBroad.INCOME_75TO100,
                )
            ]
        )

        persons = pl.DataFrame(
            [
                {
                    "hh_id": 1,
                    "person_id": 1,
                    "employment": Employment.EMPLOYED_FULLTIME.value,
                },
                {
                    "hh_id": 1,
                    "person_id": 2,
                    "employment": Employment.UNEMPLOYED_NOT_LOOKING.value,
                },
            ]
        )
        tours = pl.DataFrame([], schema=get_tour_schema())
        result = format_households(households, persons, tours, standard_config)

        assert len(result) == 1
        assert result["hh_id"][0] == 1
        assert result["taz"][0] == 100
        assert result["income"][0] == 87000  # Midpoint rounded to $1000
        assert result["autos"][0] == 1
        assert result["size"][0] == 2
        assert result["workers"][0] == 1
        assert result["jtf_choice"][0] == JTFChoice.NONE_NONE.value

    def test_income_bin_midpoint(self, standard_config):
        """Test income is derived from income_bin midpoint when income is not set."""
        households = pl.DataFrame(
            [
                create_household(
                    hh_id=1,
                    income_bin=IncomeBroad.INCOME_50TO75,
                )
            ]
        )

        persons = pl.DataFrame(
            {"hh_id": [], "employment": []},
            schema={"hh_id": pl.Int64, "employment": pl.Int64},
        )
        tours = pl.DataFrame([], schema=get_tour_schema())
        result = format_households(households, persons, tours, standard_config)

        assert result["income"][0] == 62000  # Midpoint of $50,000-$74,999 rounded to $1000


class TestPersonFormatting:
    """Tests for person formatting."""

    def test_basic_person_formatting(self, standard_config):
        """Test basic person formatting with all required fields."""
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    age=AgeCategory.AGE_35_TO_44,
                    gender=Gender.MALE,
                    employment=Employment.EMPLOYED_FULLTIME,
                    student=Student.NONSTUDENT,
                    commute_subsidy_use_3=BooleanYesNo.YES,
                )
            ]
        )

        result = format_persons(persons, pl.DataFrame(), standard_config)

        assert len(result) == 1
        assert result["hh_id"][0] == 1
        assert result["person_id"][0] == 101
        assert result["person_num"][0] == 1
        assert result["age"][0] == 39  # Midpoint of 35-44
        assert result["gender"][0] == "m"
        assert result["type"][0] == CTRAMPPersonType.FULL_TIME_WORKER.label
        assert result["fp_choice"][0] == FreeParkingChoice.PARK_FOR_FREE.value
        assert result["activity_pattern"][0] == "H"  # Placeholder
        assert result["imf_choice"][0] == 0  # Placeholder
        assert result["inmf_choice"][0] == 0  # Placeholder (default)
        assert result["wfh_choice"][0] == WFHChoice.NON_WORKER_OR_NO_WFH.value  # Placeholder

    def test_gender_mapping(self, standard_config):
        """Test gender mapping to m/f format."""
        persons = pl.DataFrame(
            [
                create_person(person_id=101, gender=Gender.FEMALE),
                create_person(person_id=102, gender=Gender.MALE),
                create_person(person_id=103, gender=Gender.OTHER),
            ]
        )

        result = format_persons(persons, pl.DataFrame(), standard_config)

        assert result["gender"][0] == "f"
        assert result["gender"][1] == "m"
        assert result["gender"][2] == "f"  # Defaults to f

    # value_of_time is optional - no need to test default


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
            income_low_threshold=standard_config.income_low_threshold,
            income_med_threshold=standard_config.income_med_threshold,
            income_high_threshold=standard_config.income_high_threshold,
            income_base_year_dollars=standard_config.income_base_year_dollars,
        )

        households_ctramp = result["households_ctramp"]
        persons_ctramp = result["persons_ctramp"]

        assert len(households_ctramp) == 1
        assert len(persons_ctramp) == 1
        assert households_ctramp["hh_id"][0] == 1
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
            income_low_threshold=standard_config.income_low_threshold,
            income_med_threshold=standard_config.income_med_threshold,
            income_high_threshold=standard_config.income_high_threshold,
            income_base_year_dollars=standard_config.income_base_year_dollars,
        )

        households_ctramp = result["households_ctramp"]
        persons_ctramp = result["persons_ctramp"]

        assert len(households_ctramp) == 1
        assert len(persons_ctramp) == 4

        # Check person types
        person_types = persons_ctramp["type"].to_list()
        assert CTRAMPPersonType.FULL_TIME_WORKER.label in person_types
        assert CTRAMPPersonType.PART_TIME_WORKER.label in person_types
        assert CTRAMPPersonType.CHILD_DRIVING_AGE.label in person_types
        assert CTRAMPPersonType.CHILD_NON_DRIVING_AGE.label in person_types

    def test_retired_household(self, standard_config):
        """Test formatting of retired household."""
        households, persons = create_retired_household()

        result = format_ctramp(
            persons,
            households,
            linked_trips=empty_linked_trips(),
            tours=empty_tours(),
            joint_trips=empty_joint_trips(),
            income_low_threshold=standard_config.income_low_threshold,
            income_med_threshold=standard_config.income_med_threshold,
            income_high_threshold=standard_config.income_high_threshold,
            income_base_year_dollars=standard_config.income_base_year_dollars,
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
            income_low_threshold=standard_config.income_low_threshold,
            income_med_threshold=standard_config.income_med_threshold,
            income_high_threshold=standard_config.income_high_threshold,
            income_base_year_dollars=standard_config.income_base_year_dollars,
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
            income_low_threshold=standard_config.income_low_threshold,
            income_med_threshold=standard_config.income_med_threshold,
            income_high_threshold=standard_config.income_high_threshold,
            income_base_year_dollars=standard_config.income_base_year_dollars,
            drop_missing_taz=True,
        )

        households_ctramp = result["households_ctramp"]
        persons_ctramp = result["persons_ctramp"]

        # Only household 1 should remain
        assert len(households_ctramp) == 1
        assert len(persons_ctramp) == 1
        assert households_ctramp["hh_id"][0] == 1
        assert persons_ctramp["hh_id"][0] == 1

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
            income_low_threshold=standard_config.income_low_threshold,
            income_med_threshold=standard_config.income_med_threshold,
            income_high_threshold=standard_config.income_high_threshold,
            income_base_year_dollars=standard_config.income_base_year_dollars,
            drop_missing_taz=False,
        )

        households_ctramp = result["households_ctramp"]
        persons_ctramp = result["persons_ctramp"]

        # Both households should remain
        assert len(households_ctramp) == 2
        assert len(persons_ctramp) == 2


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
            tours, trips, persons, households_formatted, standard_config
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
            tours, trips, persons, households_formatted, config=standard_config
        )
        result = format_individual_trip(
            trips, tours_formatted, persons, households_formatted, config=standard_config
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
                    tour_id=1001, person_id=101, hh_id=1, joint_tour_id=9001, num_travelers=2
                ),
                create_tour(
                    tour_id=1002, person_id=102, hh_id=1, joint_tour_id=9001, num_travelers=2
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
            tours, trips, persons_formatted, households_formatted, standard_config
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
                create_tour(tour_id=1001, person_id=101, hh_id=1, joint_tour_id=9001),
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
            joint_trips, trips, tours, households_formatted, config=standard_config
        )

        required_columns = get_required_non_null_fields(JointTripCTRAMPModel)
        for col in required_columns:
            assert col in result.columns, f"Missing required column: {col}"


class TestIndividualTourFormatting:
    """Tests for individual tour formatting."""

    def test_basic_work_tour(self, standard_config):
        """Test formatting of a basic work tour with outbound/inbound trips."""
        # Create canonical data
        households_canonical = pl.DataFrame(
            [create_household(hh_id=1, income_bin=IncomeBroad.INCOME_75TO100)]
        )
        persons_canonical = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    employment=Employment.EMPLOYED_FULLTIME,
                    person_type=CTRAMPPersonType.FULL_TIME_WORKER.value,
                )
            ]
        )
        tours_canonical = pl.DataFrame(
            [
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    tour_purpose=PurposeCategory.WORK,
                    o_taz=100,
                    d_taz=200,
                    origin_depart_time=datetime.combine(datetime(2024, 1, 1), time(8, 0)),
                    origin_arrive_time=datetime.combine(datetime(2024, 1, 1), time(17, 0)),
                    student_category="Not student",
                )
            ],
            schema=get_tour_schema(),
        )

        # Format to CTRAMP (tours formatter needs formatted households/persons)
        households = format_households(
            households_canonical, persons_canonical, tours_canonical, standard_config
        )
        # Pass canonical persons for person_type and school_type
        tours = tours_canonical
        trips_canonical = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10001,
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10002,
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    tour_direction=TourDirection.INBOUND,
                ),
            ]
        )
        trips = trips_canonical

        result = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            persons_canonical=persons_canonical,
            households_ctramp=households,
            config=standard_config,
        )

        assert len(result) == 1
        assert result["tour_id"][0] == 1  # CTRAMP tour_id is tour_num (1 for first tour)
        assert result["hh_id"][0] == 1
        assert result["person_id"][0] == 101
        assert result["orig_taz"][0] == 100
        assert result["dest_taz"][0] == 200
        assert result["start_hour"][0] == 8
        assert result["end_hour"][0] == 17
        assert result["num_ob_stops"][0] == 0  # 1 OB trip = 0 stops
        assert result["num_ib_stops"][0] == 0  # 1 IB trip = 0 stops
        assert result["atWork_freq"][0] == 0  # No subtours
        # Purpose should be work_med (income 100-150k is in med bracket)
        assert result["tour_purpose"][0] == "work_med"

    def test_stop_counting_multiple_stops(self, standard_config):
        """Test stop counting with multiple outbound and inbound stops."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [create_tour(tour_id=1001, person_id=101, hh_id=1)],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                # 3 outbound trips
                create_linked_trip(
                    trip_id=10001,
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10002,
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10003,
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                # 2 inbound trips
                create_linked_trip(
                    trip_id=10004,
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    tour_direction=TourDirection.INBOUND,
                ),
                create_linked_trip(
                    trip_id=10005,
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    tour_direction=TourDirection.INBOUND,
                ),
            ]
        )

        # Format to CTRAMP first
        households_formatted = format_households(households, persons, tours, standard_config)

        result = format_individual_tour(
            tours,
            trips,
            persons,
            households_formatted,
            standard_config,
        )

        assert result["num_ob_stops"][0] == 2  # 3 trips = 2 stops
        assert result["num_ib_stops"][0] == 1  # 2 trips = 1 stop

    def test_subtour_counting(self, standard_config):
        """Test at-work tour frequency counting."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [
                # Primary work tour (tour_num=1)
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    tour_num=1,
                    tour_purpose=PurposeCategory.WORK,
                ),
                # At-work subtour 1 (tour_num=2)
                create_tour(
                    tour_id=1002,
                    person_id=101,
                    hh_id=1,
                    tour_num=2,
                    parent_tour_id=1001,
                    tour_purpose=PurposeCategory.WORK_RELATED,
                ),
                # At-work subtour 2 (tour_num=3)
                create_tour(
                    tour_id=1003,
                    person_id=101,
                    hh_id=1,
                    tour_num=3,
                    parent_tour_id=1001,
                    tour_purpose=PurposeCategory.MEAL,
                ),
            ],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                # Primary tour trips
                create_linked_trip(
                    trip_id=10001,
                    tour_id=1001,
                    person_id=101,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10002,
                    tour_id=1001,
                    person_id=101,
                    tour_direction=TourDirection.INBOUND,
                ),
                # Subtour 1 trips
                create_linked_trip(
                    trip_id=10003,
                    tour_id=1002,
                    person_id=101,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10004,
                    tour_id=1002,
                    person_id=101,
                    tour_direction=TourDirection.INBOUND,
                ),
                # Subtour 2 trips
                create_linked_trip(
                    trip_id=10005,
                    tour_id=1003,
                    person_id=101,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10006,
                    tour_id=1003,
                    person_id=101,
                    tour_direction=TourDirection.INBOUND,
                ),
            ]
        )

        # Format to CTRAMP
        households_formatted = format_households(households, persons, tours, standard_config)

        result = format_individual_tour(
            tours,
            trips,
            persons,
            households_formatted,
            standard_config,
        )

        # Primary tour should have 2 subtours (CTRAMP tour_id is tour_num: 1, 2, 3)
        primary_tour = result.filter(pl.col("tour_id") == 1)
        assert primary_tour["atWork_freq"][0] == 2

        # Subtours should have 0 subtours
        subtour1 = result.filter(pl.col("tour_id") == 2)
        subtour2 = result.filter(pl.col("tour_id") == 3)
        assert subtour1["atWork_freq"][0] == 0
        assert subtour2["atWork_freq"][0] == 0

    def test_zero_trip_tour_validation(self, standard_config):
        """Test that tours with zero trips raise validation error."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [create_tour(tour_id=1001, person_id=101, hh_id=1)],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame([])  # No trips!

        # Format to CTRAMP
        households_formatted = format_households(households, persons, tours, standard_config)

        with pytest.raises(ValueError, match="Found 1 tours with zero trips"):
            format_individual_tour(
                tours,
                trips,
                persons,
                households_formatted,
                standard_config,
            )

    def test_joint_tour_exclusion(self, standard_config):
        """Test that joint tours are excluded from individual tours."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [
                # Individual tour
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    joint_tour_id=None,
                ),
                # Joint tour (should be excluded)
                create_tour(
                    tour_id=1002,
                    person_id=101,
                    hh_id=1,
                    joint_tour_id=9001,
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
                ),
                create_linked_trip(
                    trip_id=10002,
                    tour_id=1001,
                    person_id=101,
                    tour_direction=TourDirection.INBOUND,
                ),
                create_linked_trip(
                    trip_id=10003,
                    tour_id=1002,
                    person_id=101,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10004,
                    tour_id=1002,
                    person_id=101,
                    tour_direction=TourDirection.INBOUND,
                ),
            ]
        )

        # Format to CTRAMP
        households_formatted = format_households(households, persons, tours, standard_config)
        format_persons(persons, pl.DataFrame(), standard_config)

        result = format_individual_tour(
            tours,
            trips,
            persons,
            households_formatted,
            standard_config,
        )

        # Only individual tour should be included
        assert len(result) == 1
        assert result["tour_id"][0] == 1  # CTRAMP tour_id is tour_num (1 for first tour)


class TestJointTourFormatting:
    """Tests for joint tour formatting."""

    def test_basic_joint_tour(self, standard_config):
        """Test formatting of a basic joint tour."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    age=AgeCategory.AGE_35_TO_44,
                ),
                create_person(
                    person_id=102,
                    hh_id=1,
                    person_num=2,
                    age=AgeCategory.AGE_5_TO_15,
                ),
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
                ),
                create_tour(
                    tour_id=1002,
                    person_id=102,
                    hh_id=1,
                    joint_tour_id=9001,
                    num_travelers=2,
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

        # Format to CTRAMP
        households_formatted = format_households(households, persons, tours, standard_config)

        result = format_joint_tour(
            tours,
            trips,
            persons,
            households_formatted,
            standard_config,
        )

        assert len(result) == 1
        assert result["tour_id"][0] == 9001
        assert result["hh_id"][0] == 1
        assert result["num_ob_stops"][0] == 0  # 1 trip = 0 stops
        assert result["num_ib_stops"][0] == 0  # 1 trip = 0 stops
        # Composition: 1 adult + 1 child
        assert result["tour_composition"][0] == TourComposition.ADULTS_AND_CHILDREN.value

    def test_individual_tour_exclusion_joint_formatter(self, standard_config):
        """Test that individual tours are excluded from joint tours."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [
                # Individual tour (should be excluded)
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    joint_tour_id=None,
                ),
                # Joint tour
                create_tour(
                    tour_id=1002,
                    person_id=101,
                    hh_id=1,
                    joint_tour_id=9001,
                ),
            ],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10001,
                    tour_id=1001,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10002,
                    tour_id=1001,
                    tour_direction=TourDirection.INBOUND,
                ),
                create_linked_trip(
                    trip_id=10003,
                    tour_id=1002,
                    tour_direction=TourDirection.OUTBOUND,
                    joint_tour_id=9001,
                ),
                create_linked_trip(
                    trip_id=10004,
                    tour_id=1002,
                    tour_direction=TourDirection.INBOUND,
                    joint_tour_id=9001,
                ),
            ]
        )

        # Format to CTRAMP
        households_formatted = format_households(households, persons, tours, standard_config)

        result = format_joint_tour(
            tours,
            trips,
            persons,
            households_formatted,
            standard_config,
        )

        # Only joint tour should be included
        assert len(result) == 1
        assert result["tour_id"][0] == 9001

    def test_empty_joint_tours(self, standard_config):
        """Test that formatter handles no joint tours gracefully."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [
                # Only individual tours
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    joint_tour_id=None,
                )
            ],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10001,
                    tour_id=1001,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10002,
                    tour_id=1001,
                    tour_direction=TourDirection.INBOUND,
                ),
            ]
        )

        # Format to CTRAMP
        households_formatted = format_households(households, persons, tours, standard_config)
        persons_formatted = format_persons(persons, pl.DataFrame(), standard_config)

        result = format_joint_tour(
            tours,
            trips,
            persons_formatted,
            households_formatted,
            standard_config,
        )

        # Should return empty DataFrame
        assert len(result) == 0
