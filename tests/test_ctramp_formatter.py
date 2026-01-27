"""Unit tests for CT-RAMP formatter.

Tests person type classification, household formatting, tour formatting,
and end-to-end transformation from canonical survey data to CT-RAMP model
format.
"""

from datetime import datetime, time
from pathlib import Path
from typing import get_args

import polars as pl
import pytest

from data_canon.codebook.ctramp import (
    CTRAMPPersonType,
    CTRAMPTourCategory,
    FreeParkingChoice,
    JTFChoice,
    TourComposition,
    WFHChoice,
    build_alternatives,
    load_alternatives_from_csv,
)
from data_canon.codebook.generic import BooleanYesNo
from data_canon.codebook.households import IncomeDetailed, IncomeFollowup
from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    Gender,
    JobType,
    SchoolType,
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
from processing.formatting.ctramp.format_persons import (
    classify_person_type,
    format_persons,
)
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


class TestPersonTypeClassification:
    """Tests for person type classification logic."""

    def test_full_time_worker(self):
        """Test classification of full-time worker."""
        person_type = classify_person_type(
            age=AgeCategory.AGE_35_TO_44.value,
            employment=Employment.EMPLOYED_FULLTIME.value,
            student=Student.NONSTUDENT.value,
            school_type=SchoolType.MISSING.value,
        )
        assert person_type == CTRAMPPersonType.FULL_TIME_WORKER.value

    def test_part_time_worker(self):
        """Test classification of part-time worker."""
        person_type = classify_person_type(
            age=AgeCategory.AGE_25_TO_34.value,
            employment=Employment.EMPLOYED_PARTTIME.value,
            student=Student.NONSTUDENT.value,
            school_type=SchoolType.MISSING.value,
        )
        assert person_type == CTRAMPPersonType.PART_TIME_WORKER.value

    def test_university_student(self):
        """Test classification of university student."""
        person_type = classify_person_type(
            age=AgeCategory.AGE_18_TO_24.value,
            employment=Employment.UNEMPLOYED_NOT_LOOKING.value,
            student=Student.FULLTIME_INPERSON.value,
            school_type=SchoolType.COLLEGE_4YEAR.value,
        )
        assert person_type == CTRAMPPersonType.UNIVERSITY_STUDENT.value

    def test_retired_person(self):
        """Test classification of retired person."""
        person_type = classify_person_type(
            age=AgeCategory.AGE_65_TO_74.value,
            employment=Employment.UNEMPLOYED_NOT_LOOKING.value,
            student=Student.NONSTUDENT.value,
            school_type=SchoolType.MISSING.value,
        )
        assert person_type == CTRAMPPersonType.RETIRED.value

    def test_nonworker(self):
        """Test classification of non-worker (under 65)."""
        person_type = classify_person_type(
            age=AgeCategory.AGE_45_TO_54.value,
            employment=Employment.UNEMPLOYED_NOT_LOOKING.value,
            student=Student.NONSTUDENT.value,
            school_type=SchoolType.MISSING.value,
        )
        assert person_type == CTRAMPPersonType.NON_WORKER.value

    def test_driving_age_student(self):
        """Test classification of driving age student."""
        person_type = classify_person_type(
            age=AgeCategory.AGE_16_TO_17.value,
            employment=Employment.UNEMPLOYED_NOT_LOOKING.value,
            student=Student.FULLTIME_INPERSON.value,
            school_type=SchoolType.HIGH_SCHOOL.value,
        )
        assert person_type == CTRAMPPersonType.CHILD_DRIVING_AGE.value

    def test_non_driving_age_student(self):
        """Test classification of non-driving age student."""
        person_type = classify_person_type(
            age=AgeCategory.AGE_5_TO_15.value,
            employment=Employment.UNEMPLOYED_NOT_LOOKING.value,
            student=Student.FULLTIME_INPERSON.value,
            school_type=SchoolType.ELEMENTARY.value,
        )
        assert person_type == CTRAMPPersonType.CHILD_NON_DRIVING_AGE.value

    def test_child_too_young(self):
        """Test classification of child too young for school."""
        person_type = classify_person_type(
            age=AgeCategory.AGE_UNDER_5.value,
            employment=Employment.UNEMPLOYED_NOT_LOOKING.value,
            student=Student.NONSTUDENT.value,
            school_type=SchoolType.PRESCHOOL.value,
        )
        assert person_type == CTRAMPPersonType.CHILD_UNDER_5.value


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
                    income_detailed=IncomeDetailed.INCOME_75TO100,
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

    def test_income_fallback_logic(self, standard_config):
        """Test income fallback from detailed to followup."""
        households = pl.DataFrame(
            [
                create_household(
                    hh_id=1,
                    income_detailed=None,
                    income_followup=IncomeFollowup.INCOME_50TO75,
                )
            ]
        )

        persons = pl.DataFrame(
            {"hh_id": [], "employment": []},
            schema={"hh_id": pl.Int64, "employment": pl.Int64},
        )
        tours = pl.DataFrame([], schema=get_tour_schema())
        result = format_households(households, persons, tours, standard_config)

        assert result["income"][0] == 62000  # Midpoint of 50-75k from followup rounded to $1000


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
            [create_household(hh_id=1, income_detailed=IncomeDetailed.INCOME_100TO150)]
        )
        persons_canonical = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    employment=Employment.EMPLOYED_FULLTIME,
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


class TestHouseholdFieldCorrections:
    """Tests for household field corrections."""

    def test_autos_computed_from_vehicles(self, standard_config):
        """Test that autos field is computed from vehicle count, not hardcoded to 0."""
        households = pl.DataFrame(
            [
                create_household(hh_id=1, num_vehicles=2),
                create_household(hh_id=2, num_vehicles=0),
                create_household(hh_id=3, num_vehicles=3),
            ]
        )
        persons = pl.DataFrame(
            [
                create_person(person_id=101, hh_id=1),
                create_person(person_id=201, hh_id=2),
                create_person(person_id=301, hh_id=3),
            ]
        )
        tours = pl.DataFrame([], schema=get_tour_schema())

        result = format_households(households, persons, tours, standard_config)

        assert result["autos"][0] == 2, "Should match num_vehicles"
        assert result["autos"][1] == 0, "Should be 0 when no vehicles"
        assert result["autos"][2] == 3, "Should match num_vehicles"

    def test_jtf_choice_computed_from_joint_tours(self, standard_config):
        """Test that jtf_choice is computed from joint tours, not hardcoded to -4.

        Note: Implementation now uses JTFChoice enum values based on joint tour purposes.
        This test needs updating to provide proper tour purposes.
        """
        households = pl.DataFrame([create_household(hh_id=1, home_taz=100)])
        persons = pl.DataFrame(
            [
                create_person(person_id=101, hh_id=1),
                create_person(person_id=102, hh_id=1),
            ]
        )

        # Create 2 joint tours for household (both shopping)
        tours = pl.DataFrame(
            [
                create_tour(
                    tour_id=1001,
                    hh_id=1,
                    person_id=101,
                    joint_tour_id=9001,
                    tour_purpose=PurposeCategory.SHOP,
                ),
                create_tour(
                    tour_id=1002,
                    hh_id=1,
                    person_id=102,
                    joint_tour_id=9001,
                    tour_purpose=PurposeCategory.SHOP,
                ),
                create_tour(
                    tour_id=1003,
                    hh_id=1,
                    person_id=101,
                    joint_tour_id=9002,
                    tour_purpose=PurposeCategory.SHOP,
                ),
                create_tour(
                    tour_id=1004,
                    hh_id=1,
                    person_id=102,
                    joint_tour_id=9002,
                    tour_purpose=PurposeCategory.SHOP,
                ),
            ],
            schema=get_tour_schema(),
        ).with_columns(
            pl.lit(value=True).alias("single_trip_tour")
            # Each tour has 1 trip, so flag should be True
        )

        # Add trips for each tour to avoid validation error
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    linked_trip_id=10001,
                    tour_id=1001,
                    person_id=101,
                    tour_direction=TourDirection.OUTBOUND,
                    joint_tour_id=9001,
                ),
                create_linked_trip(
                    linked_trip_id=10002,
                    tour_id=1002,
                    person_id=102,
                    tour_direction=TourDirection.OUTBOUND,
                    joint_tour_id=9001,
                ),
                create_linked_trip(
                    linked_trip_id=10003,
                    tour_id=1003,
                    person_id=101,
                    tour_direction=TourDirection.OUTBOUND,
                    joint_tour_id=9002,
                ),
                create_linked_trip(
                    linked_trip_id=10004,
                    tour_id=1004,
                    person_id=102,
                    tour_direction=TourDirection.OUTBOUND,
                    joint_tour_id=9002,
                ),
            ]
        )

        result = format_ctramp(
            persons,
            households,
            linked_trips=trips,
            tours=tours,
            joint_trips=empty_joint_trips(),
            income_low_threshold=standard_config.income_low_threshold,
            income_med_threshold=standard_config.income_med_threshold,
            income_high_threshold=standard_config.income_high_threshold,
            income_base_year_dollars=standard_config.income_base_year_dollars,
        )

        households_ctramp = result["households_ctramp"]
        # With 2 joint shopping tours, should get TWO_SHOP (JTFChoice value 7)
        assert households_ctramp["jtf_choice"][0] == JTFChoice.TWO_SHOP.value, (
            "Should have TWO_SHOP jtf_choice"
        )

    def test_jtf_choice_zero_when_no_joint_tours(self, standard_config):
        """Test that jtf_choice is 0 when there are no joint tours."""
        households = pl.DataFrame([create_household(hh_id=1, home_taz=100)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])

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
        assert households_ctramp["jtf_choice"][0] == JTFChoice.NONE_NONE.value, (
            "Should be NONE_NONE with no joint tours"
        )


class TestPersonFieldCorrections:
    """Tests for person field corrections."""

    def test_inmf_matches_csv_fixture(self):
        """Validate that get_inmf_code_from_counts matches the CSV fixture row by row."""
        csv_path = (
            Path(__file__).parent
            / "fixtures"
            / "CTRAMP_IndividualNonMandatoryTourFrequencyAlternatives.csv"
        )

        # Example usage: print all alternatives
        csv_alternatives = load_alternatives_from_csv(csv_path)
        # Use `maxes` (inclusive max frequencies) with the new API
        py_alternatives = build_alternatives(
            maxes={
                "escort": 2,
                "shopping": 1,
                "othmaint": 1,
                "othdiscr": 1,
                "eatout": 1,
                "social": 1,
            }
        )

        # Compare
        for code in sorted(set(csv_alternatives.keys()).union(py_alternatives.keys())):
            alt_csv = csv_alternatives.get(code)
            alt_py = py_alternatives.get(code)
            assert alt_csv == alt_py, f"Mismatch for code {code}: CSV={alt_csv}, PY={alt_py}"

    def test_type_outputs_string_labels(self, standard_config):
        """Test that person type outputs string labels, not integers."""
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    age=AgeCategory.AGE_35_TO_44,
                    employment=Employment.EMPLOYED_FULLTIME,
                    student=Student.NONSTUDENT,
                )
            ]
        )

        result = format_persons(persons, pl.DataFrame(), standard_config)

        assert result["type"][0] == "Full-time worker", "Should be string label, not integer"
        assert isinstance(result["type"][0], str), "Type should be string"

    def test_age_continuous_from_category_midpoint(self, standard_config):
        """Test that age is continuous value (midpoint), not category code."""
        persons = pl.DataFrame(
            [
                create_person(person_id=101, age=AgeCategory.AGE_UNDER_5),  # 2.5
                create_person(person_id=102, age=AgeCategory.AGE_5_TO_15),  # 10
                create_person(person_id=103, age=AgeCategory.AGE_35_TO_44),  # 39.5
                create_person(person_id=104, age=AgeCategory.AGE_85_AND_UP),  # 87.5
            ]
        )

        result = format_persons(persons, pl.DataFrame(), standard_config)

        # Age category midpoints
        assert result["age"][0] == 2, "Under 5 should be ~2"
        assert result["age"][1] == 10, "5-15 should be ~10"
        assert result["age"][2] == 39, "35-44 should be ~39"
        assert result["age"][3] == 87, "85+ should be ~87"

        # All should be continuous values, not category codes (1-11)
        # Note: Some midpoints fall in the excluded range (e.g., 10), so check for reasonable values
        for age in result["age"]:
            assert age >= 2, "Age should be at least 2"
            assert age <= 90, "Age should be at most 90"

    def test_inmf_choice_binned_to_codebook(self, standard_config):
        """Test inmf_choice binning per IndividualNonMandatoryTourFrequencyAlternatives."""
        persons = pl.DataFrame(
            [
                create_person(person_id=101, hh_id=1),
                create_person(person_id=102, hh_id=1),
                create_person(person_id=103, hh_id=1),
            ]
        )

        # Create individual non-mandatory tours
        tours = pl.DataFrame(
            [
                # Person 101: 0 non-mandatory tours (1 work tour doesn't count)
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    tour_purpose=PurposeCategory.WORK,
                ),
                # Person 102: 1 shopping tour -> code 17
                create_tour(tour_id=1002, person_id=102, tour_purpose=PurposeCategory.SHOP),
                # Person 103: shop + eatout + social -> code 23
                create_tour(tour_id=1003, person_id=103, tour_purpose=PurposeCategory.SHOP),
                create_tour(tour_id=1004, person_id=103, tour_purpose=PurposeCategory.MEAL),
                create_tour(tour_id=1005, person_id=103, tour_purpose=PurposeCategory.SOCIALREC),
            ],
            schema=get_tour_schema(),
        )

        # Format to get tour-based statistics
        households = pl.DataFrame([create_household(hh_id=1)])
        households_formatted = format_households(households, persons, tours, standard_config)
        # Create minimal trips for each tour to avoid validation error
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
                    tour_id=1002,
                    person_id=102,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10003,
                    tour_id=1003,
                    person_id=103,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10004,
                    tour_id=1004,
                    person_id=103,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10005,
                    tour_id=1005,
                    person_id=103,
                    tour_direction=TourDirection.OUTBOUND,
                ),
            ]
        )
        tours_formatted = format_individual_tour(
            tours, trips, persons, households_formatted, standard_config
        )

        result = format_persons(persons, tours_formatted, standard_config)

        # Assert CTRAMP alternative codes from codebook CSV
        # Code 0 = no non-mandatory tours (special case)
        # Code 17 = (escort=0, shopping=1, othmaint=0, othdiscr=0, eatout=0, social=0)
        # Code 23 = (escort=0, shopping=1, othmaint=0, othdiscr=0, eatout=1, social=1)
        assert result["inmf_choice"][0] == 0, "No non-mandatory tours -> code 0"
        assert result["inmf_choice"][1] == 17, "1 shopping tour -> code 17"
        assert result["inmf_choice"][2] == 23, "shop+eatout+social -> code 23"

    def test_inmf_choice_escort_tours(self, standard_config):
        """Test inmf_choice with escort tours."""
        persons = pl.DataFrame(
            [
                create_person(person_id=101, hh_id=1),
                create_person(person_id=102, hh_id=1),
            ]
        )

        tours = pl.DataFrame(
            [
                # Person 101: 1 escort tour -> code 33
                create_tour(tour_id=1001, person_id=101, tour_purpose=PurposeCategory.ESCORT),
                # Person 102: 2 escort tours -> code 65
                create_tour(tour_id=1002, person_id=102, tour_purpose=PurposeCategory.ESCORT),
                create_tour(tour_id=1003, person_id=102, tour_purpose=PurposeCategory.ESCORT),
            ],
            schema=get_tour_schema(),
        )

        households = pl.DataFrame([create_household(hh_id=1)])
        households_formatted = format_households(households, persons, tours, standard_config)
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
                    tour_id=1002,
                    person_id=102,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10003,
                    tour_id=1003,
                    person_id=102,
                    tour_direction=TourDirection.OUTBOUND,
                ),
            ]
        )
        tours_formatted = format_individual_tour(
            tours, trips, persons, households_formatted, standard_config
        )
        result = format_persons(persons, tours_formatted, standard_config)

        # Code 33 = (escort=1, shopping=0, othmaint=0, othdiscr=0, eatout=0, social=0)
        # Code 65 = (escort=2, shopping=0, othmaint=0, othdiscr=0, eatout=0, social=0)
        assert result["inmf_choice"][0] == 33, "1 escort tour -> code 33"
        assert result["inmf_choice"][1] == 65, "2 escort tours -> code 65"

    def test_inmf_choice_capping_behavior(self, standard_config):
        """Test that tour counts exceeding codebook maximums are capped properly."""
        persons = pl.DataFrame(
            [
                create_person(person_id=101, hh_id=1),
                create_person(person_id=102, hh_id=1),
            ]
        )

        tours = pl.DataFrame(
            [
                # Person 101: 3 escort tours (should cap to 2) -> code 65
                create_tour(tour_id=1001, person_id=101, tour_purpose=PurposeCategory.ESCORT),
                create_tour(tour_id=1002, person_id=101, tour_purpose=PurposeCategory.ESCORT),
                create_tour(tour_id=1003, person_id=101, tour_purpose=PurposeCategory.ESCORT),
                # Person 102: 2 shopping tours (should cap to 1) -> code 17
                create_tour(tour_id=1004, person_id=102, tour_purpose=PurposeCategory.SHOP),
                create_tour(tour_id=1005, person_id=102, tour_purpose=PurposeCategory.SHOP),
            ],
            schema=get_tour_schema(),
        )

        households = pl.DataFrame([create_household(hh_id=1)])
        households_formatted = format_households(households, persons, tours, standard_config)
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10001 + i,
                    tour_id=1001 + i,
                    person_id=101 if i < 3 else 102,
                    tour_direction=TourDirection.OUTBOUND,
                )
                for i in range(5)
            ]
        )
        tours_formatted = format_individual_tour(
            tours, trips, persons, households_formatted, standard_config
        )
        result = format_persons(persons, tours_formatted, standard_config)

        # 3 escort tours capped to 2 -> code 65
        # 2 shopping tours capped to 1 -> code 17
        assert result["inmf_choice"][0] == 65, "3 escort tours capped to 2 -> code 65"
        assert result["inmf_choice"][1] == 17, "2 shopping tours capped to 1 -> code 17"

    def test_inmf_choice_complex_combinations(self, standard_config):
        """Test various complex tour combinations."""
        persons = pl.DataFrame(
            [
                create_person(person_id=101, hh_id=1),
                create_person(person_id=102, hh_id=1),
                create_person(person_id=103, hh_id=1),
            ]
        )

        tours = pl.DataFrame(
            [
                # Person 101: 1 othdiscr -> code 2
                create_tour(tour_id=1001, person_id=101, tour_purpose=PurposeCategory.OTHER),
                # Person 102: 1 othmaint -> code 9
                create_tour(tour_id=1002, person_id=102, tour_purpose=PurposeCategory.ERRAND),
                # Person 103: 1 eatout -> code 5
                create_tour(tour_id=1003, person_id=103, tour_purpose=PurposeCategory.MEAL),
            ],
            schema=get_tour_schema(),
        )

        households = pl.DataFrame([create_household(hh_id=1)])
        households_formatted = format_households(households, persons, tours, standard_config)
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
                    tour_id=1002,
                    person_id=102,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10003,
                    tour_id=1003,
                    person_id=103,
                    tour_direction=TourDirection.OUTBOUND,
                ),
            ]
        )
        tours_formatted = format_individual_tour(
            tours, trips, persons, households_formatted, standard_config
        )
        result = format_persons(persons, tours_formatted, standard_config)

        # Code 2 = (escort=0, shopping=0, othmaint=0, othdiscr=1, eatout=0, social=0)
        # Code 9 = (escort=0, shopping=0, othmaint=1, othdiscr=0, eatout=0, social=0)
        # Code 5 = (escort=0, shopping=0, othmaint=0, othdiscr=0, eatout=1, social=0)
        assert result["inmf_choice"][0] == 2, "1 othdiscr tour -> code 2"
        assert result["inmf_choice"][1] == 9, "1 othmaint tour -> code 9"
        assert result["inmf_choice"][2] == 5, "1 eatout tour -> code 5"

    def test_inmf_choice_maximum_combination(self, standard_config):
        """Test maximum tour combination (all categories at max)."""
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])

        # Maximum: 2 escort, 1 shopping, 1 othmaint, 1 othdiscr, 1 eatout, 1 social -> code 96
        tours = pl.DataFrame(
            [
                create_tour(tour_id=1001, person_id=101, tour_purpose=PurposeCategory.ESCORT),
                create_tour(tour_id=1002, person_id=101, tour_purpose=PurposeCategory.ESCORT),
                create_tour(tour_id=1003, person_id=101, tour_purpose=PurposeCategory.SHOP),
                create_tour(tour_id=1004, person_id=101, tour_purpose=PurposeCategory.ERRAND),
                create_tour(tour_id=1005, person_id=101, tour_purpose=PurposeCategory.OTHER),
                create_tour(tour_id=1006, person_id=101, tour_purpose=PurposeCategory.MEAL),
                create_tour(tour_id=1007, person_id=101, tour_purpose=PurposeCategory.SOCIALREC),
            ],
            schema=get_tour_schema(),
        )

        households = pl.DataFrame([create_household(hh_id=1)])
        households_formatted = format_households(households, persons, tours, standard_config)
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10000 + i,
                    tour_id=1001 + i,
                    person_id=101,
                    tour_direction=TourDirection.OUTBOUND,
                )
                for i in range(7)
            ]
        )
        tours_formatted = format_individual_tour(
            tours, trips, persons, households_formatted, standard_config
        )
        result = format_persons(persons, tours_formatted, standard_config)

        # Code 96 = (escort=2, shopping=1, othmaint=1, othdiscr=1, eatout=1, social=1)
        assert result["inmf_choice"][0] == 96, "All categories at maximum -> code 96"

    def test_wfh_choice_detects_work_from_home(self, standard_config):
        """Test that wfh_choice is derived from job_type and employment status."""
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    employment=Employment.EMPLOYED_FULLTIME,
                    job_type=JobType.FIXED.value,  # Not WFH
                ),
                create_person(
                    person_id=102,
                    hh_id=1,
                    employment=Employment.EMPLOYED_FULLTIME,
                    job_type=JobType.WFH.value,  # WFH
                ),
                create_person(
                    person_id=103,
                    hh_id=1,
                    employment=Employment.UNEMPLOYED_NOT_LOOKING,
                    job_type=JobType.WFH.value,  # Non-worker, so not WFH
                ),
            ]
        )

        tours = pl.DataFrame(
            [
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    tour_purpose=PurposeCategory.WORK,
                )
            ],
            schema=get_tour_schema(),
        )

        households = pl.DataFrame([create_household(hh_id=1)])
        households_formatted = format_households(households, persons, tours, standard_config)
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10001,
                    tour_id=1001,
                    person_id=101,
                    tour_direction=TourDirection.OUTBOUND,
                ),
            ]
        )
        tours_formatted = format_individual_tour(
            tours, trips, persons, households_formatted, standard_config
        )

        result = format_persons(persons, tours_formatted, standard_config)

        assert result["wfh_choice"][0] == WFHChoice.NON_WORKER_OR_NO_WFH.value, (
            "Employed person with FIXED job_type should not be WFH"
        )
        assert result["wfh_choice"][1] == WFHChoice.WORKS_FROM_HOME.value, (
            "Employed person with WFH job_type should be WFH"
        )
        assert result["wfh_choice"][2] == WFHChoice.NON_WORKER_OR_NO_WFH.value, (
            "Non-worker should not be WFH even with WFH job_type"
        )


class TestIndividualTripFieldCorrections:
    """Tests for individual trip field corrections."""

    def test_depart_hour_field_present(self, standard_config):
        """Test that depart_hour field is present in trip output."""
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
                    hh_id=1,
                    tour_direction=TourDirection.OUTBOUND,
                    depart_time=datetime.combine(datetime(2024, 1, 1), time(8, 30)),
                )
            ]
        )

        households_formatted = format_households(households, persons, tours, standard_config)
        tours_formatted = format_individual_tour(
            tours, trips, persons, households_formatted, config=standard_config
        )
        result = format_individual_trip(
            trips, tours_formatted, persons, households_formatted, config=standard_config
        )

        assert "depart_hour" in result.columns, "depart_hour field should be present"
        assert result["depart_hour"][0] == 8, "depart_hour should be extracted from depart_time"

    def test_tour_purpose_string_not_int(self, standard_config):
        """Test that tour_purpose is string, not integer."""
        households = pl.DataFrame(
            [create_household(hh_id=1, income_detailed=IncomeDetailed.INCOME_50TO75)]
        )
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    tour_purpose=PurposeCategory.WORK,
                )
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
                    tour_direction=TourDirection.OUTBOUND,
                )
            ]
        )

        households_formatted = format_households(households, persons, tours, standard_config)
        # Format tours first to get CTRAMP-formatted tours
        tours_formatted = format_individual_tour(
            tours, trips, persons, households_formatted, standard_config
        )
        # Now pass formatted tours to format_individual_trip
        result = format_individual_trip(
            trips, tours_formatted, persons, households_formatted, config=standard_config
        )

        assert isinstance(result["tour_purpose"][0], str), "tour_purpose should be string"
        assert result["tour_purpose"][0] == "work_med", "Should be income-segmented work"


class TestIndividualTourFieldCorrections:
    """Tests for individual tour field corrections."""

    def test_type_outputs_string_not_int(self, standard_config):
        """Test that tour type field outputs string labels, not integers."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    age=AgeCategory.AGE_35_TO_44,
                    employment=Employment.EMPLOYED_FULLTIME,
                )
            ]
        )
        tours = pl.DataFrame(
            [create_tour(tour_id=1001, person_id=101, hh_id=1)],
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

        households_formatted = format_households(households, persons, tours, standard_config)
        result = format_individual_tour(
            tours, trips, persons, households_formatted, standard_config
        )

        assert isinstance(result["person_type"][0], int), "person_type should be integer enum"
        assert result["person_type"][0] == CTRAMPPersonType.FULL_TIME_WORKER.value, (
            "Should output person type code for Full-time worker"
        )

    def test_tour_category_string_not_int(self, standard_config):
        """Test that tour_category outputs string labels (MANDATORY, etc), not integers."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [
                # Mandatory tour
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    tour_purpose=PurposeCategory.WORK,
                ),
                # Non-mandatory tour
                create_tour(
                    tour_id=1002,
                    person_id=101,
                    tour_purpose=PurposeCategory.SHOP,
                ),
                # At-work subtour
                create_tour(
                    tour_id=1003,
                    person_id=101,
                    parent_tour_id=1001,
                    tour_purpose=PurposeCategory.MEAL,
                ),
            ],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                create_linked_trip(trip_id=i, tour_id=tid, tour_direction=TourDirection.OUTBOUND)
                for i, tid in [(10001, 1001), (10002, 1002), (10003, 1003)]
            ]
            + [
                create_linked_trip(trip_id=i, tour_id=tid, tour_direction=TourDirection.INBOUND)
                for i, tid in [(10004, 1001), (10005, 1002), (10006, 1003)]
            ]
        )

        households_formatted = format_households(households, persons, tours, standard_config)
        result = format_individual_tour(
            tours, trips, persons, households_formatted, standard_config
        )

        assert result["tour_category"][0] == CTRAMPTourCategory.MANDATORY.value, (
            "Work tour should be MANDATORY"
        )
        assert result["tour_category"][1] == CTRAMPTourCategory.INDIVIDUAL_NON_MANDATORY.value, (
            "Shopping should be INDIVIDUAL_NON_MANDATORY"
        )
        assert result["tour_category"][2] == CTRAMPTourCategory.AT_WORK.value, (
            "Subtour should be AT_WORK"
        )

    def test_tour_purpose_not_all_othdisc(self, standard_config):
        """Test that tour_purpose correctly maps various purposes, not all to 'othdisc'."""
        households = pl.DataFrame(
            [create_household(hh_id=1, income_detailed=IncomeDetailed.INCOME_100TO150)]
        )
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [
                create_tour(tour_id=1001, person_id=101, tour_purpose=PurposeCategory.WORK),
                create_tour(tour_id=1002, person_id=101, tour_purpose=PurposeCategory.SCHOOL),
                create_tour(tour_id=1003, person_id=101, tour_purpose=PurposeCategory.SHOP),
                create_tour(tour_id=1004, person_id=101, tour_purpose=PurposeCategory.MEAL),
            ],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                create_linked_trip(trip_id=i, tour_id=tid, tour_direction=TourDirection.OUTBOUND)
                for i, tid in [(10001, 1001), (10002, 1002), (10003, 1003), (10004, 1004)]
            ]
            + [
                create_linked_trip(trip_id=i, tour_id=tid, tour_direction=TourDirection.INBOUND)
                for i, tid in [(10005, 1001), (10006, 1002), (10007, 1003), (10008, 1004)]
            ]
        )

        households_formatted = format_households(households, persons, tours, standard_config)
        result = format_individual_tour(
            tours, trips, persons, households_formatted, standard_config
        )

        # Check various purposes are mapped correctly
        assert result["tour_purpose"][0] == "work_med", "Work should map to income-segmented work"
        assert "school" in result["tour_purpose"][1].lower(), "School should map to school purpose"
        assert result["tour_purpose"][2] == "shopping", "Shopping should map correctly"
        assert result["tour_purpose"][3] == "eatout", "Dining should map to eatout"
        # Should NOT all be 'othdisc'
        assert result["tour_purpose"].unique().to_list() != ["othdisc"], "Should have variety"

    def test_num_stops_correct_not_offset(self, standard_config):
        """Test that num_ob_stops/num_ib_stops are correct (stops = trips - 1), not offset."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [create_tour(tour_id=1001, person_id=101, hh_id=1)],
            schema=get_tour_schema(),
        )

        # Home -> Stop1 -> Stop2 -> Dest (3 outbound trips = 2 stops)
        # Dest -> Stop3 -> Home (2 inbound trips = 1 stop)
        trips = pl.DataFrame(
            [
                # Outbound: 3 trips
                create_linked_trip(
                    trip_id=10001,
                    tour_id=1001,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10002,
                    tour_id=1001,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10003,
                    tour_id=1001,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                # Inbound: 2 trips
                create_linked_trip(
                    trip_id=10004,
                    tour_id=1001,
                    tour_direction=TourDirection.INBOUND,
                ),
                create_linked_trip(
                    trip_id=10005,
                    tour_id=1001,
                    tour_direction=TourDirection.INBOUND,
                ),
            ]
        )

        households_formatted = format_households(households, persons, tours, standard_config)
        result = format_individual_tour(
            tours, trips, persons, households_formatted, standard_config
        )

        # Stops = trips - 1 for each direction
        assert result["num_ob_stops"][0] == 2, "3 outbound trips = 2 stops (not 3)"
        assert result["num_ib_stops"][0] == 1, "2 inbound trips = 1 stop (not 2)"


class TestWeightsAndSampleRate:
    """Tests for weight fields and sampleRate calculation in CTRAMP output."""

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
                "income_detailed": [IncomeDetailed.INCOME_75TO100.value] * 2,
                "income_followup": [None, None],
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
                "income_detailed": [IncomeDetailed.INCOME_75TO100.value],
                "income_followup": [None],
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
            tours, trips, persons, households_formatted, standard_config
        )

        # Verify weight column present
        assert "tour_weight" in result.columns
        assert "sampleRate" in result.columns

        # Verify sampleRate = 1/weight (CTRAMP tour_id is tour_num: 1, 2)
        assert result.filter(pl.col("tour_id") == 1)["sampleRate"][0] == pytest.approx(1 / 3.0)
        assert result.filter(pl.col("tour_id") == 2)["sampleRate"][0] == pytest.approx(1 / 5.0)

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
            tours, trips, persons, households_formatted, standard_config
        )

        # CTRAMP tour_id is tour_num: 1, 2
        assert result.filter(pl.col("tour_id") == 1)["sampleRate"][0] is None
        assert result.filter(pl.col("tour_id") == 2)["sampleRate"][0] == pytest.approx(1 / 2.5)

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
            tours, trips, persons, households_formatted, standard_config
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
            tours, trips, persons, households_formatted, standard_config
        )
        result = format_individual_trip(
            trips, tours_formatted, persons, households_formatted, standard_config
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
            tours, trips, persons, households_formatted, standard_config
        )
        result = format_individual_trip(
            trips, tours_formatted, persons, households_formatted, standard_config
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
            tours, trips, persons, households_formatted, standard_config
        )
        result = format_individual_trip(
            trips, tours_formatted, persons, households_formatted, standard_config
        )

        # Weight columns should not be present
        assert "trip_weight" not in result.columns
        assert "sampleRate" not in result.columns

    def test_joint_tours_no_weight_fields(self, standard_config):
        """Test joint tours do not include weight or sampleRate fields."""
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
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10001,
                    tour_id=1001,
                    joint_tour_id=5001,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10002,
                    tour_id=1001,
                    joint_tour_id=5001,
                    tour_direction=TourDirection.INBOUND,
                ),
            ]
        )

        households_formatted = format_households(households, persons, tours, standard_config)
        result = format_joint_tour(tours, trips, persons, households_formatted, standard_config)

        # Joint tours should NOT have weight or sampleRate fields
        assert "tour_weight" not in result.columns
        assert "sampleRate" not in result.columns

    def test_joint_trips_no_weight_fields(self, standard_config):
        """Test joint trips do not include weight or sampleRate fields."""
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
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10001,
                    tour_id=1001,
                    joint_tour_id=5001,
                    joint_trip_id=8001,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10002,
                    tour_id=1001,
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
        )

        households_formatted = format_households(households, persons, tours, standard_config)
        result = format_joint_trip(joint_trips, trips, tours, households_formatted, standard_config)

        # Joint trips should NOT have trip_weight or sampleRate fields
        assert "trip_weight" not in result.columns
        assert "sampleRate" not in result.columns
