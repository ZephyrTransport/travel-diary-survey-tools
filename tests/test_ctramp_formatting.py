"""Unit tests for CT-RAMP formatter.

Tests formatting, field corrections, and end-to-end transformation from
canonical survey data to CT-RAMP model format.
"""

from datetime import datetime, time
from typing import get_args

import polars as pl
import pytest

from data_canon.codebook.ctramp import (
    AtWorkFreq,
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


class TestFreeParkingChoice:
    """Tests for free parking choice in person formatting."""

    def test_free_parking_used(self, standard_config):
        """Test free parking choice when free parking is used."""
        persons = pl.DataFrame(
            [
                create_person(
                    commute_subsidy_use_free_parking=BooleanYesNo.YES,
                    commute_subsidy_use_discounted_parking=BooleanYesNo.NO,
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
                    commute_subsidy_use_free_parking=BooleanYesNo.NO,
                    commute_subsidy_use_discounted_parking=BooleanYesNo.YES,
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
                    commute_subsidy_use_free_parking=BooleanYesNo.YES,
                    commute_subsidy_use_discounted_parking=BooleanYesNo.YES,
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
                    commute_subsidy_use_free_parking=BooleanYesNo.NO,
                    commute_subsidy_use_discounted_parking=BooleanYesNo.NO,
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
                    commute_subsidy_use_free_parking=BooleanYesNo.MISSING,
                    commute_subsidy_use_discounted_parking=BooleanYesNo.MISSING,
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
        assert result["income"][0] == 46277  # $87k (2023) midpoint deflated to $2000 (/1.88)
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

        assert result["income"][0] == 32979  # $62k (2023) midpoint deflated to $2000 (/1.88)


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
                    commute_subsidy_use_free_parking=BooleanYesNo.YES,
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
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons_canonical,
            households_ctramp=households,
            config=standard_config,
        )

        assert len(result) == 1
        assert result["tour_id"][0] == 0  # CTRAMP tour_id is 0-based (0 for first tour)
        assert result["hh_id"][0] == 1
        assert result["person_id"][0] == 101
        assert result["orig_taz"][0] == 100
        assert result["dest_taz"][0] == 200
        assert result["start_hour"][0] == 8
        assert result["end_hour"][0] == 17
        assert result["num_ob_stops"][0] == 0  # 1 OB trip = 0 stops
        assert result["num_ib_stops"][0] == 0  # 1 IB trip = 0 stops
        # A work tour with no subtours is NO_SUBTOUR (1); 0 means "tour is not at work".
        assert result["atWork_freq"][0] == AtWorkFreq.NO_SUBTOUR.value
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
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
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
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        # Primary tour is 0-based (tour_id 0); its at-work subtours are encoded as
        # two-digit <1-based parent tour #><subtour #> -> 11 and 12.
        # atWork_freq is a CT-RAMP category, not a raw subtour count: this tour's
        # WORK_RELATED + MEAL subtours are one business and one eating out.
        primary_tour = result.filter(pl.col("tour_id") == 0)
        assert primary_tour["atWork_freq"][0] == AtWorkFreq.ONE_EAT_ONE_BUSINESS.value

        # Subtours are not themselves at work, so they take the not-at-work category.
        subtour1 = result.filter(pl.col("tour_id") == 11)
        subtour2 = result.filter(pl.col("tour_id") == 12)
        assert subtour1["atWork_freq"][0] == AtWorkFreq.NONE_NOT_WORK.value
        assert subtour2["atWork_freq"][0] == AtWorkFreq.NONE_NOT_WORK.value

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
                tours_canonical=tours,
                linked_trips_canonical=trips,
                unlinked_trips_canonical=pl.DataFrame(),
                persons_canonical=persons,
                households_ctramp=households_formatted,
                config=standard_config,
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
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        # Only individual tour should be included
        assert len(result) == 1
        assert result["tour_id"][0] == 0  # CTRAMP tour_id is 0-based (0 for first tour)


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

        # Format to CTRAMP
        households_formatted = format_households(households, persons, tours, standard_config)

        result = format_joint_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            joint_tours_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        assert len(result) == 1
        assert result["tour_id"][0] == 0  # CTRAMP joint tour_id is 0-based per household
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
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            joint_tours_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        # Only joint tour should be included
        assert len(result) == 1
        assert result["tour_id"][0] == 0  # CTRAMP joint tour_id is 0-based per household

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
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            joint_tours_canonical=pl.DataFrame(),
            persons_canonical=persons_formatted,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        # Should return empty DataFrame
        assert len(result) == 0


class TestWeightsAndSampleRateFormatting:
    """Tests for weight fields and sampleRate calculation in CTRAMP formatting output."""

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
        joint-tours table as ``joint_tour_weight`` rather than being relabelled from
        ``hh_weight``; ``sampleRate`` is 1/weight.
        """
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

        joint_tours_canonical = pl.DataFrame({"joint_tour_id": [5001], "joint_tour_weight": [2.5]})

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
        assert result["joint_tour_weight"][0] == pytest.approx(2.5)
        assert result["sampleRate"][0] == pytest.approx(1 / 2.5)

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
            .with_columns(pl.lit(2.0).alias("joint_trip_weight"))
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

        assert result["joint_trip_weight"][0] == 2.0
        assert result["sampleRate"][0] == 0.5
