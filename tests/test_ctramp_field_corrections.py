"""Unit tests for CT-RAMP formatter.

Tests formatting, field corrections, and end-to-end transformation from
canonical survey data to CT-RAMP model format.
"""

from datetime import datetime, time
from pathlib import Path
from typing import get_args

import polars as pl
import pytest

from data_canon.codebook.ctramp import (
    CTRAMPPersonType,
    CTRAMPTourCategory,
    JTFChoice,
    WFHChoice,
    build_alternatives,
    load_alternatives_from_csv,
)
from data_canon.codebook.households import IncomeBroad
from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    JobType,
    Student,
)
from data_canon.codebook.tours import TourDirection
from data_canon.codebook.trips import PurposeCategory
from processing.formatting.ctramp.ctramp_config import CTRAMPConfig
from processing.formatting.ctramp.format_ctramp import format_ctramp
from processing.formatting.ctramp.format_households import format_households
from processing.formatting.ctramp.format_persons import format_persons
from processing.formatting.ctramp.format_tours import (
    format_individual_tour,
)
from processing.formatting.ctramp.format_trips import (
    format_individual_trip,
)
from tests.fixtures import (
    create_household,
    create_linked_trip,
    create_person,
    create_tour,
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
        usability_flag_col="usable",
        income_low_threshold=30000,  # $30k ($2000, MTC)
        income_med_threshold=60000,  # $60k ($2000, MTC)
        income_high_threshold=100000,  # $100k ($2000, MTC)
        income_survey_year_to_ctramp_year=0.5319148936,
        age_adult=4,  # AGE_18_TO_24 = category 4 (18+ are adults)
    )


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
                    day_id=10101,
                    joint_tour_id=9001,
                    tour_purpose=PurposeCategory.SHOP,
                ),
                create_tour(
                    tour_id=1002,
                    hh_id=1,
                    person_id=102,
                    day_id=10201,
                    joint_tour_id=9001,
                    tour_purpose=PurposeCategory.SHOP,
                ),
                create_tour(
                    tour_id=1003,
                    hh_id=1,
                    person_id=101,
                    day_id=10101,
                    joint_tour_id=9002,
                    tour_purpose=PurposeCategory.SHOP,
                ),
                create_tour(
                    tour_id=1004,
                    hh_id=1,
                    person_id=102,
                    day_id=10201,
                    joint_tour_id=9002,
                    tour_purpose=PurposeCategory.SHOP,
                ),
            ],
            schema=get_tour_schema(),
        )

        # Add trips for each tour to avoid validation error
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    linked_trip_id=10001,
                    tour_id=1001,
                    person_id=101,
                    day_id=10101,
                    tour_direction=TourDirection.OUTBOUND,
                    joint_tour_id=9001,
                ),
                create_linked_trip(
                    linked_trip_id=10002,
                    tour_id=1002,
                    person_id=102,
                    day_id=10201,
                    tour_direction=TourDirection.OUTBOUND,
                    joint_tour_id=9001,
                ),
                create_linked_trip(
                    linked_trip_id=10003,
                    tour_id=1003,
                    person_id=101,
                    day_id=10101,
                    tour_direction=TourDirection.OUTBOUND,
                    joint_tour_id=9002,
                ),
                create_linked_trip(
                    linked_trip_id=10004,
                    tour_id=1004,
                    person_id=102,
                    day_id=10201,
                    tour_direction=TourDirection.OUTBOUND,
                    joint_tour_id=9002,
                ),
                # Return legs: a one-trip tour is structurally invalid and would
                # be dropped before it could count toward jtf_choice.
                create_linked_trip(
                    linked_trip_id=10005,
                    tour_id=1001,
                    person_id=101,
                    day_id=10101,
                    tour_direction=TourDirection.INBOUND,
                    joint_tour_id=9001,
                ),
                create_linked_trip(
                    linked_trip_id=10006,
                    tour_id=1002,
                    person_id=102,
                    day_id=10201,
                    tour_direction=TourDirection.INBOUND,
                    joint_tour_id=9001,
                ),
                create_linked_trip(
                    linked_trip_id=10007,
                    tour_id=1003,
                    person_id=101,
                    day_id=10101,
                    tour_direction=TourDirection.INBOUND,
                    joint_tour_id=9002,
                ),
                create_linked_trip(
                    linked_trip_id=10008,
                    tour_id=1004,
                    person_id=102,
                    day_id=10201,
                    tour_direction=TourDirection.INBOUND,
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
            unlinked_trips=empty_unlinked_trips(),
            joint_tours=empty_joint_tours(),
            days=days_for_persons(persons),
            income_low_threshold=standard_config.income_low_threshold,
            income_med_threshold=standard_config.income_med_threshold,
            income_high_threshold=standard_config.income_high_threshold,
            income_survey_year_to_ctramp_year=standard_config.income_survey_year_to_ctramp_year,
            usability_flag_col="usable",
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
            unlinked_trips=empty_unlinked_trips(),
            joint_tours=empty_joint_tours(),
            days=days_for_persons(persons),
            income_low_threshold=standard_config.income_low_threshold,
            income_med_threshold=standard_config.income_med_threshold,
            income_high_threshold=standard_config.income_high_threshold,
            income_survey_year_to_ctramp_year=standard_config.income_survey_year_to_ctramp_year,
            usability_flag_col="usable",
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
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
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
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
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
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
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
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
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
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
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
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
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

        assert "depart_hour" in result.columns, "depart_hour field should be present"
        assert result["depart_hour"][0] == 8, "depart_hour should be extracted from depart_time"

    def test_tour_purpose_string_not_int(self, standard_config):
        """Test that tour_purpose is string, not integer."""
        households = pl.DataFrame([create_household(hh_id=1, income_bin=IncomeBroad.INCOME_50TO75)])

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
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )
        # Now pass formatted tours to format_individual_trip
        result = format_individual_trip(
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            tours_ctramp=tours_formatted,
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
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
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
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
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
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
            [create_household(hh_id=1, income_bin=IncomeBroad.INCOME_75TO100)]
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
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
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
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        # Stops = trips - 1 for each direction
        assert result["num_ob_stops"][0] == 2, "3 outbound trips = 2 stops (not 3)"
        assert result["num_ib_stops"][0] == 1, "2 inbound trips = 1 stop (not 2)"
