"""Ensure test fixtures meet validation requirements for all pipeline steps."""

import pytest

from data_canon.models.survey import (
    HouseholdModel,
    LinkedTripModel,
    PersonModel,
)
from data_canon.validation.row import validate_row_for_step
from tests.fixtures.tour_test_data import ScenarioBuilder, TestDataBuilder


def test_person_fixture_has_all_required_fields():
    """Verify person test data includes all fields for extract_tours step."""
    person_dict = TestDataBuilder.create_minimal_person()

    # Should not raise validation error
    try:
        validate_row_for_step(person_dict, PersonModel, step_name="extract_tours")
    except (ValueError, KeyError, TypeError) as e:
        pytest.fail(f"Person fixture missing required fields: {e}")


def test_household_fixture_has_all_required_fields():
    """Verify household test data includes all required fields."""
    hh_dict = TestDataBuilder.create_minimal_household()

    try:
        validate_row_for_step(hh_dict, HouseholdModel, step_name="extract_tours")
    except (ValueError, KeyError, TypeError) as e:
        pytest.fail(f"Household fixture missing required fields: {e}")


def test_trip_fixture_has_all_required_fields():
    """Verify trip test data includes all required fields."""
    trip_dict = TestDataBuilder.create_minimal_trip(trip_id=1)

    try:
        validate_row_for_step(trip_dict, LinkedTripModel, step_name="extract_tours")
    except (ValueError, KeyError, TypeError) as e:
        pytest.fail(f"Trip fixture missing required fields: {e}")


def test_all_scenarios_validate():
    """Ensure all pre-built scenarios pass validation."""
    scenarios = [
        ("simple_work_tour", ScenarioBuilder.simple_work_tour),
        ("work_tour_with_subtour", ScenarioBuilder.work_tour_with_subtour),
        ("multiple_tours_same_day", ScenarioBuilder.multiple_tours_same_day),
        ("no_work_location", ScenarioBuilder.no_work_location),
    ]

    for name, scenario_fn in scenarios:
        hh, persons, trips = scenario_fn()

        # Validate each DataFrame
        for person_row in persons.to_dicts():
            try:
                validate_row_for_step(person_row, PersonModel, "extract_tours")
            except (ValueError, KeyError, TypeError) as e:
                pytest.fail(f"Scenario '{name}' person validation failed: {e}")

        for hh_row in hh.to_dicts():
            try:
                validate_row_for_step(hh_row, HouseholdModel, "extract_tours")
            except (ValueError, KeyError, TypeError) as e:
                pytest.fail(f"Scenario '{name}' household validation failed: {e}")

        for trip_row in trips.to_dicts():
            try:
                validate_row_for_step(trip_row, LinkedTripModel, "extract_tours")
            except (ValueError, KeyError, TypeError) as e:
                pytest.fail(f"Scenario '{name}' trip validation failed: {e}")
