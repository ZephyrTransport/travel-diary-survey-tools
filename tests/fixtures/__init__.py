"""Test fixtures for travel diary survey processing tests.

This module provides a unified interface to canonical test data builders.
All functionality has been reorganized into focused modules but is re-exported
here for backward compatibility.

Modules:
    - base_records: create_household, create_person, create_day
    - trip_records: create_unlinked_trip, create_linked_trip, create_trip
    - tour_records: create_tour, get_tour_schema
    - scenario_builders: simple_work_tour, transit_commute,
      multi_stop_tour, etc.
    - fixtures: pytest fixtures and process_scenario_through_pipeline
"""

# Re-export constants
# Re-export base record builders
from .base_records import create_day, create_household, create_person

# Re-export fixtures and pipeline processing
from .fixtures import (
    add_test_taz_maz_ids,
    create_multi_person_household_processed,
    create_simple_work_tour_processed,
    create_transit_commute_processed,
    multi_stop_tour_processed,
    multi_tour_day_processed,
    process_scenario_through_pipeline,
    simple_work_tour_processed,
)
from .locations import (
    BART_HOME_LOCATION,
    BART_WORK_LOCATION,
    HOME_2_LOCATION,
    HOME_3_LOCATION,
    HOME_LOCATION,
    LOCATIONS,
    RESTAURANT_LOCATION,
    SCHOOL_COLLEGE_LOCATION,
    SCHOOL_HIGH_LOCATION,
    SHOPPING_LOCATION,
    WORK_2_LOCATION,
    WORK_3_LOCATION,
    WORK_LOCATION,
    Location,
    lookup_location,
)

# Re-export scenario builders
from .scenario_builders import (
    DEFAULT_TRANSIT_MODE_CODES,
    create_family_household,
    create_retired_household,
    create_single_adult_household,
    create_university_student_household,
    multi_person_household,
    multi_stop_tour,
    multi_tour_day,
    simple_work_tour,
    transit_commute,
    work_tour_no_usual_location,
)

# Re-export tour builders
from .tour_records import create_tour, get_tour_schema

# Re-export trip builders
from .trip_records import (
    create_linked_trip,
    create_unlinked_trip,
)

__all__ = [
    "BART_HOME_LOCATION",
    "BART_WORK_LOCATION",
    # Constants
    "DEFAULT_TRANSIT_MODE_CODES",
    "HOME_2_LOCATION",
    "HOME_3_LOCATION",
    "HOME_LOCATION",
    "LOCATIONS",
    "RESTAURANT_LOCATION",
    "SCHOOL_COLLEGE_LOCATION",
    "SCHOOL_HIGH_LOCATION",
    "SHOPPING_LOCATION",
    "WORK_2_LOCATION",
    "WORK_3_LOCATION",
    "WORK_LOCATION",
    # Location constants
    "Location",
    # Base records
    "add_test_taz_maz_ids",
    "create_day",
    "create_family_household",
    "create_household",
    "create_linked_trip",
    "create_multi_person_household_processed",
    "create_person",
    "create_retired_household",
    "create_simple_work_tour_processed",
    "create_single_adult_household",
    "create_tour",
    "create_transit_commute_processed",
    "create_university_student_household",
    "create_unlinked_trip",
    "get_tour_schema",
    "lookup_location",
    "multi_person_household",
    "multi_stop_tour",
    "multi_stop_tour_processed",
    "multi_tour_day",
    "multi_tour_day_processed",
    "process_scenario_through_pipeline",
    "simple_work_tour",
    "simple_work_tour_processed",
    "transit_commute",
    "work_tour_no_usual_location",
]
