"""A model field is required only in the steps whose contract asks for it."""

from datetime import datetime
from typing import Any

import pytest
from pydantic import ValidationError as PydanticValidationError

import processing  # triggers step registration
from data_canon.models.survey import UnlinkedTripModel
from data_canon.validation.row import (
    get_required_fields_for_step,
    validate_row_for_step,
)
from pipeline.step_registry import STEP_REGISTRY

DATETIMES = {
    "depart_time": datetime(2024, 1, 15, 10, 0, 0),
    "arrive_time": datetime(2024, 1, 15, 11, 30, 0),
}
COORDINATES = {
    "o_lat": 37.7749,
    "o_lon": -122.4194,
    "d_lat": 37.7849,
    "d_lon": -122.4094,
}


def trip_row(**overrides: Any) -> dict[str, Any]:
    """A trip row without ``linked_trip_id``, the datetimes or the coordinates."""
    row = {
        "unlinked_trip_id": 1,
        "person_id": 101,
        "hh_id": 1,
        "person_day_id": 10101,
        "depart_date": "2024-01-15",
        "depart_hour": 10,
        "depart_minute": 0,
        "depart_seconds": 0,
        "arrive_date": "2024-01-15",
        "arrive_hour": 11,
        "arrive_minute": 30,
        "arrive_seconds": 0,
        "o_purpose_category": 1,
        "d_purpose_category": 2,
        "mode_type": 1,
        "duration_minutes": 90.0,
        "distance_miles": 10.5,
    }
    row.update(overrides)
    return row


class TestSelectiveFieldRequirements:
    """Which fields each step requires."""

    @pytest.mark.parametrize(
        ("field", "step", "required"),
        [
            # linked_trip_id is produced by link_trips, so only extract_tours needs it.
            ("linked_trip_id", "link_trips", False),
            ("linked_trip_id", "extract_tours", True),
            # depart_time/arrive_time are built after load_data.
            ("depart_time", "load_data", False),
            ("arrive_time", "load_data", False),
            ("depart_time", "link_trips", True),
            ("arrive_time", "link_trips", True),
            ("depart_time", "extract_tours", True),
            ("arrive_time", "extract_tours", True),
        ],
    )
    def test_step_specific_fields_required_only_in_that_step(
        self, field: str, step: str, required: bool
    ):
        """A field is required only from the step whose contract names it."""
        assert (field in get_required_fields_for_step("unlinked_trips", step)) is required

    def test_an_unregistered_step_requires_nothing(self):
        """An unknown step, or a table the step does not declare, requires no fields."""
        assert get_required_fields_for_step("unlinked_trips", "no_such_step") == set()
        assert get_required_fields_for_step("no_such_table", "extract_tours") == set()


class TestStepValidationBehavior:
    """Row validation against a step's contract."""

    @pytest.mark.parametrize(
        ("row", "step"),
        [
            # add_zone_ids requires the coordinates but not linked_trip_id or the datetimes.
            pytest.param(trip_row(**COORDINATES), "add_zone_ids", id="zones-no-linked-id"),
            pytest.param(
                trip_row(linked_trip_id=1, tour_id=1, **DATETIMES),
                "extract_tours",
                id="tours-complete",
            ),
            pytest.param(trip_row(**DATETIMES, **COORDINATES), "link_trips", id="link-complete"),
        ],
    )
    def test_validation_passes_when_the_step_has_what_it_needs(
        self, row: dict[str, Any], step: str
    ):
        """A row missing fields another step requires still validates for this one."""
        validate_row_for_step(row, UnlinkedTripModel, "unlinked_trips", step)

    @pytest.mark.parametrize(
        ("row", "step", "match"),
        [
            pytest.param(
                trip_row(**DATETIMES), "extract_tours", "linked_trip_id", id="tours-no-linked-id"
            ),
            pytest.param(
                trip_row(**COORDINATES),
                "link_trips",
                r"depart_time|arrive_time",
                id="link-no-datetimes",
            ),
        ],
    )
    def test_validation_fails_without_step_specific_fields(
        self, row: dict[str, Any], step: str, match: str
    ):
        """A row missing a field its step requires fails."""
        with pytest.raises(ValueError, match=match):
            validate_row_for_step(row, UnlinkedTripModel, "unlinked_trips", step)

    def test_validates_present_fields_even_if_not_required_in_step(self):
        """A present field is checked against its constraints even where it is not required."""
        row = trip_row(linked_trip_id=-5)  # invalid: must be >= 1

        with pytest.raises(PydanticValidationError, match="greater than or equal"):
            validate_row_for_step(row, UnlinkedTripModel, "unlinked_trips", "load_data")


# Steps that declare no columns, and why. Any other step must declare a contract.
_NO_CONTRACT = {
    "load_data": "reads whatever files the config names",
    "write_data": "writes whatever tables exist",
}


def test_every_step_declares_its_columns():
    """A step with an empty ``@step()`` is either listed above or a forgotten contract."""
    # Only the package's steps: other tests register throwaway steps in the same registry.
    steps = {name for name in dir(processing) if name in STEP_REGISTRY}
    undeclared = sorted(name for name in steps if not STEP_REGISTRY[name].tables)
    assert undeclared == sorted(_NO_CONTRACT), (
        f"Steps declaring no requires/produces: {undeclared}. Declare what the step reads "
        f"and writes, or add it to _NO_CONTRACT with the reason."
    )
