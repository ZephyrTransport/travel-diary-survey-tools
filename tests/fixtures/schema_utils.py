"""Schema utilities for generating Polars schemas from data models.

Provides functions to convert Pydantic data models to Polars schemas and
create properly-typed empty DataFrames for testing.
"""

from datetime import datetime
from enum import Enum
from typing import get_args

import polars as pl
from pydantic import BaseModel

from data_canon.models.survey import (
    JointTourModel,
    LinkedTripModel,
    PersonDayModel,
    TourModel,
    UnlinkedTripModel,
)


def model_to_polars_schema(model: type[BaseModel]) -> dict[str, type]:
    """Convert a Pydantic BaseModel to a Polars schema dictionary.

    Introspects the model's field annotations to generate appropriate
    Polars data types. Handles optional fields by preserving nullable types.

    Args:
        model: Pydantic BaseModel class to convert

    Returns:
        Dictionary mapping field names to Polars data types

    Example:
        >>> schema = model_to_polars_schema(LinkedTripModel)
        >>> df = pl.DataFrame(schema=schema)
    """
    schema = {}

    for field_name, field_info in model.model_fields.items():
        annotation = field_info.annotation

        # Check if it's an optional type (has None in union)
        type_args = get_args(annotation)
        if type_args and type(None) in type_args:
            # Extract the non-None type from the union
            actual_type = next(t for t in type_args if t is not type(None))
        else:
            actual_type = annotation

        # Map Python/Pydantic types to Polars types
        polars_type = _python_type_to_polars(actual_type)
        schema[field_name] = polars_type

    return schema


def _python_type_to_polars(python_type) -> type:
    """Map Python type annotations to Polars data types.

    Args:
        python_type: Python type annotation

    Returns:
        Corresponding Polars data type
    """
    # Map basic types to Polars types
    type_mapping = {
        int: pl.Int64,
        float: pl.Float64,
        str: pl.String,
        bool: pl.Boolean,
        datetime: pl.Datetime,
    }

    # Check direct type match
    if python_type in type_mapping:
        return type_mapping[python_type]

    # Check for generic types with __origin__
    if hasattr(python_type, "__origin__") and python_type.__origin__ in type_mapping:
        return type_mapping[python_type.__origin__]

    # Handle Enum types - check if values are integers
    if hasattr(python_type, "__mro__") and Enum in python_type.__mro__:
        # Check if this enum has integer values by inspecting first member
        try:
            first_member = next(iter(python_type))
            if isinstance(first_member.value, int):
                return pl.Int64
        except (StopIteration, AttributeError):
            pass

    # Handle enums and other int-based types
    if hasattr(python_type, "__bases__"):
        if int in python_type.__bases__:
            return pl.Int64
        if str in python_type.__bases__:
            return pl.String

    # Default to String for unknown types (can be extended as needed)
    return pl.String


def empty_linked_trips() -> pl.DataFrame:
    """Create empty linked_trips DataFrame with complete schema.

    Returns properly typed empty DataFrame that passes @step validation checks
    for LinkedTripModel.

    Returns:
        Empty DataFrame with LinkedTripModel schema
    """
    return pl.DataFrame(schema=model_to_polars_schema(LinkedTripModel))


def empty_tours() -> pl.DataFrame:
    """Create empty tours DataFrame with complete schema.

    Returns properly typed empty DataFrame that passes @step validation checks
    for TourModel.

    Returns:
        Empty DataFrame with TourModel schema
    """
    return pl.DataFrame(schema=model_to_polars_schema(TourModel))


def empty_unlinked_trips() -> pl.DataFrame:
    """Create empty unlinked_trips DataFrame with complete schema.

    Returns:
        Empty DataFrame with UnlinkedTripModel schema
    """
    return pl.DataFrame(schema=model_to_polars_schema(UnlinkedTripModel))


def empty_joint_tours() -> pl.DataFrame:
    """Create empty joint_tours DataFrame with complete schema.

    Returns:
        Empty DataFrame with JointTourModel schema
    """
    return pl.DataFrame(schema=model_to_polars_schema(JointTourModel))


def empty_days() -> pl.DataFrame:
    """Create empty person-days DataFrame with complete schema.

    Returns:
        Empty DataFrame with PersonDayModel schema
    """
    return pl.DataFrame(schema=model_to_polars_schema(PersonDayModel))


def days_for_persons(persons: pl.DataFrame, day_num: int = 1) -> pl.DataFrame:
    """Build a single-day person-day table covering every person given.

    ``format_ctramp`` expands households and persons to person-day rows with an
    *inner* join on the ``days`` table, and restricts tours/trips to those days
    with a semi-join. A test that passes a ``days`` table therefore gets empty
    CT-RAMP output for every person not represented here.

    ``day_id`` is a person-day key and must be unique, so it follows the same
    convention as the pipeline and the e2e toy data: ``person_id * 100 + day_num``.
    CT-RAMP ids derive from it, so a person's CT-RAMP ``person_id`` becomes that
    ``day_id`` and its household becomes ``hh_id * 100 + day_num``.

    Args:
        persons: Canonical persons DataFrame with person_id and hh_id
        day_num: Day number within the survey period

    Returns:
        One row per person, or an empty day frame if there are no persons
    """
    if persons.is_empty():
        return empty_days()
    return persons.select(
        (pl.col("person_id") * 100 + day_num).cast(pl.Int64).alias("day_id"),
        pl.col("person_id"),
        pl.col("hh_id"),
    ).unique()


def empty_joint_trips() -> pl.DataFrame:
    """Create empty joint_trips DataFrame with complete schema.

    Returns properly typed empty DataFrame that passes @step validation checks.
    Note: JointTripModel is generated dynamically, so we define the schema manually.

    Returns:
        Empty DataFrame with joint trips schema
    """
    return pl.DataFrame(
        schema={
            "joint_trip_id": pl.Int64,
            "hh_id": pl.Int64,
            "day_id": pl.Int64,
            "joint_trip_num": pl.Int64,
            "num_participants": pl.Int64,
            "o_purpose": pl.Int64,
            "o_purpose_category": pl.Int64,
            "d_purpose": pl.Int64,
            "d_purpose_category": pl.Int64,
            "mode_type": pl.Int64,
            "travel_dow": pl.Int64,
        }
    )
