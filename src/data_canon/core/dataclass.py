"""Data validation functions for travel survey data using Pydantic models."""

import inspect
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field

import polars as pl
from pydantic import BaseModel

from data_canon.models import daysim as daysim_models
from data_canon.models import survey as survey_models
from data_canon.validation.column import (
    check_unique_constraints,
    get_unique_fields,
)
from data_canon.validation.custom import CUSTOM_VALIDATORS
from data_canon.validation.relational import (
    check_foreign_keys,
    get_foreign_key_fields,
    get_required_children_fields,
    validate_fk_references,
)
from data_canon.validation.row import validate_dataframe_rows

from .exceptions import DataValidationError

logger = logging.getLogger(__name__)


@dataclass
class CanonicalData:
    """Canonical data structure for travel survey data with validation.

    Use the validate() method to validate specific tables.
    """

    households: pl.DataFrame | None = None
    persons: pl.DataFrame | None = None
    days: pl.DataFrame | None = None
    unlinked_trips: pl.DataFrame | None = None
    linked_trips: pl.DataFrame | None = None
    tours: pl.DataFrame | None = None
    joint_trips: pl.DataFrame | None = None

    # Daysim-specific tables
    households_daysim: pl.DataFrame | None = None
    persons_daysim: pl.DataFrame | None = None
    days_daysim: pl.DataFrame | None = None
    linked_trips_daysim: pl.DataFrame | None = None
    tours_daysim: pl.DataFrame | None = None

    # Model mapping for validation
    _models: dict[str, type[BaseModel]] = field(
        default_factory=lambda: {
            "households": survey_models.HouseholdModel,
            "persons": survey_models.PersonModel,
            "days": survey_models.PersonDayModel,
            "unlinked_trips": survey_models.UnlinkedTripModel,
            "linked_trips": survey_models.LinkedTripModel,
            "tours": survey_models.TourModel,
            "joint_trips": survey_models.JointTripModel,
            # Daysim models
            "households_daysim": daysim_models.HouseholdDaysimModel,
            "persons_daysim": daysim_models.PersonDaysimModel,
            "days_daysim": daysim_models.PersonDayDaysimModel,
            "linked_trips_daysim": daysim_models.LinkedTripDaysimModel,
            "tours_daysim": daysim_models.TourDaysimModel,
        }
    )

    # Custom validators: table_name -> list of validator functions
    # Populated from custom_validation.CUSTOM_VALIDATORS
    _custom_validators: dict[str, list[Callable]] = field(
        default_factory=lambda: {
            table: list(validators)
            for table, validators in CUSTOM_VALIDATORS.items()
        }
    )

    def __post_init__(self) -> None:
        """Validate FK references point to unique fields."""
        validate_fk_references(self._models)

    def validate(self, table_name: str, step: str | None = None) -> None:
        """Validate a table through all validation layers.

        Runs validation in this order:
        1. Column constraints (uniqueness)
        2. Foreign key constraints
        3. Row-level Pydantic validation (step-aware if step provided)
        4. Custom user-registered validators

        Args:
            table_name: Name of the table to validate
            step: Pipeline step name for step-aware validation.
                 If None, validates all fields strictly.

        Raises:
            DataDataValidationError: If any validation check fails
        """
        if table_name not in self._models:
            valid_tables = ", ".join(self._models.keys())
            msg = (
                f"Invalid table name: {table_name}. "
                f"Valid tables: {valid_tables}"
            )
            raise ValueError(msg)

        df = getattr(self, table_name)
        if df is None:
            logger.warning(
                "Table '%s' is None - skipping validation", table_name
            )
            return

        start_time = time.time()
        step_info = f" for step '{step}'" if step else ""
        logger.info(
            "Validating table '%s'%s (%s rows)",
            table_name,
            step_info,
            f"{len(df):,}",
        )

        # 1. Column constraints (uniqueness)
        # Extract unique fields from model metadata
        unique_fields = get_unique_fields(self._models[table_name])
        if unique_fields:
            check_unique_constraints(
                table_name,
                df,
                unique_fields,
            )

        # 2. Foreign key constraints
        # Extract FK fields from model metadata
        fk_fields = get_foreign_key_fields(self._models[table_name])
        if fk_fields:
            check_foreign_keys(
                table_name,
                df,
                fk_fields,
                lambda t: getattr(self, t),
            )

        # 3. Row validation (step-aware)
        validate_dataframe_rows(
            table_name,
            df,
            self._models[table_name],
            step,
        )

        # 4. Custom validators
        self._run_custom_validators(table_name, df)

        # 5. Required children (bidirectional FK check)
        self._check_required_children(table_name, df)

        elapsed = time.time() - start_time
        logger.info(
            "✓ Table '%s'%s validated successfully in %.2fs",
            table_name,
            step_info,
            elapsed,
        )

    def _run_custom_validators(
        self,
        table_name: str,
        _df: pl.DataFrame,
    ) -> None:
        """Run user-registered custom validators for a table.

        Args:
            table_name: Name of the table being validated
            df: DataFrame being validated
        """
        if table_name not in self._custom_validators:
            return

        for validator_func in self._custom_validators[table_name]:
            # Inspect function signature to build arguments
            sig = inspect.signature(validator_func)
            kwargs = {}

            for param_name in sig.parameters:
                if hasattr(self, param_name):
                    table_df = getattr(self, param_name)
                    # Skip validator if required table is None
                    if table_df is None:
                        logger.warning(
                            "Skipping validator %s: required table '%s' "
                            "is None",
                            validator_func.__name__,
                            param_name,
                        )
                        return
                    kwargs[param_name] = table_df
                else:
                    msg = (
                        f"Validator {validator_func.__name__} requires "
                        f"unknown table: {param_name}"
                    )
                    raise ValueError(msg)

            # Call validator
            errors = validator_func(**kwargs)
            if errors:
                # Convert string errors to structured errors
                if isinstance(errors, list):
                    error_msg = "; ".join(errors)
                else:
                    error_msg = str(errors)
                raise DataValidationError(
                    table=table_name,
                    rule=validator_func.__name__,
                    message=error_msg,
                )

    def _check_required_children(
        self,
        table_name: str,
        df: pl.DataFrame,
    ) -> None:
        """Check that all records have required children (bidirectional FK).

        Iterates through all other tables to find FK fields with
        required_child=True that reference this table.

        Args:
            table_name: Name of the table being validated
            df: DataFrame being validated
        """
        # Get the unique field from model metadata
        unique_fields = get_unique_fields(self._models[table_name])
        if not unique_fields:
            logger.warning(
                "Skipping required children check: no unique field found "
                "for '%s'",
                table_name,
            )
            return

        parent_col = unique_fields[0]
        parent_ids = set(df[parent_col].to_list())

        # Find all child tables that have required_child FK to this table
        for child_table_name, child_model in self._models.items():
            required_child_fields = get_required_children_fields(child_model)

            for child_fk_col, (
                parent_table,
                _,
            ) in required_child_fields.items():
                # Check if this FK references current table
                if parent_table != table_name:
                    continue

                child_table = child_table_name
                child_df = getattr(self, child_table)

                if child_df is None:
                    logger.warning(
                        "Skipping required children check: child table '%s' "
                        "is None",
                        child_table,
                    )
                    continue

                if child_fk_col not in child_df.columns:
                    logger.warning(
                        "Skipping required children check: FK column '%s' "
                        "not in '%s'",
                        child_fk_col,
                        child_table,
                    )
                    continue

                child_parent_ids = set(
                    child_df[child_fk_col].drop_nulls().unique().to_list()
                )
                parents_without_children = parent_ids - child_parent_ids

                if parents_without_children:
                    missing_list = sorted(parents_without_children)
                    max_display = 10
                    sample = missing_list[:max_display]
                    sample_str = ", ".join(str(v) for v in sample)
                    has_more = len(parents_without_children) > max_display
                    ellipsis = " ..." if has_more else ""
                    msg = (
                        f"Found {len(parents_without_children)} "
                        f"'{table_name}' records with no '{child_table}' "
                        f"children. Sample: {sample_str}{ellipsis}"
                    )
                    raise DataValidationError(
                        table=table_name,
                        rule="required_children",
                        column=parent_col,
                        message=msg,
                    )

    # Left this in here for future extension, but not currently used.
    def register_validator(self, *table_names: str) -> Callable:
        """Register a custom validator on one or more tables.

        Args:
            *table_names: One or more table names to register validator on

        Returns:
            Decorator function

        Example:
            >>> @data.register_validator("tours")
            >>> def check_tours(tours: pl.DataFrame) -> list[str]:
            >>>     errors = []
            >>>     # Check logic
            >>>     return errors

            >>> @data.register_validator("tours", "linked_trips")
            >>> def check_consistency(
            >>>     tours: pl.DataFrame,
            >>>     linked_trips: pl.DataFrame
            >>> ) -> list[str]:
            >>>     # Multi-table check
            >>>     return []
        """
        if not table_names:
            msg = "Must specify at least one table name"
            raise ValueError(msg)

        def decorator(func: Callable) -> Callable:
            for table_name in table_names:
                if table_name not in self._models:
                    msg = f"Unknown table: {table_name}"
                    raise ValueError(msg)
                if table_name not in self._custom_validators:
                    self._custom_validators[table_name] = []
                self._custom_validators[table_name].append(func)
            return func

        return decorator
