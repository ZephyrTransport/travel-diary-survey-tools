"""Data validation functions for travel survey data using Pydantic models."""

import inspect
import logging
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field

import polars as pl
from pydantic import BaseModel

from data_canon.models import survey as survey_models
from data_canon.models import weighting as weighting_models
from data_canon.validation.column import (
    GeneratedColumn,
    check_generated_constraints,
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


CANONICAL_MODELS: dict[str, type[BaseModel]] = {
    "households": survey_models.HouseholdModel,
    "persons": survey_models.PersonModel,
    "days": survey_models.PersonDayModel,
    "unlinked_trips": survey_models.UnlinkedTripModel,
    "linked_trips": survey_models.LinkedTripModel,
    "tours": survey_models.TourModel,
    "joint_trips": survey_models.JointTripModel,
    "joint_tours": survey_models.JointTourModel,
    "habitual_locations": survey_models.HabitualLocationModel,
    "habitual_location_days": survey_models.HabitualLocationDayModel,
    "household_weights": weighting_models.HouseholdWeightingModel,
}


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
    joint_tours: pl.DataFrame | None = None
    habitual_locations: pl.DataFrame | None = None
    habitual_location_days: pl.DataFrame | None = None
    household_weights: pl.DataFrame | None = None

    # Model mapping for validation. Named at module scope as well, because the
    # declared relationships between these models are read outside validation --
    # a consumer removing records has to respect the same foreign keys.
    models: dict[str, type[BaseModel]] = field(default_factory=lambda: dict(CANONICAL_MODELS))

    # Columns a step generated whose names come from configuration rather than
    # a model field: zone ids, pre-imputation stashes, usability profiles,
    # per-profile weights. Populated at runtime by the steps that create them;
    # read when deciding what to deliver, and when checking what was promised.
    # Maps table -> column -> what the step promised of it, so a column a project
    # named can still explain itself and still be constrained.
    generated_columns: dict[str, dict[str, GeneratedColumn]] = field(
        default_factory=dict, repr=False
    )

    # Custom validators: table_name -> list of validator functions
    # Populated from custom_validation.CUSTOM_VALIDATORS
    custom_validators: dict[str, list[Callable]] = field(
        default_factory=lambda: {
            table: list(validators) for table, validators in CUSTOM_VALIDATORS.items()
        }
    )

    # Canonical table names in hierarchy order
    TABLE_NAMES: list[str] = field(
        default_factory=lambda: [
            "households",
            "persons",
            "days",
            "unlinked_trips",
            "linked_trips",
            "joint_trips",
            "tours",
            "joint_tours",
            "habitual_locations",
            "habitual_location_days",
            "household_weights",
        ],
        repr=False,
    )

    def __post_init__(self) -> None:
        """Validate FK references point to unique fields."""
        validate_fk_references(self.models)

    def register_generated_columns(
        self,
        table: str,
        columns: Iterable[str] | Mapping[str, "str | GeneratedColumn"],
    ) -> None:
        """Record columns a step generated whose names come from configuration.

        ``add_zone_ids`` emits ``{prefix}_{zone_name}``, the imputation step
        emits ``{column}_preimputed``, and ``cascade_completeness`` emits one
        column per usability profile, so the names differ per project and cannot
        be declared as model fields. They are still ours -- computed, named in
        config, and wanted in the delivered output -- so a step that creates them
        says so here rather than leaving a consumer to guess from the name.

        Pass a mapping to describe them as well. A model field carries its
        description to the codebook; a generated column has nowhere else to say
        what it means, so a column whose name a project chose can still explain
        itself. Passing a bare iterable records the names with no description.

        A mapping may give a [`GeneratedColumn`]
        [data_canon.validation.column.GeneratedColumn] instead of a string, to
        declare the constraints a model field would have carried. Without that a
        generated column is delivered unchecked.

        Args:
            table: Canonical table the columns were added to.
            columns: Column names, or column name -> description or
                ``GeneratedColumn``.
        """
        given = dict(columns) if isinstance(columns, Mapping) else dict.fromkeys(columns, "")
        registry = self.generated_columns.setdefault(table, {})
        for column, promise in given.items():
            spec = promise if isinstance(promise, GeneratedColumn) else GeneratedColumn(promise)
            # Never let a later bare registration blank what is already promised.
            if spec != GeneratedColumn() or column not in registry:
                registry[column] = spec

    def public_columns(self, table: str) -> set[str]:
        """Columns of *table* the pipeline stands behind: declared or generated."""
        model = self.models.get(table)
        declared = set(model.model_fields) if model is not None else set()
        return declared | set(self.generated_columns.get(table, {}))

    def describe_generated(self, table: str) -> dict[str, str]:
        """Descriptions for *table*'s generated columns, empty string where none."""
        return {
            column: spec.description
            for column, spec in self.generated_columns.get(table, {}).items()
        }

    def as_dict(self) -> dict[str, pl.DataFrame | None]:
        """Return all canonical tables as a dict (including None entries)."""
        return {name: getattr(self, name) for name in self.TABLE_NAMES}

    def as_dict_non_null(self) -> dict[str, pl.DataFrame]:
        """Return only non-None canonical tables as a dict."""
        return {name: df for name in self.TABLE_NAMES if (df := getattr(self, name)) is not None}

    def add_models(self, new_models: dict[str, type[BaseModel]]) -> None:
        """Add or override data models for validation.

        Also adds the new tables as canonical data attributes.

        Args:
            new_models: Dictionary of table name to Pydantic model class
        """
        for table_name, model in new_models.items():
            self.models[table_name] = model
            if not hasattr(self, table_name):
                setattr(self, table_name, None)
                self.__annotations__[table_name] = pl.DataFrame | None

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
            DataValidationError: If any validation check fails
        """
        if table_name not in self.models:
            valid_tables = ", ".join(self.models.keys())
            msg = f"Invalid table name: {table_name}. Valid tables: {valid_tables}"
            raise ValueError(msg)

        df = getattr(self, table_name)
        if df is None:
            logger.warning("Table '%s' is None - skipping validation", table_name)
            return

        start_time = time.time()
        step_info = f" for step '{step}'" if step else ""
        logger.info(
            "Validating table '%s'%s (%s rows)",
            table_name,
            step_info,
            f"{len(df):,}",
        )

        # 1. Column constraints (uniqueness, then what generated columns promised)
        # Extract unique fields from model metadata
        unique_fields = get_unique_fields(self.models[table_name])
        if unique_fields:
            check_unique_constraints(
                table_name,
                df,
                unique_fields,
            )
        check_generated_constraints(table_name, df, self.generated_columns.get(table_name, {}))

        # 2. Foreign key constraints
        # Extract FK fields from model metadata
        fk_fields = get_foreign_key_fields(self.models[table_name])
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
            self.models[table_name],
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
        if table_name not in self.custom_validators:
            return

        for validator_func in self.custom_validators[table_name]:
            # Inspect function signature to build arguments
            sig = inspect.signature(validator_func)
            kwargs = {}

            for param_name in sig.parameters:
                if hasattr(self, param_name):
                    table_df = getattr(self, param_name)
                    # Skip validator if required table is None
                    if table_df is None:
                        logger.warning(
                            "Skipping validator %s: required table '%s' is None",
                            validator_func.__name__,
                            param_name,
                        )
                        return
                    kwargs[param_name] = table_df
                else:
                    msg = (
                        f"Validator {validator_func.__name__} requires unknown table: {param_name}"
                    )
                    raise ValueError(msg)

            # Call validator
            errors = validator_func(**kwargs)
            if errors:
                # Convert string errors to structured errors
                error_msg = "; ".join(errors) if isinstance(errors, list) else str(errors)
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
        unique_fields = get_unique_fields(self.models[table_name])
        if not unique_fields:
            logger.warning(
                "Skipping required children check: no unique field found for '%s'",
                table_name,
            )
            return

        parent_col = unique_fields[0]

        # Find all child tables that have required_child FK to this table
        for child_table_name, child_model in self.models.items():
            required_child_fields = get_required_children_fields(child_model)

            for child_fk_col, (
                parent_table,
                _,
                when_col,
            ) in required_child_fields.items():
                # Check if this FK references current table
                if parent_table != table_name:
                    continue

                # With required_child_when, only parent rows where that column
                # is true need a child -- e.g. only *surveyable* persons must
                # have days; unrelated household members are enumerated but
                # file no travel. A missing column or a null value still needs
                # a child, so the constraint can never weaken by accident.
                must_have_children = df
                if when_col and when_col in df.columns:
                    must_have_children = df.filter(
                        pl.col(when_col).cast(pl.Boolean).fill_null(value=True)
                    )
                elif when_col:
                    logger.warning(
                        "required_child_when column '%s' not in '%s'; "
                        "requiring children for every row",
                        when_col,
                        table_name,
                    )
                parent_ids = set(must_have_children[parent_col].to_list())

                child_table = child_table_name
                child_df = getattr(self, child_table)

                if child_df is None:
                    logger.warning(
                        "Skipping required children check: child table '%s' is None",
                        child_table,
                    )
                    continue

                if child_fk_col not in child_df.columns:
                    logger.warning(
                        "Skipping required children check: FK column '%s' not in '%s'",
                        child_fk_col,
                        child_table,
                    )
                    continue

                child_parent_ids = set(child_df[child_fk_col].drop_nulls().unique().to_list())
                parents_without_children = parent_ids - child_parent_ids

                if parents_without_children:
                    missing_list = sorted(parents_without_children)
                    max_display = 10
                    sample = missing_list[:max_display]
                    sample_str = ", ".join(str(v) for v in sample)
                    has_more = len(parents_without_children) > max_display
                    ellipsis = " ..." if has_more else ""
                    when_note = f" (only rows where {when_col} is true)" if when_col else ""
                    msg = (
                        f"Found {len(parents_without_children)} "
                        f"'{table_name}' records with no '{child_table}' "
                        f"children{when_note}. Sample: {sample_str}{ellipsis}"
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
                if table_name not in self.models:
                    msg = f"Unknown table: {table_name}"
                    raise ValueError(msg)
                if table_name not in self.custom_validators:
                    self.custom_validators[table_name] = []
                self.custom_validators[table_name].append(func)
            return func

        return decorator
