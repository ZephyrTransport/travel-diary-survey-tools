"""Final Validation Step.

Performs final validation checks on the complete processed dataset to ensure data
quality and schema compliance before export. This is a pass-through validation step
that leverages the `@step()` decorator's automatic Pydantic model validation.

!!! Algorithm

    # Pydantic Model Validation

    1. This step is decorated with `@step(validate_input=True, validate_output=True)`
    2. The pipeline framework automatically validates all input/output against Pydantic
       data models
    3. Validation checks:
        - **Schema Compliance:** All required columns present with correct data types
        - **Value Constraints:** Numeric ranges, categorical values, enum memberships
        - **Referential Integrity:** Foreign keys match (person_id → persons,
          hh_id → households, etc.)
        - **Business Rules:** Domain-specific constraints (e.g., depart_time < arrive_time)

    # Custom Validation Space

    - The function body is intentionally simple (pass-through)
    - Pydantic handles validation automatically at model instantiation
    - This space *could* be extended with additional custom checks not covered by models:
        - Cross-table consistency checks
        - Statistical outlier detection
        - Survey-specific business rules
        - Data quality metrics logging
    - However, validation logic should ideally be implemented in Pydantic models
      themselves for reusability

    # Validation Failure Handling

    - If validation fails, raises `DataValidationError` with detailed error messages
    - Error messages indicate:
        - Which table failed validation
        - Which rows/columns have issues
        - What constraint was violated
    - Pipeline execution halts on validation failure

!!! Notes

    - This is the last checkpoint before data export
    - Ensures output meets canonical data specifications
    - Validation errors caught here prevent invalid data from reaching models/analyses
    - Pydantic models defined in `src/data_canon/models/` provide the validation rules
    - Comprehensive logging helps diagnose data quality issues
    - Pass-through design allows validation to occur transparently
"""

import logging

import polars as pl

from pipeline import step

logger = logging.getLogger(__name__)


@step(validate_input=True, validate_output=True)
def final_check(
    households: pl.DataFrame,
    persons: pl.DataFrame,
    days: pl.DataFrame,
    unlinked_trips: pl.DataFrame,
    linked_trips: pl.DataFrame,
    tours: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """Run comprehensive validation on all canonical survey tables.

    This is a pass-through function that relies on the `@step()` decorator to
    perform automatic Pydantic model validation on both inputs and outputs.
    Validation checks schema compliance, value constraints, referential integrity,
    and business rules.

    Args:
        households: Processed household table with all required fields
        persons: Processed person table with all required fields
        days: Processed person-day table with all required fields
        unlinked_trips: Processed unlinked trip records with all required fields
        linked_trips: Processed linked trip records (journey-level) with all
            required fields
        tours: Processed tour records with all required fields

    Returns:
        Dictionary containing the same validated tables:

            - households: Validated household table
            - persons: Validated person table
            - days: Validated person-day table
            - unlinked_trips: Validated unlinked trip records
            - linked_trips: Validated linked trip records
            - tours: Validated tour records

    Raises:
        DataValidationError: If pydantic validation fails on any table. Error
            message indicates which table, row, column, and constraint failed.

    Notes:
        - Pydantic handles validation automatically at model instantiation
        - This is the final quality checkpoint before data export
        - Custom validation logic can be added here if needed, but should
          ideally be implemented in Pydantic models for reusability
    """
    logger.info("Starting final validation checks")

    # Pydantic handles validation automatically when parsing/creating models
    # Just pass through the data - validation happens at model instantiation

    # This space *could* be used to implement any additional custom checks
    # that are not covered by the pydantic models, if needed in future. But
    # checks should ideally be implemented in the models themselves!

    logger.info("Final validation checks completed successfully")
    return {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_trips,
        "linked_trips": linked_trips,
        "tours": tours,
    }
