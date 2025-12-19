"""Final validation step for the entire dataset."""

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
    """Run validation checks on the entire dataset.

    Args:
        households: The households dataframe
        persons: The persons dataframe
        days: The days dataframe
        unlinked_trips: The unlinked trips dataframe
        linked_trips: The linked trips dataframe
        tours: The tours dataframe

    Returns:
        The validated dataset

    Raises:
        DataValidationError: If pydantic validation fails
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
