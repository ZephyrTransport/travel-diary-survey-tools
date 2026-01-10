"""DaySim Formatting Step.

Transforms canonical survey data (persons, households, trips, tours) into
DaySim model format. DaySim is an activity-based travel demand model
requiring specific data structures and coding schemes.

This module serves as the main orchestrator, delegating formatting of each
table type to specialized modules:
- format_persons: Person type classification and day completeness
- format_households: Household composition and income processing
- format_trips: Linked trip mode, path type, and driver/passenger codes
- format_tours: Tour purpose, timing, and location mapping
"""

import logging

import polars as pl

from data_canon.codebook.tours import TourCategory, TourDataQuality
from pipeline.decoration import step

from .format_days import format_days
from .format_households import format_households
from .format_persons import format_persons
from .format_tours import format_tours
from .format_trips import format_linked_trips

logger = logging.getLogger(__name__)


@step()
def format_daysim(  # noqa: PLR0913
    persons: pl.DataFrame,
    households: pl.DataFrame,
    unlinked_trips: pl.DataFrame,
    linked_trips: pl.DataFrame,
    tours: pl.DataFrame,
    days: pl.DataFrame,
    drop_partial_tours: bool = True,
    drop_missing_taz: bool = True,
    drop_invalid_tours: bool = True,
) -> dict[str, pl.DataFrame]:
    """Format canonical survey data to DaySim model specification.

    Transforms person, household, trip, and tour data from canonical format to
    DaySim format required by the activity-based travel demand model. This
    includes:
    - Person type classification based on age, employment, and student status
    - Household composition calculation from person data
    - Trip mode, path type, and driver/passenger code derivation
    - Tour purpose, timing, and location mapping
    - Day completeness computation for survey weighting (optional)

    Args:
        persons: Canonical person data with demographic and location fields
        households: Canonical household data with income and dwelling fields
        unlinked_trips: Canonical unlinked trip data with mode, purpose, and
            timing fields
        linked_trips: Canonical linked trip data with mode, purpose, and
            timing fields
        tours: Canonical tour data with purpose, timing, and location fields
        days: Day-level data for completeness calculation
        drop_partial_tours: If True, remove tours not marked as complete
        drop_missing_taz: If True, remove households without valid TAZ/MAZ IDs
        drop_invalid_tours: If True, remove tours marked as invalid

    Returns:
        Dictionary with keys:
        - households_daysim: Formatted household data
        - persons_daysim: Formatted person data
        - trips_daysim: Formatted trip data
        - tours_daysim: Formatted tour data
    """
    logger.info("Starting DaySim formatting")

    # Drop invalid tours if specified
    if drop_invalid_tours:
        n_og_tours = len(tours)
        n_og_trips = len(linked_trips)
        tours = tours.filter(pl.col("tour_data_quality") == TourDataQuality.VALID.value)
        linked_trips = linked_trips.filter(pl.col("tour_id").is_in(tours["tour_id"].implode()))

        # NOTE: We keep all days even if their tours are invalid
        # Days with invalid tours become "no travel" days in the model

        logger.info(
            "Dropped %d invalid tours with %d linked trips; "
            "%d tours remain and %d linked trips remain",
            n_og_tours - len(tours),
            n_og_trips - len(linked_trips),
            len(tours),
            len(linked_trips),
        )

    # Drop partial/incomplete tours if specified
    if drop_partial_tours:
        n_og_tours = len(tours)
        n_og_trips = len(linked_trips)
        tours = tours.filter(pl.col("tour_category") == TourCategory.COMPLETE.value)
        linked_trips = linked_trips.filter(pl.col("tour_id").is_in(tours["tour_id"].implode()))
        # NOTE: We keep all days even if their tours are partial/incomplete
        # Days with partial tours become "no travel" days in the model
        logger.info(
            "Dropped %d partial tours with %d linked trips; "
            "%d tours remain and %d linked trips remain",
            n_og_tours - len(tours),
            n_og_trips - len(linked_trips),
            len(tours),
            len(linked_trips),
        )

    # Drop any households that do not have a MAZ/TAZ assigned
    if drop_missing_taz:
        n_og_households = len(households)
        n_og_persons = len(persons)
        n_og_linked_trips = len(linked_trips)
        n_og_tours = len(tours)

        households = households.filter(
            households["home_taz"].is_not_null() & (households["home_taz"] != -1)
        )
        persons = persons.filter(pl.col("hh_id").is_in(households["hh_id"].implode()))
        days = days.filter(pl.col("hh_id").is_in(households["hh_id"].implode()))
        linked_trips = linked_trips.filter(pl.col("hh_id").is_in(households["hh_id"].implode()))
        tours = tours.filter(pl.col("hh_id").is_in(households["hh_id"].implode()))
        logger.info(
            "Dropped %d households without TAZ/MAZ with "
            "%d persons, %d linked trips, and %d tours; "
            "%d households, %d persons, %d linked trips, and %d tours remain",
            n_og_households - len(households),
            n_og_persons - len(persons),
            n_og_linked_trips - len(linked_trips),
            n_og_tours - len(tours),
            len(households),
            len(persons),
            len(linked_trips),
            len(tours),
        )

    # Format each table

    # Format persons, includes day for completeness computation
    persons_daysim = format_persons(persons, days)

    # Format households, requires the daysim formatted person types
    households_daysim = format_households(households, persons_daysim)

    # Format days
    days_daysim = format_days(persons, days, tours)

    # Format linked trips
    linked_trips_daysim = format_linked_trips(persons, unlinked_trips, linked_trips)

    # Format tours
    tours_daysim = format_tours(persons, days, linked_trips, tours)

    logger.info("DaySim formatting complete")

    return {
        "households_daysim": households_daysim,
        "persons_daysim": persons_daysim,
        "days_daysim": days_daysim,
        "linked_trips_daysim": linked_trips_daysim,
        "tours_daysim": tours_daysim,
    }
