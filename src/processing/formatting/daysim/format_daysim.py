"""DaySim Formatting Step.

Transforms canonical survey data (persons, households, trips, tours, days) into
DaySim activity-based travel demand model format, applying model-specific coding
schemes and data structures. See [DaySim Data Models](../../models/daysim.md).

This module orchestrates specialized formatting for each table type, applying
DaySim-specific integer codes for categorical variables while maintaining
referential integrity across tables.

# Components

* [`format_persons`][processing.formatting.daysim.format_persons]: Produces persons_daysim
  table consistent with [`PersonDaysimModel`][data_canon.models.daysim.PersonDaysimModel].
* [`format_households`][processing.formatting.daysim.format_households]: Produces
  households_daysim table consistent with
  [`HouseholdDaysimModel`][data_canon.models.daysim.HouseholdDaysimModel].
* [`format_linked_trips`][processing.formatting.daysim.format_trips]: Produces
  trips_daysim table consistent with
  [`LinkedTripDaysimModel`][data_canon.models.daysim.LinkedTripDaysimModel].
* [`format_tours`][processing.formatting.daysim.format_tours]: Produces tours_daysim table
  consistent with [`TourDaysimModel`][data_canon.models.daysim.TourDaysimModel].
* [`format_days`][processing.formatting.daysim.format_days]: Produces days_daysim table
  consistent with [`PersonDayDaysimModel`][data_canon.models.daysim.PersonDayDaysimModel].

# Data Quality Filters

- **Partial Tours**: Optionally drop tours without return home
- **Missing TAZ**: Remove records without spatial assignment (required for model)
- **Invalid Tours**: Filter out tours failing validation rules (zero distance,
  negative duration, data quality flags)

# Implementation Notes

- DaySim requires specific integer codes for categorical variables
- Formatting maintains referential integrity across tables
- TAZ (Traffic Analysis Zone) assignment critical for model application
- Person type classification affects downstream choice model applicability
- Mode/purpose hierarchies ensure consistent coding
- Output validates against DaySim data specifications
- Days with invalid/partial tours become "no travel" days in the model
"""

import logging

import polars as pl

from data_canon.codebook.tours import TourCategory
from pipeline.decoration import step
from processing.completeness import suggest_usability_columns

from .format_days import format_days
from .format_households import format_households
from .format_persons import format_persons
from .format_tours import format_tours
from .format_trips import format_linked_trips

logger = logging.getLogger(__name__)


@step(
    requires={
        "households": {"residence_rent_own", "residence_type"},
        "persons": {"person_num", "work_park", "work_mode", "is_proxy"},
        "days": {"travel_dow"},
        "unlinked_trips": {"tour_id"},
        "linked_trips": {
            "tour_id",
            "tour_direction",
            "driver",
            "access_mode",
            "egress_mode",
        },
        "tours": {"origin_linked_trip_id", "dest_linked_trip_id"},
    },
)
def format_daysim(
    persons: pl.DataFrame,
    households: pl.DataFrame,
    unlinked_trips: pl.DataFrame,
    linked_trips: pl.DataFrame,
    tours: pl.DataFrame,
    days: pl.DataFrame,
    usability_flag_col: str,
    drop_partial_tours: bool = True,
    drop_missing_taz: bool = True,
    drop_invalid_tours: bool = True,
) -> dict[str, pl.DataFrame]:
    """Format canonical survey data to DaySim model specification.

    Converts canonical survey tables to DaySim activity-based travel demand model
    format. See module docstring for complete component descriptions.

    Args:
        persons: Person attributes in canonical format. Required columns:
            person_id, hh_id, age, employment, student, etc.
        households: Household attributes in canonical format. Required columns:
            hh_id, home_taz, income, etc.
        unlinked_trips: Individual trip segments with mode, purpose, and timing.
        linked_trips: Journey records with coordinates, mode, purpose, and timing.
        tours: Tour records with purpose, timing, and location fields.
        days: Person-day records for completeness calculation.
        drop_partial_tours: If True, remove tours not marked as complete
            (default: True). Tours without return home are excluded.
        drop_missing_taz: If True, remove households without valid TAZ/MAZ IDs
            (default: True). Required for model application.
        usability_flag_col: Which usability profile decides the tour universe.
            Required: with several profiles stamped there is no defensible
            default, and naming a different one from the CT-RAMP formatter or
            the weighting means those outputs describe different universes.
        drop_invalid_tours: If True, remove tours marked as invalid
            (default: True). Filters out zero distance, negative duration, and
            data quality flagged tours.

    Returns:
        Dictionary containing:
            - households_daysim: Formatted household data with person type
              composition and income categories
            - persons_daysim: Formatted person data with person type, day pattern,
              and completeness flags
            - days_daysim: Formatted day-level data with summaries
            - linked_trips_daysim: Formatted trip data with DaySim mode, path type,
              and driver/passenger codes
            - tours_daysim: Formatted tour data with DaySim purpose codes and
              timing

    !!! Example

        ```python

        result = format_daysim(
            persons=canonical_persons,
            households=canonical_households,
            unlinked_trips=canonical_unlinked_trips,
            linked_trips=canonical_linked_trips,
            tours=canonical_tours,
            days=canonical_days,
            drop_partial_tours=True,
            drop_missing_taz=True,
            drop_invalid_tours=True
        )
        households_daysim = result["households_daysim"]
        persons_daysim = result["persons_daysim"]
        ```
    """
    logger.info("Starting DaySim formatting")

    # Drop invalid tours if specified
    if drop_invalid_tours:
        n_og_tours = len(tours)
        n_og_trips = len(linked_trips)
        # The gate stamped by cascade_completeness is the only criterion, so
        # DaySim, CT-RAMP and the weighting agree on the tour universe by
        # construction rather than by three places implementing one rule.
        if usability_flag_col not in tours.columns:
            msg = (
                f"Tours carry no '{usability_flag_col}' column, so there is nothing "
                f"to gate on. Declare it in cascade_completeness's "
                f"usability_profiles, or set drop_invalid_tours: false to keep "
                f"every tour. {suggest_usability_columns(tours)}"
            )
            raise ValueError(msg)

        logger.info("DaySim tour universe gated on %s", usability_flag_col)
        # A null means the cascade never reached this row -- a broken frame, not
        # licence to guess a criterion of our own.
        keep = pl.col(usability_flag_col).fill_null(value=False)
        tours = tours.filter(keep)
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
