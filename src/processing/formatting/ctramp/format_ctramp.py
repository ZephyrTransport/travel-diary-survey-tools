"""CT-RAMP Formatting Step.

Transforms canonical survey data (persons, households, tours, trips) into
CT-RAMP model format. CT-RAMP (Coordinated Travel - Regional Activity Modeling
Platform) is an activity-based travel demand model requiring specific data
structures and coding schemes.

This module serves as the main orchestrator, delegating formatting of each
table type to specialized modules:
- format_persons: Person type classification, free parking, activity patterns
- format_households: Household income, vehicle counts, and composition
- format_tours_trips: Tours, trips, and mandatory locations
"""

import logging

import polars as pl

from data_canon.models.ctramp import (
    HouseholdCTRAMPModel,
    IndividualTourCTRAMPModel,
    IndividualTripCTRAMPModel,
    JointTourCTRAMPModel,
    JointTripCTRAMPModel,
    MandatoryLocationCTRAMPModel,
    PersonCTRAMPModel,
)
from pipeline.decoration import step

from .ctramp_config import CTRAMPConfig
from .format_households import format_households
from .format_mandatory_location import format_mandatory_location
from .format_persons import format_persons
from .format_tours import format_individual_tour, format_joint_tour
from .format_trips import format_individual_trip, format_joint_trip

logger = logging.getLogger(__name__)


MODEL_MAP = {
    "households_ctramp": HouseholdCTRAMPModel,
    "persons_ctramp": PersonCTRAMPModel,
    "mandatory_locations_ctramp": MandatoryLocationCTRAMPModel,
    "individual_trips_ctramp": IndividualTripCTRAMPModel,
    "individual_tours_ctramp": IndividualTourCTRAMPModel,
    "joint_trips_ctramp": JointTripCTRAMPModel,
    "joint_tours_ctramp": JointTourCTRAMPModel,
}


def _drop_missing_taz(
    households: pl.DataFrame,
    persons: pl.DataFrame,
    tours: pl.DataFrame,
    linked_trips: pl.DataFrame,
    joint_trips: pl.DataFrame,
    config: CTRAMPConfig,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Remove records with missing TAZ fields and ensure referential integrity.

    Performs cascading deletions to maintain data consistency:
    1. Remove households without valid home TAZ
    2. Remove persons from dropped households
    3. Remove tours/trips with missing origin or destination TAZ
    4. Remove tours that have no trips remaining
    5. Remove joint trips that have no linked trips remaining

    Args:
        households: Canonical household data
        persons: Canonical person data
        tours: Canonical tour data
        linked_trips: Canonical linked trip data
        joint_trips: Canonical joint trip data
        config: CTRAMP configuration with taz_field name

    Returns:
        Tuple of filtered DataFrames maintaining referential integrity
    """
    # Track original counts for logging
    counts = {
        "households": len(households),
        "persons": len(persons),
        "tours": max(0, len(tours)),
        "linked_trips": max(0, len(linked_trips)),
        "joint_trips": max(0, len(joint_trips)),
    }

    # Step 1: Filter households by home TAZ
    households = households.filter(
        pl.col(f"home_{config.taz_field}").is_not_null()
        & (pl.col(f"home_{config.taz_field}") != -1)
    )
    valid_hh_ids = households["hh_id"]

    # Step 2: Remove orphaned persons
    persons = persons.filter(pl.col("hh_id").is_in(valid_hh_ids))

    logger.info(
        "Dropped %d households without valid home TAZ (keeping %d households, %d persons)",
        counts["households"] - len(households),
        len(households),
        len(persons),
    )

    # Step 3: Filter tours by household and TAZ fields
    if len(tours) > 0:
        tours = tours.filter(
            pl.col("hh_id").is_in(valid_hh_ids)
            & pl.col(f"o_{config.taz_field}").is_not_null()
            & (pl.col(f"o_{config.taz_field}") != -1)
            & pl.col(f"d_{config.taz_field}").is_not_null()
            & (pl.col(f"d_{config.taz_field}") != -1)
        )
        valid_tour_ids = tours["tour_id"]
    else:
        valid_tour_ids = pl.Series("tour_id", [], dtype=pl.Int64)

    # Step 4: Filter linked trips by tour and TAZ fields
    if len(linked_trips) > 0:
        linked_trips = linked_trips.filter(
            pl.col("tour_id").is_in(valid_tour_ids)
            & pl.col(f"o_{config.taz_field}").is_not_null()
            & (pl.col(f"o_{config.taz_field}") != -1)
            & pl.col(f"d_{config.taz_field}").is_not_null()
            & (pl.col(f"d_{config.taz_field}") != -1)
        )
        # Get tours that still have trips
        tours_with_trips = linked_trips["tour_id"].unique()

        # Remove tours that lost all their trips
        if len(tours) > 0:
            tours_before = len(tours)
            tours = tours.filter(pl.col("tour_id").is_in(tours_with_trips))
            if tours_before != len(tours):
                logger.info(
                    "Removed %d tours that had no valid trips remaining",
                    tours_before - len(tours),
                )
    # No trips means no tours should remain
    elif len(tours) > 0:
        logger.info("Removed all %d tours because no valid trips exist", len(tours))
        tours = tours.head(0)

    # Step 5: Filter joint trips by linked trips and TAZ fields
    if len(joint_trips) > 0:
        # First filter by household
        joint_trips = joint_trips.filter(pl.col("hh_id").is_in(valid_hh_ids))

        # Filter by TAZ fields if they exist in joint_trips
        taz_cols = [f"o_{config.taz_field}", f"d_{config.taz_field}"]
        has_taz = all(col in joint_trips.columns for col in taz_cols)
        if has_taz:
            joint_trips = joint_trips.filter(
                pl.col(f"o_{config.taz_field}").is_not_null()
                & (pl.col(f"o_{config.taz_field}") != -1)
                & pl.col(f"d_{config.taz_field}").is_not_null()
                & (pl.col(f"d_{config.taz_field}") != -1)
            )

        # Keep only joint trips that have corresponding linked trips
        if len(linked_trips) > 0 and "joint_trip_id" in linked_trips.columns:
            valid_joint_trip_ids = linked_trips.filter(pl.col("joint_trip_id").is_not_null())[
                "joint_trip_id"
            ].unique()
            joint_trips = joint_trips.filter(pl.col("joint_trip_id").is_in(valid_joint_trip_ids))
        else:
            # No linked trips with joint_trip_id means no joint trips should remain
            logger.info(
                "Removed all %d joint trips because no valid linked trips exist", len(joint_trips)
            )
            joint_trips = joint_trips.head(0)

    # Final logging
    logger.info(
        "TAZ filtering complete:\n"
        "  Tours: %d → %d (dropped %d)\n"
        "  Linked trips: %d → %d (dropped %d)\n"
        "  Joint trips: %d → %d (dropped %d)",
        counts["tours"],
        len(tours),
        counts["tours"] - len(tours),
        counts["linked_trips"],
        len(linked_trips),
        counts["linked_trips"] - len(linked_trips),
        counts["joint_trips"],
        len(joint_trips),
        counts["joint_trips"] - len(joint_trips),
    )

    return households, persons, tours, linked_trips, joint_trips


def _drop_excess_fields(
    df: pl.DataFrame,
    model_cls: type,
) -> pl.DataFrame:
    """Drop columns from df that are not in the data model class.

    Args:
        df: Input DataFrame
        model_cls: Data model class with defined fields
    Returns:
        DataFrame with only columns defined in the model class
    valid_fields = set(model_cls.__fields__.keys())
    cols_to_drop = [col for col in df.columns if col not in valid_fields]
    return df.drop(cols_to_drop)
    """
    valid_fields = set(model_cls.__fields__.keys())
    cols_to_drop = set(df.columns) - valid_fields
    return df.drop(cols_to_drop)


@step()
def format_ctramp(
    persons: pl.DataFrame,
    households: pl.DataFrame,
    linked_trips: pl.DataFrame,
    tours: pl.DataFrame,
    joint_trips: pl.DataFrame,
    income_low_threshold: int,
    income_med_threshold: int,
    income_high_threshold: int,
    income_base_year_dollars: int,
    taz_field: str = "taz",
    drop_missing_taz: bool = True,
) -> dict[str, pl.DataFrame]:
    """Format canonical survey data to CT-RAMP model specification.

    Transforms person, household, tour, and trip data from canonical format to
    CT-RAMP format required by the activity-based travel demand model. This
    includes:
    - Person type classification based on age, employment, and student status
    - Free parking eligibility determination
    - Household income conversion to CT-RAMP brackets
    - Vehicle counts (human-driven and autonomous)
    - TAZ and walk-to-transit subzone mapping
    - Mandatory locations (work/school)
    - Individual and joint tours
    - Individual and joint trips

    Args:
        persons: Canonical person data with demographic fields
        households: Canonical household data with income and dwelling fields
        linked_trips: Canonical linked trip data (required)
        tours: Canonical tour data (required)
        joint_trips: Aggregated joint trip data (required)
        income_low_threshold: Income threshold for low-income bracket
        income_med_threshold: Income threshold for medium-income bracket
        income_high_threshold: Income threshold for high-income bracket
        income_base_year_dollars: Base year for income adjustment
        taz_field: The field that contains the TAZ ID for CTRAMP formatting.
        drop_missing_taz: If True, remove households without valid TAZ IDs

    Returns:
        Dictionary with keys:
        - households_ctramp: Formatted household data
        - persons_ctramp: Formatted person data
        - mandatory_location_ctramp: Mandatory location data
        - individual_tour_ctramp: Individual tour data
        - individual_trip_ctramp: Individual trip data
        - joint_tour_ctramp: Joint tour data
        - joint_trip_ctramp: Joint trip data

    Example:
        >>> result = format_ctramp(persons, households, linked_trips, tours)
        >>> households_ctramp = result["households_ctramp"]
        >>> persons_ctramp = result["persons_ctramp"]
        >>> tours_ctramp = result["individual_tour_ctramp"]
    """
    # Validate configuration parameters
    config = CTRAMPConfig(
        income_low_threshold=income_low_threshold,
        income_med_threshold=income_med_threshold,
        income_high_threshold=income_high_threshold,
        income_base_year_dollars=income_base_year_dollars,
        drop_missing_taz=drop_missing_taz,
        taz_field=taz_field,
    )
    logger.info("Starting CT-RAMP formatting")

    # Ensure TAZ columns are Int64 for filtering
    households = households.with_columns(pl.col(f"home_{config.taz_field}").cast(pl.Int64))

    # Drop any households that do not have a TAZ assigned
    if config.drop_missing_taz:
        (
            households,
            persons,
            tours,
            linked_trips,
            joint_trips,
        ) = _drop_missing_taz(households, persons, tours, linked_trips, joint_trips, config)

    # Format each table
    households_ctramp = format_households(households, persons, tours, config)

    # Format tours - use empty DataFrame with proper schema if no tours exist
    if len(tours) == 0:
        individual_tours_ctramp = pl.DataFrame(
            schema={
                "person_id": pl.Int64,
                "tour_purpose": pl.String,
            }
        )
    else:
        individual_tours_ctramp = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=linked_trips,
            persons_canonical=persons,
            households_ctramp=households_ctramp,
            config=config,
        )

    # Format persons with tour statistics (works with empty or populated tours)
    persons_ctramp = format_persons(
        persons_canonical=persons,
        tours_ctramp=individual_tours_ctramp,
        config=config,
    )

    # Format mandatory locations - uses formatted persons and households
    mandatory_location_ctramp = format_mandatory_location(
        persons_ctramp=persons_ctramp,
        households_ctramp=households_ctramp,
        linked_trips_canonical=linked_trips,
        config=config,
    )

    # Add formatted tours to results
    joint_tours_ctramp = format_joint_tour(
        tours_canonical=tours,
        linked_trips_canonical=linked_trips,
        persons_canonical=persons,
        households_ctramp=households_ctramp,
        config=config,
    )

    individual_trips_ctramp = format_individual_trip(
        linked_trips_canonical=linked_trips,
        tours_ctramp=individual_tours_ctramp,
        persons_canonical=persons,
        households_ctramp=households_ctramp,
        config=config,
    )

    joint_trips_ctramp = format_joint_trip(
        joint_trips_canonical=joint_trips,
        linked_trips_canonical=linked_trips,
        tours_canonical=tours,
        households_ctramp=households_ctramp,
        config=config,
    )

    logger.info("CT-RAMP formatting complete")

    # Prepare result dictionary and clean up temporary columns
    tables = {
        "households_ctramp": households_ctramp,
        "persons_ctramp": persons_ctramp,
        "individual_trips_ctramp": individual_trips_ctramp,
        "individual_tours_ctramp": individual_tours_ctramp,
        "joint_trips_ctramp": joint_trips_ctramp,
        "joint_tours_ctramp": joint_tours_ctramp,
        "mandatory_locations_ctramp": mandatory_location_ctramp,
    }

    # Cleanup tables
    for table_name, df in tables.items():
        model_cls = MODEL_MAP[table_name]
        tables[table_name] = _drop_excess_fields(df, model_cls)

    return tables
