"""CT-RAMP Formatting Step.

Transforms canonical survey data (persons, households, tours, trips) into
CT-RAMP (Coordinated Travel - Regional Activity Modeling Platform) format for
use with activity-based travel demand models. See [CTRAMP Data Models](../../models/ctramp.md).

This module orchestrates the transformation of households, persons, tours, and
trips from canonical format into seven CT-RAMP tables, intelligently handling
missing data and providing configurable income thresholds and filtering options.

# Components

* [`format_households`][processing.formatting.ctramp.format_households]: Produces households
  table consistent with [`HouseholdCTRAMPModel`][data_canon.models.ctramp.HouseholdCTRAMPModel].
* [`format_persons`][processing.formatting.ctramp.format_persons]: Produces persons
  table consistent with [`PersonCTRAMPModel`][data_canon.models.ctramp.PersonCTRAMPModel].
* [`format_mandatory_location`][processing.formatting.ctramp.format_mandatory_location]:
  Produces mandatory locations ctramp table with work and school location records for persons
  with usual work/school locations, linking person characteristics with destination TAZ.  This
  table is consistent with
  [`MandatoryLocationCTRAMPModel`][data_canon.models.ctramp.MandatoryLocationCTRAMPModel].
* [`format_tours`][processing.formatting.ctramp.format_tours]: Produces individual and joint
  tours tables, consistent with
  [`IndividualTourCTRAMPModel`][data_canon.models.ctramp.IndividualTourCTRAMPModel] and
  [`JointTourCTRAMPModel`][data_canon.models.ctramp.JointTourCTRAMPModel].
* [`format_trips`][processing.formatting.ctramp.format_trips]: Produces individual and joint trips
  tables consistent with
  [`IndividualTripCTRAMPModel`][data_canon.models.ctramp.IndividualTripCTRAMPModel] and
  [`JointTripCTRAMPModel`][data_canon.models.ctramp.JointTripCTRAMPModel].

# Data Flow

## Key Dependencies

1. households_ctramp must be created first (needed by all downstream formatters)
2. individual_tours_ctramp must be created before persons_ctramp (provides tour
   statistics)
3. persons_ctramp and trips use already-formatted tour/household data
4. Joint tables can be processed independently from individual tables

## Mermaid Diagram

```mermaid
flowchart TD
    %% Canonical inputs
    subgraph inputs["Canonical Inputs"]
        direction TB
        hh_canon[("households")]
        per_canon[("persons")]
        tours_canon[("tours")]
        trips_canon[("linked_trips")]
        joint_trips_canon[("joint_trips")]
    end

    %% Stage 1
    subgraph s1["Stage 1: Households"]
        direction LR
        fmt_hh["format_households"]
        hh_ctramp{{"households_ctramp"}}
        fmt_hh --> hh_ctramp
    end

    %% Stage 2
    subgraph s2["Stage 2: Tours"]
        direction TB
        fmt_ind_tour["format_individual_tour"]
        ind_tour_ctramp{{"individual_tours_ctramp"}}
        fmt_ind_tour --> ind_tour_ctramp

        fmt_joint_tour["format_joint_tour"]
        joint_tour_ctramp{{"joint_tours_ctramp"}}
        fmt_joint_tour --> joint_tour_ctramp
    end

    %% Stage 3
    subgraph s3["Stage 3: Persons"]
        direction LR
        fmt_persons["format_persons"]
        per_ctramp{{"persons_ctramp"}}
        fmt_persons --> per_ctramp
    end

    %% Stage 4
    subgraph s4["Stage 4: Locations & Trips"]
        direction TB
        fmt_mand_loc["format_mandatory_location"]
        mand_loc_ctramp{{"mandatory_locations_ctramp"}}
        fmt_mand_loc --> mand_loc_ctramp

        fmt_ind_trip["format_individual_trip"]
        ind_trip_ctramp{{"individual_trips_ctramp"}}
        fmt_ind_trip --> ind_trip_ctramp

        fmt_joint_trip["format_joint_trip"]
        joint_trip_ctramp{{"joint_trips_ctramp"}}
        fmt_joint_trip --> joint_trip_ctramp
    end

    %% Canonical inputs to stages (consolidated)
    inputs -.-> s1
    inputs -.-> s2
    inputs -.-> s3
    inputs -.-> s4

    %% Stage ordering (vertical layout)
    s1 ~~~ s2
    s2 ~~~ s3
    s3 ~~~ s4

    %% Formatted table dependencies (thick solid)
    hh_ctramp ==> fmt_ind_tour
    hh_ctramp ==> fmt_joint_tour
    ind_tour_ctramp ==> fmt_persons
    hh_ctramp ==> fmt_mand_loc
    hh_ctramp ==> fmt_ind_trip
    ind_tour_ctramp ==> fmt_ind_trip
    hh_ctramp ==> fmt_joint_trip

    %% Styling
    classDef canonClass fill:#e1f5ff,stroke:#0277bd,stroke-width:2px
    classDef formatterClass fill:#fff3e0,stroke:#ef6c00,stroke-width:2px
    classDef outputClass fill:#e8f5e9,stroke:#2e7d32,stroke-width:3px

    class hh_canon,per_canon,tours_canon,trips_canon,joint_trips_canon canonClass
    class fmt_hh,fmt_ind_tour,fmt_persons,fmt_mand_loc,fmt_ind_trip formatterClass
    class fmt_joint_tour,fmt_joint_trip formatterClass
    class hh_ctramp,per_ctramp,ind_tour_ctramp,ind_trip_ctramp outputClass
    class mand_loc_ctramp,joint_tour_ctramp,joint_trip_ctramp outputClass
```

**Legend:**

- 🔵 **Blue cylinders**: Canonical input tables
- 🟠 **Orange boxes**: Formatter functions
- 🟢 **Green hexagons**: CT-RAMP output tables (all formatted)
- **Dashed arrows (⋯→)**: Canonical inputs available to formatters
  (see component details above for specifics)
- **Thick solid arrows (⟹)**: Formatted table dependencies
  (execution order)

**Note:** Canonical input details for each formatter are listed in the
[Components](#components) section above.

## Execution Stages

1. TAZ Filtering (optional): Households without valid home_taz are removed,
   cascading to persons, tours, and trips
2. Households: Formatted first to establish income brackets needed downstream
3. Tours: Formatted before persons to provide tour frequency statistics
4. Persons: Combines tour-derived fields (activity patterns, frequencies) with
   demographic characteristics
5. Mandatory Locations: Uses both canonical (TAZ) and formatted (income)
   household data
6. Trips: Formatted last, linking to already-formatted tours

## Configuration

All thresholds and defaults are managed via
[`CTRAMPConfig`][processing.formatting.ctramp.ctramp_config.CTRAMPConfig]

## Implementation Notes

### Excluded Fields

- Random Number Fields: Excluded because they are simulation-specific and not
  derivable from survey data (household: `ao_rn`, `fp_rn`, `cdap_rn`; tour: `imtf_rn`,
  `imtod_rn`, `immc_rn`, `jtf_rn`, `jtl_rn`, `jtod_rn`, etc.)
- Model Output Fields: `auto_suff` (auto sufficiency), `walk_subzone`
  (walk-to-transit accessibility), wait times and logsums

### Placeholder Values

When tour data is unavailable or incomplete, person-level tour frequency fields
use these defaults:

- `activity_pattern`: 'H' (home all day)
- `imf_choice`: 0 (no mandatory tours)
- `inmf_choice`: 1 (minimum valid code)
- `wfh_choice`: 0 (no work from home)
- `jtf_choice`: Derived from joint tour data if available; otherwise NONE_NONE (-4)

### Empty Data Handling

The module gracefully handles missing data:

- Empty tour DataFrames result in placeholder person fields
- Missing joint tour IDs default to individual tour treatment
- Missing or null values use sensible defaults per field type

"""

import logging

import polars as pl

from data_canon.codebook.ctramp import CTRAMPEmploymentCategory
from data_canon.models.ctramp import (
    AOResultsCTRAMPModel,
    CDAPResultsCTRAMPModel,
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
from .filters import _drop_invalid_tours, _drop_missing_taz
from .format_ao import format_ao_results
from .format_cdap import format_cdap_results
from .format_households import format_households
from .format_mandatory_location import format_mandatory_location
from .format_persons import enrich_persons_with_person_type, format_persons
from .format_tours import format_individual_tour, format_joint_tour
from .format_trips import format_individual_trip, format_joint_trip
from .mappings import EMPLOYMENT_TO_CTRAMP, ctramp_student_category_expression

logger = logging.getLogger(__name__)


MODEL_MAP = {
    "households_ctramp": HouseholdCTRAMPModel,
    "persons_ctramp": PersonCTRAMPModel,
    "mandatory_locations_ctramp": MandatoryLocationCTRAMPModel,
    "individual_trips_ctramp": IndividualTripCTRAMPModel,
    "individual_tours_ctramp": IndividualTourCTRAMPModel,
    "joint_trips_ctramp": JointTripCTRAMPModel,
    "joint_tours_ctramp": JointTourCTRAMPModel,
    "cdap_results_ctramp": CDAPResultsCTRAMPModel,
    "ao_results_ctramp": AOResultsCTRAMPModel,
}


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
    """
    valid_fields = set(model_cls.model_fields.keys())
    cols_to_drop = set(df.columns) - valid_fields
    return df.drop(cols_to_drop)


@step(
    requires={
        "persons": {
            "person_num",
            "gender",
            "job_type",
            "commute_subsidy_use_3",
            "commute_subsidy_use_4",
        },
        "linked_trips": {
            "o_purpose",
            "d_purpose",
            "access_mode",
            "egress_mode",
        },
        "tours": {"num_travelers"},
    },
)
def format_ctramp(  # noqa: PLR0913
    persons: pl.DataFrame,
    households: pl.DataFrame,
    linked_trips: pl.DataFrame,
    tours: pl.DataFrame,
    joint_trips: pl.DataFrame,
    income_low_threshold: int,
    income_med_threshold: int,
    income_high_threshold: int,
    income_base_year_dollars: int,
    joint_tours: pl.DataFrame | None = None,
    taz_field: str = "taz",
    drop_missing_taz: bool = True,
    drop_invalid_tours: bool = True,
) -> dict[str, pl.DataFrame]:
    """Format canonical survey data to CT-RAMP model specification.

    Transforms person, household, tour, and trip data from canonical format to
    CT-RAMP format required by the activity-based travel demand model. See module
    docstring for complete component descriptions and data flow.

    Args:
        persons: Canonical person data with demographic fields. Required columns:
            person_id, hh_id, person_num, age, gender, employment, student,
            school_type, commute subsidies.
        households: Canonical household data with income and dwelling fields.
            Required columns: hh_id, home_taz, income_detailed, income_followup,
            num_vehicles.
        linked_trips: Canonical linked trip data. Required columns: linked_trip_id,
            tour_id, o_purpose_category, d_purpose_category, mode_type, o_taz,
            d_taz, tour_direction, times, person_id, hh_id.
        tours: Canonical tour data. Required columns: tour_id, hh_id, person_id,
            person_num, tour_category, tour_purpose, o_taz, d_taz, times,
            tour_mode, joint_tour_id, parent_tour_id.
        joint_tours: Aggregated joint tour data carrying ``joint_tour_weight``.
            Optional; when absent the joint tour output simply carries no weight.
        joint_trips: Aggregated joint trip data. Required columns: joint_trip_id,
            hh_id, num_joint_travelers.
        income_low_threshold: Dollar value dividing low from medium income bracket.
            Must be less than income_med_threshold.
        income_med_threshold: Dollar value dividing medium from high income bracket.
            Must be between income_low_threshold and income_high_threshold.
        income_high_threshold: Dollar value dividing high from very high income
            bracket. Must be greater than income_med_threshold.
        income_base_year_dollars: Target year for income conversion (e.g., 2000,
            2010). Categorical income values are converted to midpoint dollars in
            this base year.
        taz_field: Field name containing the TAZ ID for CTRAMP formatting
            (default: "taz").
        drop_missing_taz: If True, remove households without valid TAZ IDs. This
            cascades to persons, tours, and trips (default: True).
        drop_invalid_tours: If True, remove tours that are not VALID (single-trip,
            loop, missing-anchor, change-mode, indeterminate) or not COMPLETE (do
            not start and end at home), mirroring the DaySim formatter (which drops
            both). Cascades to linked and joint trips (default: True).

    Returns:
        Dictionary with keys:
            - households_ctramp: Formatted household data (7 core fields including
              income, TAZ, size)
            - persons_ctramp: Formatted person data (20+ fields including person
              type, activity patterns, tour frequencies)
            - mandatory_locations_ctramp: Mandatory location data (work/school
              location records)
            - individual_tours_ctramp: Individual tour data (tour-level attributes:
              purpose, mode, time, stops)
            - individual_trips_ctramp: Individual trip data (trip-level attributes
              for individual tours)
            - joint_tours_ctramp: Joint tour data (joint tours with composition and
              participants)
            - joint_trips_ctramp: Joint trip data (trip-level attributes for joint
              tours)

    !!! Example

        ```python
        from processing.formatting.ctramp import format_ctramp

        result = format_ctramp(
            persons=canonical_persons,
            households=canonical_households,
            linked_trips=canonical_linked_trips,
            tours=canonical_tours,
            joint_trips=canonical_joint_trips,
            income_low_threshold=60000,          # $60k divides low from medium
            income_med_threshold=150000,         # $150k divides medium from high
            income_high_threshold=250000,        # $250k divides high from very high
            income_base_year_dollars=2000,       # Convert income to $2000
            drop_missing_taz=True                # Remove households without TAZ
        )

        # Access formatted tables
        households_ctramp = result["households_ctramp"]
        persons_ctramp = result["persons_ctramp"]
        mandatory_locations_ctramp = result["mandatory_locations_ctramp"]
        individual_tours_ctramp = result["individual_tours_ctramp"]
        joint_tours_ctramp = result["joint_tours_ctramp"]
        individual_trips_ctramp = result["individual_trips_ctramp"]
        joint_trips_ctramp = result["joint_trips_ctramp"]
        ```
    """
    # Validate configuration parameters
    config = CTRAMPConfig(
        income_low_threshold=income_low_threshold,
        income_med_threshold=income_med_threshold,
        income_high_threshold=income_high_threshold,
        income_base_year_dollars=income_base_year_dollars,
        drop_missing_taz=drop_missing_taz,
        drop_invalid_tours=drop_invalid_tours,
        taz_field=taz_field,
    )
    logger.info("Starting CT-RAMP formatting")

    # Drop invalid/partial tours before anything else so CT-RAMP and DaySim
    # outputs contain the same set of tours (and no null-purpose leakage).
    if config.drop_invalid_tours:
        tours, linked_trips, joint_trips = _drop_invalid_tours(tours, linked_trips, joint_trips)

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

    # Format each table ----------------------------------------------------
    # Format households first since it has no derived field dependencies
    households_ctramp = format_households(households, persons, tours, config)

    # Derive/validate person_type and type for use in tour/trip formatting
    # Pre-compute student_category and employment_category so person_type
    # expression can use them for consistent classification
    if "student_category" not in persons.columns:
        persons = persons.with_columns(
            ctramp_student_category_expression(school_taz_col=f"school_{config.taz_field}").alias(
                "student_category"
            )
        )
    if "employment_category" not in persons.columns:
        persons = persons.with_columns(
            pl.col("employment")
            .replace_strict(
                EMPLOYMENT_TO_CTRAMP,
                default=CTRAMPEmploymentCategory.NOT_EMPLOYED.value,
            )
            .alias("employment_category")
        )
    persons_with_type = enrich_persons_with_person_type(persons)

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
            persons_canonical=persons_with_type,
            households_ctramp=households_ctramp,
            config=config,
        )

    # Format persons with tour statistics (works with empty or populated tours)
    persons_ctramp = format_persons(
        persons_canonical=persons_with_type,
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
        joint_tours_canonical=joint_tours,
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

    # Derive cdapResults / aoResults from the formatted persons/households before
    # the cleanup loop trims each table to its own model.
    tables["cdap_results_ctramp"] = format_cdap_results(tables["persons_ctramp"])
    tables["ao_results_ctramp"] = format_ao_results(tables["households_ctramp"])

    # Cleanup tables
    for table_name, df in tables.items():
        model_cls = MODEL_MAP[table_name]
        tables[table_name] = _drop_excess_fields(df, model_cls)

    return tables
