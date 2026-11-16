# Data Validation Framework

A comprehensive, layered validation framework for travel survey data that ensures data quality through multiple validation stages.

## Overview

The validation framework provides 5 layers of validation:

1. **Column Constraints** - Uniqueness checks on key columns
2. **Foreign Key Constraints** - Relational integrity between tables
3. **Row Validation** - Pydantic model validation for types and business rules
4. **Custom Validators** - User-defined validation logic

## Quick Start

```python
from data_canon.core.dataclass import CanonicalData
import polars as pl

# Create canonical data structure
data = CanonicalData()

# Load your data
data.households = pl.read_csv("households.csv")
data.persons = pl.read_csv("persons.csv")
data.unlinked_trips = pl.read_csv("trips.csv")

# Validate tables (optionally specify pipeline step)
data.validate("households")
data.validate("persons", step="link_trips")
data.validate("unlinked_trips", step="link_trips")
```

## Validation Layers

### 1. Column Constraints

Automatically checks uniqueness on primary key columns.

**Built-in constraints:**
- `households`: `hh_id` must be unique
- `persons`: `person_id` must be unique
- `person_days`: `person_day_id` must be unique
- `unlinked_trips`: `trip_id` must be unique
- `linked_trips`: `linked_trip_id` must be unique
- `tours`: `tour_id` must be unique

**Example:**
```python
# This will pass
data.households = pl.DataFrame({
    "hh_id": [1, 2, 3],  # All unique
    "home_taz": [100, 200, 300],
    "income": [50000, 75000, 100000],
    "hh_size": [2, 3, 4],
    "num_vehicles": [1, 2, 2],
})
data.validate("households")  # ✓ Success

# This will fail
data.households = pl.DataFrame({
    "hh_id": [1, 2, 2],  # Duplicate ID!
    "home_taz": [100, 200, 300],
    # ...
})
data.validate("households")  # ✗ DataDataValidationError: Duplicate hh_id values
```

### 2. Foreign Key Constraints

Ensures referential integrity between related tables.

**Built-in FK relationships:**
- `persons.hh_id` → `households.hh_id`
- `person_days.person_id` → `persons.person_id`
- `person_days.hh_id` → `households.hh_id`
- `unlinked_trips.person_id` → `persons.person_id`
- `unlinked_trips.person_day_id` → `person_days.person_day_id`
- `linked_trips.person_id` → `persons.person_id`
- `linked_trips.tour_id` → `tours.tour_id`
- `tours.person_id` → `persons.person_id`

**Example:**
```python
data.households = pl.DataFrame({
    "hh_id": [1, 2, 3],
    # ...
})

# This will pass
data.persons = pl.DataFrame({
    "person_id": [101, 102, 103],
    "hh_id": [1, 2, 3],  # All reference valid households
    # ...
})
data.validate("persons")  # ✓ Success

# This will fail
data.persons = pl.DataFrame({
    "person_id": [101, 102, 103],
    "hh_id": [1, 2, 999],  # 999 doesn't exist!
    # ...
})
data.validate("persons")  # ✗ DataValidationError: Orphaned FK
```

**Graceful handling:**
- Skips validation if parent table is `None`
- Skips validation if FK column doesn't exist yet (for forward references)

### 3. Row Validation

Uses Pydantic models to validate data types, enums, and business logic for each row.

**Built-in models:**
- `HouseholdModel`: Household attributes
- `PersonModel`: Person demographics
- `PersonDayModel`: Daily travel records
- `UnlinkedTripModel`: Individual trip segments
- `LinkedTripModel`: Connected trip chains
- `TourModel`: Complete tour structures

**Example:**
```python
# This will pass
data.persons = pl.DataFrame({
    "person_id": [101, 102],
    "hh_id": [1, 2],
    "age": [35, 42],  # Valid ages
    "gender": ["male", "female"],  # Valid enum values
    "worker": [True, True],  # Boolean
    "student": [False, False],
})
data.validate("persons")  # ✓ Success

# This will fail
data.persons = pl.DataFrame({
    "person_id": [101, 102],
    "hh_id": [1, 2],
    "age": [-5, 200],  # Invalid ages
    "gender": ["male", "alien"],  # Invalid enum
    "worker": [True, "maybe"],  # Wrong type
    "student": [False, False],
})
data.validate("persons")  # ✗ DataValidationError: Type/enum violations
```

### 4. Custom Validators

User-defined validation functions for business logic that spans rows or tables.

**How to add custom checks:**

1. Define your check function in `src/data_canon/validation/custom.py`:
```python
def check_arrival_after_departure(unlinked_trips: pl.DataFrame) -> list[str]:
    """Ensure arrive_time is after depart_time for all trips."""
    errors = []
    bad_trips = unlinked_trips.filter(
        pl.col("arrive_time") < pl.col("depart_time")
    )
    if len(bad_trips) > 0:
        trip_ids = bad_trips["trip_id"].to_list()[:5]
        errors.append(
            f"Found {len(bad_trips)} trips where arrive_time < depart_time. "
            f"Sample trip IDs: {trip_ids}"
        )
    return errors
```

2. Register it in the `CUSTOM_VALIDATORS` dictionary in `custom.py`:
```python
# src/data_canon/validation/custom.py
CUSTOM_VALIDATORS = {
    "unlinked_trips": [check_arrival_after_departure],
    "linked_trips": [],
}
```

3. The check automatically runs when validating that table:
```python
data.validate("unlinked_trips")  # Runs check_arrival_after_departure
```

**Multi-table validator:**
```python
# In validation/custom.py
def check_household_size_consistency(
    persons: pl.DataFrame,
    households: pl.DataFrame,
) -> list[str]:
    """Check that hh_size matches actual person count."""
    errors = []

    actual_sizes = persons.group_by("hh_id").agg(pl.len().alias("actual"))
    merged = households.join(actual_sizes, on="hh_id", how="left")
    mismatches = merged.filter(pl.col("hh_size") != pl.col("actual"))

    if len(mismatches) > 0:
        ids = mismatches["hh_id"].to_list()
        errors.append(
            f"Household size mismatch for hh_ids: {ids[:5]}"
            f"{' ...' if len(ids) > 5 else ''}"
        )

    return errors

# Register in CUSTOM_VALIDATORS
CUSTOM_VALIDATORS = {
    "persons": [check_household_size_consistency],
}

data.validate("persons")  # Uses both persons and households
```

Custom validators automatically receive any tables they need from the CanonicalData instance based on their function signature.

See `src/data_canon/validation/custom.py` for implementation examples.

### 5. Required Children (Bidirectional FK)

Ensures parent records have required child records.

**Built-in requirements:**
- Every `household` must have at least one `person`
- Every `person` must have at least one `day`

**Example:**
```python
data.households = pl.DataFrame({
    "hh_id": [1, 2, 3],
    # ...
})

# This will pass
data.persons = pl.DataFrame({
    "person_id": [101, 102, 103],
    "hh_id": [1, 2, 3],  # All households have a person
    # ...
})
data.validate("households")  # ✓ Success

# This will fail
data.persons = pl.DataFrame({
    "person_id": [101, 102],
    "hh_id": [1, 2],  # hh_id=3 has no persons!
    # ...
})
data.validate("households")  # ✗ DataValidationError: Missing required children
```

## Pipeline Integration

The pipeline decorator validates a step's canonical tables for it:

```python
from pipeline.decoration import step

@step(
    validate_input=True,    # check inputs before the step runs (the default)
    validate_output=False,  # check outputs after it returns (off by default)
    requires={"persons": {"person_id", "hh_id"}, "households": {"hh_id", "income"}},
    produces={"persons": {"income"}},
)
def enrich_persons(
    households: pl.DataFrame,
    persons: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    persons_enriched = persons.join(
        households.select(["hh_id", "income"]),
        on="hh_id",
    )
    return {"persons": persons_enriched}
```

A step's config entry can turn either check on or off (`validate_input: false`,
`validate_output: true`). A flag the config leaves out keeps the decorator's value.

## Step-Aware Validation

A model field says what a value may be; the step that reads it says when it must
exist. Each step lists the fields it reads (`requires`) and the ones it creates or
updates (`produces`), table by table:

```python
@step(
    requires={"linked_trips": {"person_day_id", "o_purpose_category", "d_purpose_category"}},
    produces={"tours": {"tour_id", "joint_tour_id"}},
)
def extract_tours(...): ...
```

Validating for a step requires that step's fields and type-checks any other field
that is present:

```python
data.validate("persons", step="compute_weights")  # gender, age, ... must be present
data.validate("persons", step="load_data")        # nothing required; present fields still checked
```

A column whose name comes from the config is written as a pattern, such as
`usable_<profile>` or `home_<zone>`. Validation skips patterns, since the real name
is only known at run time, and the step checks for the column itself.

The step also lists the real names, with `generated_columns`: a function in the step's own
module that takes some of the step's settings and returns the columns per table.
The decorator registers them on every call, so `write_data` keeps them:

```python
def zone_columns(zone_geographies: list[dict]) -> dict[str, list[str]]:
    return {"households": [f"home_{z['zone_name']}" for z in zone_geographies]}

@step(produces={"households": {"home_<zone>"}}, generated_columns=zone_columns)
def add_zone_ids(zone_geographies: list[dict], households: pl.DataFrame, ...): ...
```

Every step in `processing` declares a contract, except the few listed with their
reason in `tests/test_step_validation.py` (`load_data`, `write_data`, `final_check`).
The [Column Requirements](COLUMN_REQUIREMENTS.md) page is generated from these
declarations by `scripts/generate_column_matrix.py`.

## Configuration

### Adding Custom Constraints

Constraints are defined directly in the Pydantic models using the `schema_field()` helper:

```python
# In src/data_canon/models/survey.py
from data_canon.core.schema_field import schema_field

class MyCustomModel(BaseModel):
    # Unique constraint
    email: str = schema_field(unique=True)

    # Foreign key
    person_id: int = schema_field(
        ge=1,
        fk_to="persons.person_id"
    )

    # Required child (bidirectional FK)
    hh_id: int = schema_field(
        ge=1,
        fk_to="households.hh_id",
        required_child=True
    )
```

## Error Handling

All validation errors raise `DataValidationError` with structured information:

```python
from data_canon.core.exceptions import DataValidationError

try:
    data.validate("households")
except DataValidationError as e:
    print(f"Table: {e.table}")        # Which table failed
    print(f"Rule: {e.rule}")          # Which validation rule
    print(f"Message: {e.message}")    # Error details
    print(f"Column: {e.column}")      # Column involved (if applicable)
    print(f"Row ID: {e.row_id}")      # Row identifier (if applicable)
```

## Best Practices

1. **Validate early and often** - Keep `validate_input` on, and turn on `validate_output` where a step's output matters
2. **Declare what each step reads** - List it in `@step(requires=...)`, so a missing column fails before the step runs
3. **Add custom validators** in `src/data_canon/validation/custom.py` for business logic
4. **Return empty list for success** - Custom validators should return `[]` when passing
5. **Provide informative messages** - Include context and sample data in error messages
6. **Use multi-table validators** - Validators automatically receive needed tables from CanonicalData
7. **Final validation step** - Use `final_check` step at pipeline end to validate all tables

## Testing

Run the validation test suite:

```bash
pytest tests/test_validation.py -v
```
