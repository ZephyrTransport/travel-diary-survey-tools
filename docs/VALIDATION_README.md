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
- `days`: `day_id` must be unique
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
- `days.person_id` → `persons.person_id`
- `days.hh_id` → `households.hh_id`
- `unlinked_trips.person_id` → `persons.person_id`
- `unlinked_trips.day_id` → `days.day_id`
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

The validation framework integrates seamlessly with the pipeline decorator:

```python
from pipeline.decoration import step

@step(validate=True)
def enrich_persons(
    households: pl.DataFrame,
    persons: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    # Inputs automatically validated before this runs
    persons_enriched = persons.join(
        households.select(["hh_id", "income"]),
        on="hh_id",
    )
    # Outputs automatically validated after return
    return {"persons": persons_enriched}
```

## Step-Aware Validation

Fields can be required only in specific pipeline steps using the `step_field()` helper:

```python
from data_canon.core.step_field import step_field

class PersonModel(BaseModel):
    person_id: int = step_field(ge=1)
    age: int | None = step_field(
        required_in_steps=["imputation"],
        ge=0, default=None
    )
    tour_id: int | None = step_field(
        created_in_step="extract_tours",
        default=None
    )
```

When validating, pass the step name:
```python
data.validate("persons", step="imputation")  # age is required
data.validate("persons", step="load")        # age is optional
```

## Configuration

### Adding Custom Constraints

Constraints are defined directly in the Pydantic models using the `step_field()` helper:

```python
# In src/data_canon/models.py
from data_canon.core.step_field import step_field

class MyCustomModel(BaseModel):
    # Unique constraint
    email: str = step_field(unique=True)

    # Foreign key
    person_id: int = step_field(
        ge=1,
        fk_to="persons.person_id"
    )

    # Required child (bidirectional FK)
    hh_id: int = step_field(
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

1. **Validate early and often** - Use `@step(validate=True)` on pipeline functions
2. **Use step-aware validation** - Mark fields with `required_in_steps` to validate progressively
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
