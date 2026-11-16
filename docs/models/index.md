# Data Models

Pydantic data models provide validation and type checking for survey data processing.

## Overview

Data models represent individual records (rows) and define:

- Required and optional fields
- Field validation rules and constraints
- Foreign key relationships between tables

Which steps need a field is not part of the model. Each step declares the columns it reads
and writes in its own `@step(requires=..., produces=...)`; see
[Column Requirements](../COLUMN_REQUIREMENTS.md) for the matrix built from them.

Models use Pydantic's `BaseModel` with custom field validators to ensure data quality throughout the processing pipeline.

## Key Features

### Field Validation
Each field includes validation rules:
```python
age: AgeCategory = schema_field()
home_lat: float = schema_field(ge=-90, le=90)
```

### Foreign Key Relationships
Models enforce referential integrity:
```python
hh_id: int = schema_field(
    ge=1,
    fk_to="households.hh_id",
    required_child=True,
)
```

### Pipeline Step Requirements
A step names the fields it needs, and the ones it creates:
```python
@step(
    requires={"persons": {"person_id", "age", "employment", "student", "school_type"}},
    produces={"tours": {"tour_id", "joint_tour_id"}},
)
def extract_tours(...): ...
```
A column whose name the config chooses is written as a pattern, such as `usable_<profile>`.

## Usage Example

```python
from data_canon.models.survey import PersonModel

person = PersonModel(
    person_id=1,
    hh_id=100,
    person_num=1,
    age=AgeCategory.AGE_35_TO_44,
    gender=Gender.FEMALE,
    employment=Employment.EMPLOYED_FULLTIME,
    student=Student.NONSTUDENT,
    # ... other fields
)
```

## Survey Data Models

Core data models used in the processing pipeline for households, persons, days, trips, and tours.

::: data_canon.models.survey.HouseholdModel

::: data_canon.models.survey.PersonModel

::: data_canon.models.survey.HouseholdDayModel

::: data_canon.models.survey.PersonDayModel

::: data_canon.models.survey.UnlinkedTripModel

::: data_canon.models.survey.LinkedTripModel

::: data_canon.models.survey.TourModel

::: data_canon.models.survey.JointTripModel

::: data_canon.models.survey.JointTourModel

::: data_canon.models.survey.HabitualLocationModel

::: data_canon.models.survey.HabitualLocationDayModel

::: data_canon.models.weighting.HouseholdWeightingModel

## Travel Model-formatted Data Models

### [DaySim Models](daysim.md)
Output file format models for the DaySim activity-based travel demand model.

### [CTRAMP Models](ctramp.md)
Output file format models for the CT-RAMP travel demand model.
