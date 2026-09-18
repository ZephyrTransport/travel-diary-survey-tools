# Data Models

Pydantic data models provide validation and type checking for survey data processing.

## Overview

Data models represent individual records (rows) and define:

- Required and optional fields
- Field validation rules and constraints
- Foreign key relationships between tables
- Pipeline step requirements

Models use Pydantic's `BaseModel` with custom field validators to ensure data quality throughout the processing pipeline.

## Key Features

### Field Validation
Each field includes validation rules:
```python
age: AgeCategory = step_field(required_in_steps=["extract_tours"])
home_lat: float = step_field(ge=-90, le=90, required_in_steps=["extract_tours"])
```

### Foreign Key Relationships
Models enforce referential integrity:
```python
hh_id: int = step_field(
    ge=1,
    fk_to="households.hh_id",
    required_child=True,
)
```

### Pipeline Step Requirements
Fields specify which processing steps require them:
```python
person_num: int = step_field(ge=1, required_in_steps=["format_ctramp", "format_daysim"])
```

## Usage Example

```python
from data_canon.models.survey import PersonModel

person = PersonModel(
    person_id=1,
    hh_id=100,
    person_num=1,
    age=AgeCategory.AGE_35_64,
    gender=Gender.FEMALE,
    employment=Employment.FULL_TIME,
    student=Student.NOT_STUDENT,
    # ... other fields
)
```

## Survey Data Models

Core data models used in the processing pipeline for households, persons, days, trips, and tours.

::: data_canon.models.survey.HouseholdModel

::: data_canon.models.survey.PersonModel

::: data_canon.models.survey.PersonDayModel

::: data_canon.models.survey.UnlinkedTripModel

::: data_canon.models.survey.LinkedTripModel

::: data_canon.models.survey.TourModel

::: data_canon.models.survey.JointTripModel

## Travel Model-formatted Data Models

### [DaySim Models](daysim.md)
Output file format models for the DaySim activity-based travel demand model.

### [CTRAMP Models](ctramp.md)
Output file format models for the CT-RAMP travel demand model.
