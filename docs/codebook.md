# Enum Reference

The codebook modules define enumerated value labels and standardized coding schemes used throughout the survey processing pipeline.

## Overview

Codebook enumerations use the `LabeledEnum` pattern to provide both numeric codes and human-readable labels. These are used for:

- Data validation and type checking
- Consistent coding across different survey years
- Output formatting for travel demand models
- Documentation and data dictionaries

## Usage Example

```python
from data_canon.codebook.trips import Mode, Purpose

# Access code and label
mode_code = Mode.WALK_TRANSIT.value  # 11
mode_label = Mode.WALK_TRANSIT.label  # "Walk to transit"

# Validate and look up
purpose = Purpose(4)  # Purpose.SHOPPING_ERRANDS
print(purpose.label)  # "Appointment, shopping, or errands (e.g., gas)"
```

---
::: data_canon.codebook.generic

::: data_canon.codebook.households

::: data_canon.codebook.vehicles

::: data_canon.codebook.persons

::: data_canon.codebook.trips

::: data_canon.codebook.tours

::: data_canon.codebook.days

## Project/Format-specific

::: data_canon.codebook.daysim

::: data_canon.codebook.ctramp
