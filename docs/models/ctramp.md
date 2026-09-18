# CTRAMP Models

Pydantic data models for CT-RAMP (Coordinated Travel – Regional Activity Modeling Platform) output
file formats.  These models define the schema for the seven tables produced by the
[CT-RAMP formatting pipeline step](../pipeline_steps/format_output/ctramp.md).

The schema is based on the
[MTC Data Dictionary](https://github.com/BayAreaMetro/modeling-website/wiki/DataDictionary)
for Travel Model One.

## Purpose

Survey data formatted to this schema can be compared **directly** against CT-RAMP model output
tables to assess model accuracy.  Each model class below corresponds to one output CSV file:

| Model class | CT-RAMP wiki | Description |
|---|---|---|
| `HouseholdCTRAMPModel` | [Household](https://github.com/BayAreaMetro/modeling-website/wiki/Household) | Household attributes: TAZ, income, autos, joint tour frequency |
| `PersonCTRAMPModel` | [Person](https://github.com/BayAreaMetro/modeling-website/wiki/Person) | Person attributes: type, activity pattern, tour frequencies |
| `MandatoryLocationCTRAMPModel` | [MandatoryLocation](https://github.com/BayAreaMetro/modeling-website/wiki/MandatoryLocation) | Usual work and school TAZ per person |
| `IndividualTourCTRAMPModel` | [IndividualTour](https://github.com/BayAreaMetro/modeling-website/wiki/IndividualTour) | Individual (non-joint) tour records |
| `JointTourCTRAMPModel` | [JointTour](https://github.com/BayAreaMetro/modeling-website/wiki/JointTour) | Jointly-made tour records |
| `IndividualTripCTRAMPModel` | [IndividualTrip](https://github.com/BayAreaMetro/modeling-website/wiki/IndividualTrip) | Individual trip records (stops on individual tours) |
| `JointTripCTRAMPModel` | [JointTrip](https://github.com/BayAreaMetro/modeling-website/wiki/JointTrip) | Joint trip records (stops on joint tours) |

!!! note "Model-only fields"
    Several fields in these models are populated only when running the travel demand model itself
    (e.g. random-number seeds, `walk_subzone`, `auto_suff`, `value_of_time`).  When survey data is
    formatted to this schema these fields are `null`.  See the
    [CT-RAMP Formatting documentation](../pipeline_steps/format_output/ctramp.md#fields-not-derivable-from-survey-data)
    for the full list.

---

::: data_canon.models.ctramp
    options:
      show_root_heading: true
      members:
        - HouseholdCTRAMPModel
        - PersonCTRAMPModel
        - MandatoryLocationCTRAMPModel
        - IndividualTourCTRAMPModel
        - JointTourCTRAMPModel
        - IndividualTripCTRAMPModel
        - JointTripCTRAMPModel
