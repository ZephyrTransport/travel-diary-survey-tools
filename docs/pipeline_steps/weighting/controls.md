# Controls

::: processing.weighting.controls
    options:
      show_root_heading: true
      show_docstring_description: true
      members: false

::: processing.weighting.controls.base
    options:
      show_root_heading: true
      members:
        - ControlLevel
        - ControlTarget

::: processing.weighting.controls.registry
    options:
      show_root_heading: true
      members:
        - CONTROLS
        - resolve_targets
        - pums_variables

::: processing.weighting.controls.enums
    options:
      show_root_heading: true
      members:
        - HHSizeCategory
        - HHWorkersCategory
        - HHVehiclesCategory
        - HHChildrenCategory
        - GenderCategory
        - EmploymentCategory
        - CommuteModeCategory
        - StudentCategory
        - TotalCategory

::: processing.weighting.controls.household
    options:
      show_root_heading: true
      members:
        - HHSizeControl
        - HHIncomeControl
        - HHWorkersControl
        - HHVehiclesControl
        - HHChildrenControl
        - HHTotalControl
        - HHIncomeBySizeControl

::: processing.weighting.controls.person
    options:
      show_root_heading: true
      members:
        - GenderControl
        - EmploymentControl
        - CommuteModeControl
        - StudentControl
        - EducationControl
        - RaceControl
        - EthnicityControl
        - AgeControl
        - PersonTotalControl
