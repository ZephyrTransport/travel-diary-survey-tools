# CT-RAMP Formatting

::: processing.formatting.ctramp.format_ctramp
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - format_ctramp
      filters:
        - "!^logger$"
        - "!^_"

::: processing.formatting.ctramp.ctramp_config
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - CTRAMPConfig
      filters:
        - "!^logger$"
        - "!^_"

::: processing.formatting.ctramp.format_households
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - format_households
      filters:
        - "!^logger$"
        - "!^_"

::: processing.formatting.ctramp.format_persons
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - format_persons
      filters:
        - "!^logger$"
        - "!^_"

::: processing.formatting.ctramp.format_mandatory_location
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - format_mandatory_location
      filters:
        - "!^logger$"
        - "!^_"

::: processing.formatting.ctramp.format_tours
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - format_individual_tour
        - format_joint_tour
      filters:
        - "!^logger$"
        - "!^_"

::: processing.formatting.ctramp.format_trips
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - format_individual_trip
        - format_joint_trip
      filters:
        - "!^logger$"
        - "!^_"
