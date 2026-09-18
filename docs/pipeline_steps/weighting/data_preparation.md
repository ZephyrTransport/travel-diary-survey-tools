# Data Preparation

::: processing.weighting.data_prep
    options:
      show_root_heading: true
      show_docstring_description: true
      members: false

::: processing.weighting.data_prep.crosswalk
    options:
      show_root_heading: true
      members:
        - PumaCrosswalk
        - TargetZoneConfig
        - GeographyConfig

::: processing.weighting.data_prep.census_geo
    options:
      show_root_heading: true
      members:
        - puma_vintage_for_pums_year
        - get_puma_gdf
        - get_block_gdf

::: processing.weighting.data_prep.pums_data
    options:
      show_root_heading: true
      members:
        - PUMSSource
        - fetch_pums_data
        - load_pums_from_files

::: processing.weighting.data_prep.control_data
    options:
      show_root_heading: true
      members:
        - ControlSpec
        - ControlTotals
        - recode_pums_households
        - recode_pums_persons
        - build_control_totals
        - apply_zone_groups

::: processing.weighting.data_prep.seed_data
    options:
      show_root_heading: true
      members:
        - recode_survey_households
        - recode_survey_persons
        - build_seed_table
