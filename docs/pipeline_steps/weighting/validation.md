# Validation

::: processing.weighting.validation
    options:
      show_root_heading: true
      show_docstring_description: true
      members: false

::: processing.weighting.validation.checksums
    options:
      show_root_heading: true
      members:
        - check_recode_nulls
        - check_incidence_sums

::: processing.weighting.validation.weight_checks
    options:
      show_root_heading: true
      members:
        - weight_sanity_checks

::: processing.weighting.validation.coverage
    options:
      show_root_heading: true
      members:
        - check_control_geography_coverage
