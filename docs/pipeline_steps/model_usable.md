# Model Usability

The single place the canonical completeness and model-admissibility flags are
computed. The module docstring below carries the flag definitions, the
derivation-order diagram, and the per-level rule table; the pipeline step
(`flag_model_usable`) is a thin wrapper that stamps them once so every
downstream consumer only reads the result.

::: processing.completeness
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - flag_model_usable
        - compute_model_usable
        - cascade_completeness
        - flag_household_day_complete
      filters:
        - "!^logger$"
        - "!^_"
