# Completeness & Model Usability

The single place the canonical completeness and model-admissibility flags are
computed. The module docstring below carries the flag definitions, the
derivation-order diagram, and the per-level rule table; the pipeline step
(`cascade_completeness`) is a thin, parameterless wrapper that stamps them once
so every downstream consumer only reads the result.

Two flags, computed side by side:

- `complete` — survey reporting completeness (vendor-provided at the day level,
  possibly adjusted by the project cleaner), cascaded through
  household ↔ person ↔ day ↔ trip/tour so it is internally consistent.
- `model_usable` — the subset of `complete` admissible to the tour-based
  models: VALID tour structure, COMPLETE home-to-home category, and
  household-day coherence.

Downstream consumers choose which flag to honour: weighting via
`usability_flag_col`, the CT-RAMP/DaySim formatters via `drop_invalid_tours`.

::: processing.completeness
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - cascade_completeness
        - compute_model_usable
        - rollup_completeness
        - flag_household_day_complete
      filters:
        - "!^logger$"
        - "!^_"
