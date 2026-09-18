# Diagnostics

::: processing.weighting.diagnostics
    options:
      show_root_heading: true
      show_docstring_description: true
      members: false

::: processing.weighting.diagnostics.report
    options:
      show_root_heading: true
      members:
        - generate_report

::: processing.weighting.diagnostics.charts
    options:
      show_root_heading: true
      members:
        - fit_diverging_figure
        - violins_figure
        - ef_tradeoff_figure
        - crosswalk_figure

::: processing.weighting.diagnostics.data
    options:
      show_root_heading: true
      members:
        - category_label_map
        - apply_fit_merges
        - zone_fit_summary
        - compute_weighted_totals
        - fit_table
        - profile_summary
        - weight_cascade
        - redistribution
        - split_identity

::: processing.weighting.diagnostics.comparison
    options:
      show_root_heading: true
      members:
        - WeightSet
        - Comparison
        - PairStats
        - Decile
        - Inheritance
        - fitted_weight_sets
        - reference_weight_sets
        - compare_pair
        - all_pairs
        - inheritance
        - payload

::: processing.weighting.diagnostics.glossary
    options:
      show_root_heading: true
      members:
        - Term
        - term
        - glossary_groups

::: processing.weighting.diagnostics.tables
    options:
      show_root_heading: true
      members:
        - profile_comparison_table
        - inheritance_table
        - balancer_performance_table
        - weight_quality_table
        - cascade_table
        - redistribution_table
        - split_identity_table
        - coverage_table
        - unweighted_cell_counts
        - crosswalk_summary_table
