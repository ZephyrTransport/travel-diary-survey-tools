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

::: processing.weighting.diagnostics.tables
    options:
      show_root_heading: true
      members:
        - balancer_performance_table
        - weight_quality_table
        - unweighted_cell_counts
        - crosswalk_summary_table
