# Weight Hierarchy

The canonical weight hierarchy and the code that walks it.

::: processing.weighting.core.hierarchy
    options:
      show_root_heading: true
      members:
        - HIERARCHY
        - Level
        - Flow
        - Agg
        - MEMBER_COUNT_COL
        - WEIGHT_CONFIG_MAPPING
        - weight_col_for
        - seed_col_for
        - weight_columns_for
        - describe_weight_columns

::: processing.weighting.core.propagation
    options:
      show_root_heading: true
      members:
        - propagate_weights
        - seed_admits
        - distribute_within_scope
        - split_among_usable
        - drop_unsuffixed_weights
