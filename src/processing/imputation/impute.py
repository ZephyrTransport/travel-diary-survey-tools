"""Pipeline step entry point for imputation."""

import logging
from pathlib import Path
from typing import Any

import polars as pl

from pipeline.decoration import step
from processing.imputation.comparison import compare_imputation_methods
from processing.imputation.flags import stash_preimputed_columns

from .generic_impute import (
    _METHOD_ORDER,
    _group_configs_by_method,
    _process_imputation,
)

logger = logging.getLogger(__name__)


@step()
def imputation(  # noqa: C901, PLR0912, PLR0915
    # Optional canonical tables
    households: pl.DataFrame | None = None,
    persons: pl.DataFrame | None = None,
    days: pl.DataFrame | None = None,
    unlinked_trips: pl.DataFrame | None = None,
    linked_trips: pl.DataFrame | None = None,
    tours: pl.DataFrame | None = None,
    # Config parameters
    impute_columns: dict[str, list[dict[str, Any]]] | None = None,
    stash_preimputed: bool = True,
    random_state: int | None = None,
    validate_imputation: dict[str, Any] | None = None,
) -> dict[str, pl.DataFrame]:
    """Impute missing values using KNN, Random Forest, and/or MICE methods.

    Each config block specifies its ``method`` (``knn``, ``rf``, or ``mice``)
    along with the method-specific parameters.  Configs are grouped by method
    and executed in a fixed order (KNN → RF → MICE) across all tables so that
    later phases can benefit from values filled in earlier phases.

    Handling Missing Values with Enum Labels:
        Survey data often uses special codes for missing values (e.g. 995 for
        "Missing Response", 999 for "Prefer not to answer").  Use **enum
        member names** (labels) rather than raw numeric values in the config::

            missing_values: [MISSING, PNTA]   # enum labels, not 995/999

        The module automatically:

        1. Maps the table name to the appropriate codebook module
           (e.g. ``households`` → ``data_canon.codebook.households``).
        2. Finds the enum class whose ``canonical_field_name`` matches the
           target column (e.g. ``income_broad`` → ``IncomeBroad``).
        3. Resolves enum member names to their values
           (e.g. ``MISSING`` → 995).
        4. Replaces those values with null before imputation.

        For MICE with multiple columns, ``missing_values`` can be a dict
        mapping each column to its own labels, or a single list applied to
        all columns::

            # Per-column
            missing_values:
              race: [MISSING]
              ethnicity: [MISSING, PNTA]

            # Shared
            missing_values: [MISSING, PNTA]   # applied to all columns

    Cross-Table Features (``join_tables``):
        By default only features from the same table are used.  Adding
        ``join_tables`` to a config block pulls columns from parent tables
        via left-join on known foreign keys, which can significantly improve
        quality.

        Behaviour:

        1. Columns from the specified parent table(s) are joined onto the
           child table (e.g. ``persons`` ← ``households`` via ``hh_id``).
        2. For each target column a ``hh_mode_{column}`` feature is
           auto-generated — the mode of that column among *other* household
           members (exclude-self).  This captures within-household
           correlation (e.g. siblings sharing race/ethnicity).
        3. Auto-generated ``hh_mode_*`` columns are appended to
           ``categorical_features`` automatically.
        4. After imputation all joined/aggregated columns are stripped;
           the output schema is unchanged.

        Example::

            impute_columns:
              persons:
                - method: knn
                  column: gender
                  n_neighbors: 5
                  join_tables: [households]
                  categorical_features: [age, employment, income_bin, residence_type]
                  #                                       ^^^^^^^^^^  ^^^^^^^^^^^^^^
                  #                              columns from the households table

    Child-to-Parent Aggregation (``aggregate_from``):
        The reverse of ``join_tables``: aggregate child rows up to a parent
        table.  Useful when imputing parent-level fields that depend on
        household composition (e.g. predicting household income from the
        employment/education mix of its members).

        For each child table and each field listed under ``pivot_count``, the
        module groups child rows by the parent's FK and creates one column per
        unique value, counting occurrences.  Generated columns are named
        ``{child_table}_count_{field}={value}`` and are automatically added to
        ``numeric_features``.  After imputation, all generated columns are
        stripped.

        Example::

            impute_columns:
              households:
                - method: mice
                  columns: [income_bin]
                  aggregate_from:
                    persons:
                      pivot_count: [employment, education, student]
                  categorical_features: [residence_type, residence_rent_own]
                  max_iter: 10

    Args:
        households: Households table (optional).
        persons: Persons table (optional).
        days: Days table (optional).
        unlinked_trips: Unlinked trips table (optional).
        linked_trips: Linked trips table (optional).
        tours: Tours table (optional).
        impute_columns: Dict mapping table names to list of imputation configs.
            Every config dict **must** include a ``method`` key (``knn``, ``rf``,
            or ``mice``).  The remaining keys are method-specific:

            **KNN** (``method: knn``):

            - column: Column name to impute.
            - missing_values: Enum labels to treat as missing.
            - n_neighbors: Number of neighbors (default: 5).
            - neighbor_weights: ``'uniform'`` or ``'distance'``
              (default: ``'distance'``).
            - numeric_features: Numeric feature columns.
            - categorical_features: Categorical feature columns.
            - join_tables: Parent tables to left-join for extra features.
            - aggregate_from: Child-to-parent pivot-count config.

            **Random Forest** (``method: rf``):

            - column: Column name to impute.
            - missing_values: Enum labels to treat as missing.
            - n_estimators: Number of trees (default: 100).
            - max_depth: Maximum tree depth (default: None, unlimited).
            - numeric_features: Numeric feature columns.
            - categorical_features: Categorical feature columns.
            - join_tables: Parent tables to left-join for extra features.
            - aggregate_from: Child-to-parent pivot-count config.

            **MICE** (``method: mice``):

            - columns: Column names to impute together.
            - missing_values: Dict mapping column → enum labels, or a
              single list applied to all columns.
            - max_iter: Maximum iterations (default: 10).
            - numeric_features: Numeric feature columns.
            - categorical_features: Categorical feature columns.
            - join_tables: Parent tables to left-join for extra features.
            - aggregate_from: Child-to-parent pivot-count config.

            At least one of ``numeric_features`` or ``categorical_features``
            is required in every config block.

        stash_preimputed: Whether to create ``{column}_preimputed`` columns
            that preserve the original value before imputation (default: True).
            The stashed column retains sentinel codes (e.g. PNTA 999,
            MISSING 995) and nulls so downstream consumers can distinguish
            the reason a value was imputed.  To recover the old boolean flag:
            ``col_preimputed.is_null() & col.is_not_null()``.
        random_state: Random seed for reproducibility across all imputation.
        validate_imputation: Optional validation config with keys:

            - enabled: Whether to run validation (default: False).
            - n_folds: Number of k-folds (default: 5).
            - sample_pct: Percentage of non-missing values to test
              (default: 5.0).
            - output_path: Path to save validation or comparison CSV.
            - compare_methods: When True, run all three methods (KNN, RF,
              MICE) against every column instead of validating only the
              configured method (default: False).

    Returns:
        Dictionary of imputed tables.  When validation is enabled, an extra
        key ``_validation_summary`` contains a Polars DataFrame with columns:
        table, variable, method, type, n_samples, n_folds, accuracy,
        precision, recall, f1, rmse, mae, r2.

        When ``compare_methods`` is True, an extra key
        ``_method_comparison`` contains a Polars DataFrame comparing
        KNN, RF, and MICE for every imputed column.

    Example config:
        .. code-block:: yaml

            impute_columns:
              households:
                - method: knn
                  column: income_broad
                  missing_values: [MISSING, PNTA]
                  n_neighbors: 5
                  neighbor_weights: distance
                  numeric_features: [num_persons, num_vehicles, num_workers]
              persons:
                - method: knn
                  column: gender
                  missing_values: [MISSING]
                  n_neighbors: 5
                  join_tables: [households]
                  numeric_features: [age]
                  categorical_features: [relationship, employment, income_bin]
                - method: rf
                  column: education
                  missing_values: [MISSING]
                  n_estimators: 200
                  max_depth: 15
                  numeric_features: [age]
                  categorical_features: [employment, occupation]
                - method: mice
                  columns: [race, ethnicity]
                  missing_values:
                    race: [MISSING]
                    ethnicity: [MISSING, PNTA]
                  join_tables: [households]
                  max_iter: 10
                  numeric_features: [age]
            random_state: 42
            stash_preimputed: true
            validate_imputation:
              enabled: true
              n_folds: 5
              sample_pct: 5.0
    """
    # Collect input tables
    tables = {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_trips,
        "linked_trips": linked_trips,
        "tours": tours,
    }

    # Remove None tables
    tables = {name: df for name, df in tables.items() if df is not None}

    if not tables:
        logger.warning("No tables provided for imputation")
        return {}

    logger.info("Starting imputation for tables: %s", list(tables.keys()))

    if not impute_columns:
        logger.warning("No impute_columns configs provided")
        return dict(tables.items())

    # Clone all tables and track originals for validation/preimputed stash
    originals = {name: df.clone() for name, df in tables.items()}
    current_dfs = dict(tables.items())
    all_imputed_columns: dict[str, list[str]] = {name: [] for name in tables}
    all_validation_results: list[dict[str, Any]] = []
    all_importance_rows: list[dict[str, Any]] = []

    # Group configs by method (KNN → RF → MICE)
    method_configs = _group_configs_by_method(impute_columns)

    # Process each method phase across all tables
    for phase_num, method in enumerate(_METHOD_ORDER, 1):
        configs_by_table = method_configs[method]
        if not configs_by_table:
            continue

        logger.info("Phase %d: %s imputation", phase_num, method.upper())
        for table_name, configs in configs_by_table.items():
            if table_name not in current_dfs:
                logger.warning(
                    "Table '%s' referenced in impute_columns but not provided", table_name
                )
                continue
            logger.info("  %s imputation for %s", method.upper(), table_name)
            current_dfs[table_name], cols, val_rows, imp_rows = _process_imputation(
                current_dfs[table_name],
                originals[table_name],
                table_name,
                configs,
                method,
                validate_imputation,
                random_state,
                tables=current_dfs,
            )
            all_imputed_columns[table_name].extend(cols)
            all_validation_results.extend(val_rows)
            all_importance_rows.extend(imp_rows)

    # Stash pre-imputation values for all tables
    result_tables = {}
    for table_name in tables:
        current_df = current_dfs[table_name]
        imputed_columns = all_imputed_columns[table_name]

        if stash_preimputed and imputed_columns:
            logger.info(
                "Stashing pre-imputation values for %s (%d columns)",
                table_name,
                len(imputed_columns),
            )
            current_df = stash_preimputed_columns(
                current_df, originals[table_name], imputed_columns
            )

        result_tables[table_name] = current_df

    # Run either per-method validation summary or full method comparison
    if validate_imputation and validate_imputation.get("compare_methods", False):
        logger.info("Running head-to-head method comparison for all imputed columns")
        comparison_df = compare_imputation_methods(
            impute_columns,
            current_dfs,
            n_folds=validate_imputation.get("n_folds", 5),
            sample_pct=validate_imputation.get("sample_pct", 5.0),
            random_state=random_state,
            output_path=validate_imputation.get("output_path"),
        )
        result_tables["_method_comparison"] = comparison_df
    elif all_validation_results:
        summary_df = pl.DataFrame(all_validation_results)
        output_path = validate_imputation.get("output_path") if validate_imputation else None
        if output_path:
            summary_df.write_csv(output_path)
            logger.info("Validation summary saved to %s", output_path)
        result_tables["_validation_summary"] = summary_df

    # Feature importance (RF only — collected during imputation)
    if all_importance_rows:
        fi_df = pl.DataFrame(all_importance_rows)
        output_path = validate_imputation.get("output_path") if validate_imputation else None
        if output_path:
            fi_path = Path(output_path).with_name(
                Path(output_path).stem + "_feature_importance.csv"
            )
            fi_df.write_csv(str(fi_path))
            logger.info("Feature importance saved to %s", fi_path)
        result_tables["_feature_importance"] = fi_df

    logger.info("Imputation complete for %d tables", len(result_tables))
    return result_tables
