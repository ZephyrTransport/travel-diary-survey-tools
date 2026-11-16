"""Pipeline step entry point for imputation."""

import logging
from pathlib import Path
from typing import Any

import polars as pl

from pipeline.decoration import Tables, step
from processing.imputation.comparison import compare_imputation_methods
from processing.imputation.flags import stash_preimputed_columns

from .generic_impute import (
    _METHOD_ORDER,
    _group_configs_by_method,
    _process_imputation,
)

logger = logging.getLogger(__name__)

# The tables imputation may fill
_TABLES = ("households", "persons", "person_days", "unlinked_trips", "linked_trips", "tours")


def preimputed_columns(
    impute_columns: dict[str, list[dict[str, Any]]] | None = None,
    stash_preimputed: bool = True,
) -> dict[str, list[str]]:
    """The ``{column}_preimputed`` columns `imputation` writes, per table."""
    if not stash_preimputed or not impute_columns:
        return {}
    return {
        table: [f"{c}_preimputed" for cfg in configs for c in cfg.get("columns") or [cfg["column"]]]
        for table, configs in impute_columns.items()
    }


@step(
    # The columns to impute come from impute_columns, so the step checks them itself.
    optional={table: set() for table in _TABLES},
    produces={table: {"<column>_preimputed"} for table in _TABLES},
    generated_columns=preimputed_columns,
)
def imputation(
    tables: Tables,
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
        ``join_tables`` to a config block pulls columns from the tables its
        rows belong to via left-join on known foreign keys, which can
        significantly improve quality.

        Behaviour:

        1. Columns from the specified table(s) are joined on
           (e.g. ``persons`` ← ``households`` via ``hh_id``).
        2. For each target column, ``n_hh_{column}={code}`` features are
           auto-generated — how many of the person's *other* household
           members have each code.  This captures within-household
           correlation (e.g. siblings sharing race/ethnicity).
        3. Auto-generated ``n_hh_*`` columns are appended to
           ``numeric_features`` automatically.
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

    Counting Rows From Another Table (``aggregate_from``):
        The reverse of ``join_tables``: count the rows that belong to each
        record, such as a household's persons.  Useful when imputing
        household fields that depend on its composition (e.g. predicting
        household income from the employment/education mix of its members).

        For each source table and each field listed under ``pivot_count``, the
        module groups its rows by the foreign key and creates one column per
        unique value, counting occurrences.  Generated columns are named
        ``{source_table}_count_{field}={value}`` and are automatically added to
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
        tables: Any of the six core tables; ``impute_columns`` names those used.
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
            - join_tables: Tables to left-join for extra features.
            - aggregate_from: Pivot-count config for rows of other tables.

            **Random Forest** (``method: rf``):

            - column: Column name to impute.
            - missing_values: Enum labels to treat as missing.
            - n_estimators: Number of trees (default: 100).
            - max_depth: Maximum tree depth (default: None, unlimited).
            - numeric_features: Numeric feature columns.
            - categorical_features: Categorical feature columns.
            - join_tables: Tables to left-join for extra features.
            - aggregate_from: Pivot-count config for rows of other tables.

            **MICE** (``method: mice``):

            - columns: Column names to impute together.
            - missing_values: Dict mapping column → enum labels, or a
              single list applied to all columns.
            - max_iter: Maximum iterations (default: 10).
            - numeric_features: Numeric feature columns.
            - categorical_features: Categorical feature columns.
            - join_tables: Tables to left-join for extra features.
            - aggregate_from: Pivot-count config for rows of other tables.

            At least one of ``numeric_features`` or ``categorical_features``
            is required in every config block.

        stash_preimputed: Whether to create ``{column}_preimputed`` columns
            that preserve the original value before imputation (default: True).
            The stashed column retains sentinel codes (e.g. PNTA 999,
            MISSING 995) and nulls so downstream consumers can distinguish
            the reason a value was imputed.  To recover the old boolean flag:
            ``col_preimputed.is_null() & col.is_not_null()``.
        random_state: Random seed for reproducibility across all imputation.
        validate_imputation: Optional k-fold check of imputation accuracy against
            known values.

            Its keys:

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
    tables = dict(tables)

    if not tables:
        logger.warning("No tables provided for imputation")
        return {}

    logger.info("Starting imputation for tables: %s", list(tables.keys()))

    if not impute_columns:
        logger.warning("No impute_columns configs provided")
        return tables

    # Originals feed validation and the pre-imputation stash
    originals = {name: df.clone() for name, df in tables.items()}
    current_dfs, imputed_columns, validation_rows, importance_rows = _run_phases(
        tables, originals, impute_columns, validate_imputation, random_state
    )

    result_tables = {}
    for table_name in tables:
        current_df = current_dfs[table_name]
        if stash_preimputed and imputed_columns[table_name]:
            logger.info(
                "Stashing pre-imputation values for %s (%d columns)",
                table_name,
                len(imputed_columns[table_name]),
            )
            current_df = stash_preimputed_columns(
                current_df, originals[table_name], imputed_columns[table_name]
            )
        result_tables[table_name] = current_df

    result_tables.update(
        _reports(
            impute_columns,
            current_dfs,
            validation_rows,
            importance_rows,
            validate_imputation,
            random_state,
        )
    )

    logger.info("Imputation complete for %d tables", len(result_tables))
    return result_tables


def _run_phases(
    tables: dict[str, pl.DataFrame],
    originals: dict[str, pl.DataFrame],
    impute_columns: dict[str, list[dict[str, Any]]],
    validate_imputation: dict[str, Any] | None,
    random_state: int | None,
) -> tuple[
    dict[str, pl.DataFrame],
    dict[str, list[str]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    """Run each method's configs across all tables, in ``_METHOD_ORDER``.

    Returns:
        ``(imputed tables, imputed columns per table, validation rows, feature
        importance rows)``.
    """
    current_dfs = dict(tables)
    imputed_columns: dict[str, list[str]] = {name: [] for name in tables}
    validation_rows: list[dict[str, Any]] = []
    importance_rows: list[dict[str, Any]] = []

    method_configs = _group_configs_by_method(impute_columns)
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
            imputed_columns[table_name].extend(cols)
            validation_rows.extend(val_rows)
            importance_rows.extend(imp_rows)

    return current_dfs, imputed_columns, validation_rows, importance_rows


def _reports(
    impute_columns: dict[str, list[dict[str, Any]]],
    current_dfs: dict[str, pl.DataFrame],
    validation_rows: list[dict[str, Any]],
    importance_rows: list[dict[str, Any]],
    validate_imputation: dict[str, Any] | None,
    random_state: int | None,
) -> dict[str, pl.DataFrame]:
    """The ``_method_comparison`` or ``_validation_summary``, and ``_feature_importance``.

    Each is written to CSV beside ``validate_imputation.output_path`` when set.
    """
    reports: dict[str, pl.DataFrame] = {}
    output_path = validate_imputation.get("output_path") if validate_imputation else None

    # Either the full method comparison or the per-method validation summary
    if validate_imputation and validate_imputation.get("compare_methods", False):
        logger.info("Running head-to-head method comparison for all imputed columns")
        reports["_method_comparison"] = compare_imputation_methods(
            impute_columns,
            current_dfs,
            n_folds=validate_imputation.get("n_folds", 5),
            sample_pct=validate_imputation.get("sample_pct", 5.0),
            random_state=random_state,
            output_path=output_path,
        )
    elif validation_rows:
        summary_df = pl.DataFrame(validation_rows)
        if output_path:
            summary_df.write_csv(output_path)
            logger.info("Validation summary saved to %s", output_path)
        reports["_validation_summary"] = summary_df

    # Feature importance (RF only — collected during imputation)
    if importance_rows:
        fi_df = pl.DataFrame(importance_rows)
        if output_path:
            fi_path = Path(output_path).with_name(
                Path(output_path).stem + "_feature_importance.csv"
            )
            fi_df.write_csv(str(fi_path))
            logger.info("Feature importance saved to %s", fi_path)
        reports["_feature_importance"] = fi_df

    return reports
