"""Shared utilities for imputation methods.

Data type handling:

* **Numeric columns**: imputed directly by KNN / RF / MICE.
* **Categorical integer columns** (e.g. enum codes 1-6): automatically
  encoded to dense 0..N codes before imputation and decoded back
  afterwards.  This prevents non-contiguous codes (e.g. 1, 2, 3, 995,
  999) from distorting distance calculations.
* **Categorical string columns** (e.g. ``"Hispanic"``, ``"White"``):
  automatically encoded to integer codes for MICE, then decoded to
  original labels after imputation.  No manual pre-processing required.

Feature selection guidance:

* ``numeric_features``: used as-is (continuous values — e.g.
  ``num_trips``, ``num_vehicles``).
* ``categorical_features``: one-hot encoded into binary columns for
  distance / regression calculations.

Tip: use ``numeric_features`` for ordinal or count variables and
``categorical_features`` for unordered enums.  Putting high-cardinality
integers (e.g. raw age) in ``categorical_features`` causes feature
explosion and slow performance.
"""

import logging
from typing import Any

import numpy as np
import polars as pl

from utils.enum_helpers import resolve_enum_labels

logger = logging.getLogger(__name__)

# Foreign keys between canonical tables: (table, the table it belongs to) -> key.
FK_RELATIONSHIPS: dict[tuple[str, str], str] = {
    ("persons", "households"): "hh_id",
    ("person_days", "persons"): "person_id",
    ("person_days", "households"): "hh_id",
    ("unlinked_trips", "person_days"): "person_day_id",
    ("unlinked_trips", "persons"): "person_id",
    ("unlinked_trips", "households"): "hh_id",
    ("linked_trips", "person_days"): "person_day_id",
    ("linked_trips", "persons"): "person_id",
    ("linked_trips", "households"): "hh_id",
    ("tours", "persons"): "person_id",
    ("tours", "households"): "hh_id",
}

# Hyperparameter defaults per method, for whatever a config block leaves out.
METHOD_DEFAULTS: dict[str, dict[str, Any]] = {
    "knn": {"n_neighbors": 5, "neighbor_weights": "distance"},
    "rf": {"n_estimators": 100, "max_depth": None},
    "mice": {"max_iter": 10},
}

# Validation metrics, in report column order: categorical, then continuous.
METRIC_KEYS = ("accuracy", "precision", "recall", "f1", "rmse", "mae", "r2")


def method_params(config: dict[str, Any], method: str) -> dict[str, Any]:
    """The method's hyperparameters: the config block's values over the defaults."""
    return {key: config.get(key, default) for key, default in METHOD_DEFAULTS[method].items()}


def missing_labels(missing_values: dict[str, list[str]] | list[str], column: str) -> list[str]:
    """The labels treated as missing for *column*: per-column dict or one shared list."""
    return missing_values.get(column, []) if isinstance(missing_values, dict) else missing_values


def metric_row(table: str, column: str, method: str, metrics: dict[str, Any]) -> dict[str, Any]:
    """One row of a validation or comparison summary."""
    return {
        "table": table,
        "variable": column,
        "method": method,
        "type": metrics.get("type", "unknown"),
        "n_samples": metrics.get("n_samples", 0),
        "n_folds": metrics.get("n_folds", 0),
        **{key: metrics.get(key) for key in METRIC_KEYS},
    }


def log_imputation_stats(
    method: str,
    columns: list[str],
    stats: dict[str, Any] | dict[str, dict[str, Any]],
    n_total: int,
) -> None:
    """Log a summary of imputation results for one config block.

    Works with both single-column stats (KNN: flat dict) and
    multi-column stats (MICE: ``{col: {...}, ...}``).

    Args:
        method: Label for the imputation method (e.g. ``"KNN"``, ``"MICE"``).
        columns: Target column names that were imputed.
        stats: Stats dict returned by ``impute_knn`` or ``impute_mice``.
        n_total: Total number of rows in the DataFrame.
    """
    # Normalise KNN's flat dict into per-column format
    if "n_imputed" in stats:
        stats = {columns[0]: stats}

    imputed = [c for c in columns if stats.get(c, {}).get("n_imputed", 0) > 0]
    if not imputed:
        return

    logger.info("Columns %s: Imputed using %s", imputed, method)
    for col in imputed:
        s = stats[col]
        logger.info(
            "  - %s imputed: %d/%d (%.1f%%)",
            col,
            s["n_imputed"],
            n_total,
            s["pct_imputed"],
        )


def validate_features_exist(
    df: pl.DataFrame,
    numeric_features: list[str] | None = None,
    categorical_features: list[str] | None = None,
) -> None:
    """Validate that at least one feature type is specified and all features exist.

    Args:
        df: DataFrame to validate against
        numeric_features: Optional list of numeric feature column names
        categorical_features: Optional list of categorical feature column names

    Raises:
        ValueError: If no features are specified or if any features are missing
    """
    if not numeric_features and not categorical_features:
        msg = "At least one of numeric_features or categorical_features must be specified"
        raise ValueError(msg)

    # Validate that all specified features exist in DataFrame
    all_features = set(numeric_features or []) | set(categorical_features or [])
    missing_features = all_features - set(df.columns)
    if missing_features:
        msg = f"Features not found in DataFrame: {missing_features}"
        raise ValueError(msg)


def count_missing(
    df: pl.DataFrame,
    column: str,
    numeric_features: list[str] | None,
    categorical_features: list[str] | None,
) -> tuple[int, float, dict[str, Any] | None]:
    """Check a single-column imputation's inputs and count what it has to fill.

    Returns:
        ``(n_missing, pct_missing, skip_stats)``: *skip_stats* is the stats
        dict to return unchanged when there is nothing to impute (no value
        missing, or every value missing), else None.

    Raises:
        ValueError: If *column* or a feature column is absent, or no feature
            is named.
    """
    if column not in df.columns:
        msg = f"Column '{column}' not found in DataFrame"
        raise ValueError(msg)

    validate_features_exist(df, numeric_features, categorical_features)

    n_missing = df[column].null_count()
    pct_missing = (n_missing / len(df)) * 100
    if n_missing == 0:
        logger.info("Column '%s': No missing values, skipping imputation", column)
        return 0, pct_missing, {"n_missing": 0, "n_imputed": 0, "pct_imputed": 0.0}
    if n_missing == len(df):
        logger.warning("Column '%s': All values are missing, cannot impute", column)
        skip = {"n_missing": n_missing, "n_imputed": 0, "pct_imputed": 100.0}
        return n_missing, pct_missing, skip
    return n_missing, pct_missing, None


def is_categorical(df: pl.DataFrame, column: str) -> bool:
    """Determine if a column should be treated as categorical.

    Args:
        df: DataFrame containing the column
        column: Column name to check

    Returns:
        True if column is categorical (non-float numeric or string), False otherwise
    """
    dtype = df[column].dtype
    return dtype.is_integer() or dtype == pl.String


def prepare_column_for_imputation(
    df: pl.DataFrame,
    table_name: str,
    column: str,
    missing_value_labels: list[str] | None = None,
) -> tuple[pl.DataFrame, list[Any]]:
    """Prepare a column for imputation by replacing missing values with null.

    This function resolves enum labels to their numeric values and replaces
    them with null, making the column ready for imputation algorithms.

    Args:
        df: DataFrame containing the column
        table_name: Name of the table (for enum resolution)
        column: Column name to prepare
        missing_value_labels: Optional list of enum labels to treat as missing
                            (e.g., ['MISSING', 'PNTA'])

    Returns:
        Tuple of (prepared_df, resolved_values) where resolved_values is the list
        of numeric/string values that were replaced with null

    Example:
        >>> df = pl.DataFrame({'income_broad': [1, 2, 995, 999, 3]})
        >>> df_prep, values = prepare_column_for_imputation(
        ...     df, 'households', 'income_broad', ['MISSING', 'PNTA']
        ... )
        >>> values
        [995, 999]
    """
    if not missing_value_labels:
        return df, []

    # Resolve enum labels to values
    missing_values = resolve_enum_labels(table_name, column, missing_value_labels)

    if not missing_values:
        logger.warning(
            "No missing values resolved for column '%s' in table '%s'",
            column,
            table_name,
        )
        return df, []

    logger.info(
        "Replacing missing values %s with null for column '%s' (from labels %s)",
        missing_values,
        column,
        missing_value_labels,
    )

    return df.with_columns(pl.col(column).replace(missing_values, None)), missing_values


# ---------------------------------------------------------------------------
# Encoding / decoding helpers
# ---------------------------------------------------------------------------


def decode_rounded(
    imputed_values: np.ndarray,
    mapping: dict[int, Any],
) -> list[Any]:
    """Decode imputed floats back to codes or labels through a code mapping.

    Reverses ``encode_string_columns`` (int -> label) and
    ``encode_integer_categoricals`` (dense int -> original code). Each float
    is rounded to the nearest integer, clamped to ``[min_key, max_key]``, and
    resolved via *mapping*.

    Args:
        imputed_values: 1-D array of imputed float values
        mapping: ``{int_key: decoded_value}``

    Returns:
        List of decoded values (strings *or* ints, depending on *mapping*)
    """
    max_key = max(mapping.keys())
    min_key = min(mapping.keys())
    fallback = mapping[min_key]

    return [mapping.get(max(min_key, min(round(v), max_key)), fallback) for v in imputed_values]


def decode_target(
    values: np.ndarray,
    column: str,
    dtype: pl.DataType,
    int_encodings: dict[str, dict[int, int]],
) -> pl.Series:
    """Turn an imputed column back into its original codes and dtype.

    A dense-encoded column is decoded through its mapping; any other integer
    column is rounded; a float column is returned as imputed.
    """
    if column in int_encodings:
        return pl.Series(column, decode_rounded(values, int_encodings[column])).cast(dtype)
    if dtype.is_integer():
        return pl.Series(column, values).round().cast(dtype)
    return pl.Series(column, values)


def encode_string_columns(
    df: pl.DataFrame,
    columns: list[str],
    verbose: bool = False,
) -> tuple[pl.DataFrame, dict[str, dict[int, str]]]:
    """Encode string columns to integer values for use in numeric imputation.

    Maps each unique non-null string value to an integer, preserving nulls.
    Returns the modified DataFrame and a mapping to decode back to strings.

    Args:
        df: DataFrame containing the columns to encode
        columns: List of column names to check and encode
        verbose: Whether to log encoding details

    Returns:
        Tuple of (encoded_df, encodings) where encodings is a dict mapping
        column name -> {int: label} for each encoded column
    """
    encodings: dict[str, dict[int, str]] = {}
    df_encoded = df.clone()

    for col in columns:
        if df_encoded[col].dtype not in (pl.Utf8, pl.String):
            continue

        unique_vals = df_encoded[col].drop_nulls().unique().sort().to_list()
        label_to_int = {label: i for i, label in enumerate(unique_vals)}
        int_to_label = {i: label for label, i in label_to_int.items()}
        encodings[col] = int_to_label

        df_encoded = df_encoded.with_columns(
            pl.col(col).replace_strict(label_to_int, default=None).cast(pl.Float64).alias(col)
        )

        if verbose:
            logger.info(
                "Encoded string column '%s' to integers: %s",
                col,
                {v: k for k, v in int_to_label.items()},
            )

    return df_encoded, encodings


def encode_integer_categoricals(
    df: pl.DataFrame,
    columns: list[str],
    verbose: bool = False,
) -> tuple[pl.DataFrame, dict[str, dict[int, int]]]:
    """Encode integer categorical columns to dense 0..N codes.

    Integer-coded enums (e.g. income_bin with codes 1-6) are already close to
    dense, but other fields may have non-contiguous codes.  Mapping to dense
    0-based integers keeps predictions in a valid range so that rounding
    always lands on a real category.

    Columns that are already coded 0..N-1 are skipped.

    Args:
        df: DataFrame containing the columns to encode
        columns: Target column names to check
        verbose: Whether to log encoding details

    Returns:
        Tuple of (encoded_df, encodings) where *encodings* maps
        ``column_name -> {dense_int: original_int}`` for every column
        that was re-coded.
    """
    encodings: dict[str, dict[int, int]] = {}
    df_encoded = df.clone()

    for col in columns:
        if not df_encoded[col].dtype.is_integer():
            continue

        unique_vals = df_encoded[col].drop_nulls().unique().sort().to_list()
        n = len(unique_vals)

        # Skip if already densely coded 0..N-1
        if unique_vals == list(range(n)):
            continue

        original_to_dense = {val: i for i, val in enumerate(unique_vals)}
        dense_to_original = {i: val for val, i in original_to_dense.items()}
        encodings[col] = dense_to_original

        df_encoded = df_encoded.with_columns(
            pl.col(col).replace_strict(original_to_dense, default=None).cast(pl.Float64).alias(col)
        )

        if verbose:
            logger.info(
                "Encoded integer column '%s' to dense codes: %s",
                col,
                dense_to_original,
            )

    return df_encoded, encodings


# ---------------------------------------------------------------------------
# Feature matrix construction (shared by KNN and MICE)
# ---------------------------------------------------------------------------


def build_feature_matrix(
    df: pl.DataFrame,
    target_columns: list[str],
    numeric_features: list[str],
    categorical_features: list[str],
) -> tuple[np.ndarray, dict[str, int], list[str]]:
    """Build a numpy feature matrix for imputation algorithms.

    Constructs a matrix from:

    1. **Continuous columns** — all *numeric_features* present in *df* plus
       any *target_columns* that are numeric.  Order-preserving deduplication
       ensures each column appears at most once.
    2. **One-hot encoded categoricals** — each unique value in a categorical
       feature becomes its own 0/1 column.  Target columns are excluded from
       one-hot expansion.

    Args:
        df: DataFrame to convert
        target_columns: Columns being imputed (included as continuous, excluded
            from one-hot expansion)
        numeric_features: Numeric / continuous feature column names
        categorical_features: Categorical feature column names (one-hot encoded)

    Returns:
        ``(matrix, column_indices, feature_names)`` where *column_indices*
        maps each target column name to its column offset in *matrix*, and
        *feature_names* lists all column names in matrix order.  One-hot
        encoded columns are named ``{cat_col}={value}``.
    """
    matrices: list[np.ndarray] = []
    feature_names: list[str] = []

    # Continuous features (deduped, order-preserving)
    continuous = list(
        dict.fromkeys(f for f in numeric_features if f in df.columns and df[f].dtype.is_numeric())
    )
    continuous.extend(
        col
        for col in target_columns
        if col not in continuous and col in df.columns and df[col].dtype.is_numeric()
    )

    if continuous:
        matrices.append(df.select(continuous).to_numpy())
        feature_names.extend(continuous)

    column_indices = {col: continuous.index(col) for col in target_columns if col in continuous}

    # One-hot encode categorical features (excluding targets)
    categorical = [
        f
        for f in categorical_features
        if f in df.columns and f not in target_columns and df[f].dtype.is_numeric()
    ]

    for cat_col in categorical:
        unique_vals = df[cat_col].drop_nulls().unique().sort().to_list()
        for val in unique_vals:
            matrices.append((df[cat_col] == val).cast(pl.Float64).to_numpy().reshape(-1, 1))
            feature_names.append(f"{cat_col}={val}")

    if not matrices:
        return np.empty((len(df), 0)), column_indices, feature_names

    return (
        np.hstack(matrices) if len(matrices) > 1 else matrices[0],
        column_indices,
        feature_names,
    )


def _join_key(table: str, owner: str) -> str:
    """The key joining *table* to the table its rows belong to, *owner*."""
    key = FK_RELATIONSHIPS.get((table, owner))
    if key is None:
        msg = (
            f"No foreign key relationship defined for {table} -> {owner}. "
            f"Known relationships: {list(FK_RELATIONSHIPS.keys())}"
        )
        raise ValueError(msg)
    return key


def join_parent_tables(
    df: pl.DataFrame,
    table_name: str,
    tables: dict[str, pl.DataFrame],
    join_tables: list[str],
) -> tuple[pl.DataFrame, list[str]]:
    """Join parent table columns onto a child table for use as imputation features.

    Looks up the foreign key relationship between the target table and each
    requested parent table, then left-joins all parent columns (excluding the
    join key itself, which already exists on the child).

    Args:
        df: The child DataFrame to enrich (e.g., persons)
        table_name: Name of the child table (e.g., "persons")
        tables: Dict of all available canonical tables
        join_tables: List of parent table names to join (e.g., ["households"])

    Returns:
        Tuple of (enriched_df, list of added column names)

    Raises:
        ValueError: If a requested parent table is not available or if no
            foreign key relationship is defined.
    """
    added_columns: list[str] = []

    for parent_name in join_tables:
        if parent_name not in tables:
            logger.warning("join_tables: table '%s' not available, skipping", parent_name)
            continue

        join_col = _join_key(table_name, parent_name)
        parent_df = tables[parent_name]

        # Select parent columns that don't already exist on the child
        parent_cols = [c for c in parent_df.columns if c not in df.columns]
        if not parent_cols:
            logger.info(
                "join_tables: no new columns from '%s' to add to '%s'",
                parent_name,
                table_name,
            )
            continue

        # Left join parent columns
        df = df.join(
            parent_df.select([join_col, *parent_cols]),
            on=join_col,
            how="left",
        )
        added_columns.extend(parent_cols)

        logger.info(
            "Joined %d columns from '%s' onto '%s' via '%s'",
            len(parent_cols),
            parent_name,
            table_name,
            join_col,
        )

    return df, added_columns


def add_household_agg_features(
    df: pl.DataFrame,
    target_columns: list[str],
) -> tuple[pl.DataFrame, list[str]]:
    """Count the rest of each person's household in each code of each target column.

    Captures within-household correlation (e.g., siblings likely share
    race/ethnicity). Column ``gender`` gains ``n_hh_gender=1``,
    ``n_hh_gender=2``, ...: how many members of the person's household, not
    counting the person, have that code. A child of a man and a woman counts
    one of each; a person living alone counts zero. Members with a null value
    are not counted.

    The person is left out because their own value is the target: counting it
    would hand the model the answer on the rows it learns from, and not on the
    rows it predicts.

    Counts, not the most common value: a tie (one man, one woman) has no single
    answer, and settling it by row order made imputation differ run to run.

    Args:
        df: DataFrame with person-level records (must contain hh_id and person_id)
        target_columns: List of columns to compute household aggregates for

    Returns:
        Tuple of (enriched_df, list of added column names)
    """
    if "hh_id" not in df.columns or "person_id" not in df.columns:
        logger.info("Skipping household aggregation: 'hh_id' or 'person_id' not in columns")
        return df, []

    added_columns: list[str] = []

    for col in target_columns:
        if col not in df.columns:
            continue

        values = df[col].drop_nulls().unique().sort().to_list()
        names = [f"n_hh_{col}={val}" for val in values]
        # The household's count of each code, less the person's own.
        df = df.with_columns(
            (
                (pl.col(col) == val).fill_null(value=False).sum().over("hh_id")
                - (pl.col(col) == val).fill_null(value=False)
            )
            .cast(pl.Int32)
            .alias(name)
            for val, name in zip(values, names, strict=True)
        )
        added_columns.extend(names)

        logger.info("Added %d household count features for column '%s'", len(names), col)

    return df, added_columns


def aggregate_from_children(
    df: pl.DataFrame,
    table_name: str,
    tables: dict[str, pl.DataFrame],
    aggregate_config: dict[str, dict[str, list[str]]],
) -> tuple[pl.DataFrame, list[str]]:
    """Aggregate child table columns onto a parent table via pivot counts.

    For each child table and each field listed under ``pivot_count``, creates
    one column per unique value counting occurrences within each parent group.

    For example, ``pivot_count: [employment]`` on persons grouped by ``hh_id``
    creates ``persons_count_employment=1``, ``persons_count_employment=2``, etc.
    The sum across a field's pivoted columns equals the household size, so a
    separate ``count`` option is unnecessary.

    Args:
        df: The parent DataFrame to enrich (e.g., households)
        table_name: Name of the parent table (e.g., "households")
        tables: Dict of all available canonical tables
        aggregate_config: Mapping of child table name to aggregation spec.
            Each spec supports:
            - ``pivot_count``: list of child column names to pivot-count

    Returns:
        Tuple of (enriched_df, list of added column names)

    Raises:
        ValueError: If no FK relationship is defined for the child→parent pair.
    """
    added_columns: list[str] = []

    for child_name, agg_spec in aggregate_config.items():
        if child_name not in tables:
            logger.warning(
                "aggregate_from: table '%s' not available, skipping",
                child_name,
            )
            continue

        join_col = _join_key(child_name, table_name)
        child_df = tables[child_name]
        pivot_count_fields = agg_spec.get("pivot_count", [])

        for field in pivot_count_fields:
            if field not in child_df.columns:
                logger.warning(
                    "aggregate_from: field '%s' not in '%s', skipping",
                    field,
                    child_name,
                )
                continue

            # Get sorted unique non-null values
            unique_vals = child_df[field].drop_nulls().unique().sort().to_list()
            if not unique_vals:
                continue

            # Build one aggregation expression per unique value
            pivot_exprs = []
            pivot_col_names = []
            for val in unique_vals:
                col_name = f"{child_name}_count_{field}={val}"
                pivot_exprs.append((pl.col(field) == val).sum().alias(col_name))
                pivot_col_names.append(col_name)

            # Group child rows by parent key and compute counts
            agg_df = child_df.group_by(join_col).agg(pivot_exprs)

            # Left join onto parent and fill nulls with 0
            df = df.join(agg_df, on=join_col, how="left")
            df = df.with_columns([pl.col(c).fill_null(0) for c in pivot_col_names])

            added_columns.extend(pivot_col_names)

            logger.info(
                "Added %d pivot_count columns for '%s.%s'",
                len(pivot_col_names),
                child_name,
                field,
            )

    return df, added_columns


def enrich_dataframe(
    df: pl.DataFrame,
    table_name: str,
    tables: dict[str, pl.DataFrame] | None,
    join_tables_list: list[str],
    target_columns: list[str],
    aggregate_from_config: dict[str, dict[str, list[str]]] | None = None,
    numeric_features: list[str] | None = None,
) -> tuple[pl.DataFrame, list[str], list[str] | None]:
    """Enrich a DataFrame with parent joins and child aggregations.

    Handles both directions:
      - Parent→child joins via ``join_tables`` config
      - Child→parent aggregations via ``aggregate_from`` config

    The added counts (household counts, ``pivot_count``) are numeric features:
    a count is a number, and as a category it would split into one column per
    count value.

    Returns:
        Tuple of (enriched_df, added_column_names, numeric_features), the last
        with the added counts in place.
    """
    added_columns: list[str] = []

    # Parent joins (e.g. persons joining household columns)
    if join_tables_list and tables:
        df, joined_cols = join_parent_tables(df, table_name, tables, join_tables_list)
        added_columns.extend(joined_cols)

        df, agg_cols = add_household_agg_features(df, target_columns)
        added_columns.extend(agg_cols)
        if agg_cols:
            numeric_features = list(numeric_features or []) + agg_cols

    # Child aggregations (e.g. households aggregating from persons)
    if aggregate_from_config and tables:
        df, child_cols = aggregate_from_children(df, table_name, tables, aggregate_from_config)
        added_columns.extend(child_cols)
        if child_cols:
            numeric_features = list(numeric_features or []) + child_cols

    return df, added_columns, numeric_features
