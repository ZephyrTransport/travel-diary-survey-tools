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


def is_categorical(df: pl.DataFrame, column: str) -> bool:
    """Determine if a column should be treated as categorical.

    Args:
        df: DataFrame containing the column
        column: Column name to check

    Returns:
        True if column is categorical (non-float numeric or string), False otherwise
    """
    dtype = df[column].dtype
    return dtype in (
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.UInt8,
        pl.UInt16,
        pl.UInt32,
        pl.UInt64,
        pl.Utf8,
    )


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

    # Log what we're doing
    logger.info(
        "Replacing missing values %s with null for column '%s' (from labels %s)",
        missing_values,
        column,
        missing_value_labels,
    )

    # Replace missing values with null inline
    expr = pl.col(column)
    for value in missing_values:
        expr = expr.replace(value, None)
    df_prepared = df.with_columns(expr)

    return df_prepared, missing_values


# ---------------------------------------------------------------------------
# Encoding / decoding helpers
# ---------------------------------------------------------------------------


def _decode_rounded(
    imputed_values: np.ndarray,
    mapping: dict[int, Any],
) -> list[Any]:
    """Round imputed floats and look up in a code mapping.

    This is the shared implementation for both ``decode_integer_to_string``
    and ``decode_dense_to_integer``.  Each float is rounded to the nearest
    integer, clamped to ``[min_key, max_key]``, and resolved via *mapping*.

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


def decode_integer_to_string(
    imputed_values: np.ndarray,
    int_to_label: dict[int, str],
) -> list[str]:
    """Decode imputed float values back to string labels.

    Rounds each value to the nearest integer and clamps to the valid
    range of encoded labels.
    """
    return _decode_rounded(imputed_values, int_to_label)


def decode_dense_to_integer(
    imputed_values: np.ndarray,
    dense_to_original: dict[int, int],
) -> list[int]:
    """Decode dense 0..N predictions back to original integer codes.

    Rounds each value to the nearest integer and clamps to the valid
    range of dense keys before looking up the original code.
    """
    return _decode_rounded(imputed_values, dense_to_original)


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
) -> tuple[np.ndarray, dict[str, int]]:
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
        ``(matrix, column_indices)`` where *column_indices* maps each target
        column name to its column offset in *matrix*.
    """
    matrices: list[np.ndarray] = []

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

    column_indices = {col: continuous.index(col) for col in target_columns if col in continuous}

    # One-hot encode categorical features (excluding targets)
    categorical = [
        f
        for f in categorical_features
        if f in df.columns and f not in target_columns and df[f].dtype.is_numeric()
    ]

    for cat_col in categorical:
        unique_vals = df[cat_col].drop_nulls().unique().sort().to_list()
        matrices.extend(
            (df[cat_col] == val).cast(pl.Float64).to_numpy().reshape(-1, 1) for val in unique_vals
        )

    if not matrices:
        return np.empty((len(df), 0)), column_indices

    return (
        np.hstack(matrices) if len(matrices) > 1 else matrices[0],
        column_indices,
    )


# Foreign key relationships between canonical tables.
# Maps (child_table, parent_table) -> join_key column name.
FK_RELATIONSHIPS: dict[tuple[str, str], str] = {
    ("persons", "households"): "hh_id",
    ("days", "persons"): "person_id",
    ("days", "households"): "hh_id",
    ("unlinked_trips", "days"): "day_id",
    ("unlinked_trips", "persons"): "person_id",
    ("unlinked_trips", "households"): "hh_id",
    ("linked_trips", "days"): "day_id",
    ("linked_trips", "persons"): "person_id",
    ("linked_trips", "households"): "hh_id",
    ("tours", "persons"): "person_id",
    ("tours", "households"): "hh_id",
}


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
            logger.warning("join_tables: parent table '%s' not available, skipping", parent_name)
            continue

        fk_key = (table_name, parent_name)
        if fk_key not in FK_RELATIONSHIPS:
            msg = (
                f"No foreign key relationship defined for "
                f"{table_name} -> {parent_name}. "
                f"Known relationships: {list(FK_RELATIONSHIPS.keys())}"
            )
            raise ValueError(msg)

        join_col = FK_RELATIONSHIPS[fk_key]
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
    hh_id_col: str = "hh_id",
    person_id_col: str = "person_id",
) -> tuple[pl.DataFrame, list[str]]:
    """Add household-aggregated features for target columns.

    For each target column, computes the mode of that column among *other*
    household members (excluding the current person). This captures
    within-household correlation (e.g., siblings likely share race/ethnicity).

    The new columns are named ``hh_mode_{column}``.

    For single-person households or when all other members have null values,
    the aggregated value will be null.

    Args:
        df: DataFrame with person-level records (must contain hh_id and person_id)
        target_columns: List of columns to compute household aggregates for
        hh_id_col: Name of the household ID column
        person_id_col: Name of the person ID column

    Returns:
        Tuple of (enriched_df, list of added column names)
    """
    if hh_id_col not in df.columns or person_id_col not in df.columns:
        logger.info(
            "Skipping household aggregation: '%s' or '%s' not in columns",
            hh_id_col,
            person_id_col,
        )
        return df, []

    added_columns: list[str] = []

    for col in target_columns:
        if col not in df.columns:
            continue

        agg_col_name = f"hh_mode_{col}"

        # For each person, compute the mode of `col` among other HH members
        # Step 1: Build a lookup of hh_id + person_id -> value
        # Step 2: Self-join excluding current person, group by hh_id+person_id, take mode

        # Get all (hh_id, person_id, col_value) with non-null values
        valid = df.select([hh_id_col, person_id_col, col]).filter(pl.col(col).is_not_null())

        if valid.is_empty():
            # No valid values to aggregate
            df = df.with_columns(pl.lit(None).alias(agg_col_name))
            added_columns.append(agg_col_name)
            continue

        # Cross-join within household: for each person, get other members' values
        # Rename to avoid column conflicts
        others = valid.rename({person_id_col: f"_other_{person_id_col}", col: f"_other_{col}"})

        # Join on hh_id, then filter out self
        crossed = (
            df.select([hh_id_col, person_id_col])
            .join(others, on=hh_id_col, how="left")
            .filter(pl.col(person_id_col) != pl.col(f"_other_{person_id_col}"))
        )

        # Compute mode per person
        hh_agg = crossed.group_by([hh_id_col, person_id_col]).agg(
            pl.col(f"_other_{col}").mode().first().alias(agg_col_name)
        )

        # Join back
        df = df.join(hh_agg, on=[hh_id_col, person_id_col], how="left")
        added_columns.append(agg_col_name)

        logger.info(
            "Added household aggregate feature '%s' for column '%s'",
            agg_col_name,
            col,
        )

    return df, added_columns


def aggregate_from_children(
    df: pl.DataFrame,
    table_name: str,
    tables: dict[str, pl.DataFrame],
    aggregate_config: dict[str, dict[str, list[str]]],
    verbose: bool = True,
) -> tuple[pl.DataFrame, list[str]]:
    """Aggregate child table columns onto a parent table via pivot counts.

    For each child table and each field listed under ``pivot_count``, creates
    one column per unique value counting occurrences within each parent group.

    For example, ``pivot_count: [employment]`` on persons grouped by ``hh_id``
    creates ``persons_count_employment_1``, ``persons_count_employment_2``, etc.
    The sum across a field's pivoted columns equals the household size, so a
    separate ``count`` option is unnecessary.

    Args:
        df: The parent DataFrame to enrich (e.g., households)
        table_name: Name of the parent table (e.g., "households")
        tables: Dict of all available canonical tables
        aggregate_config: Mapping of child table name to aggregation spec.
            Each spec supports:
            - ``pivot_count``: list of child column names to pivot-count
        verbose: Whether to log aggregation details

    Returns:
        Tuple of (enriched_df, list of added column names)

    Raises:
        ValueError: If no FK relationship is defined for the child→parent pair.
    """
    added_columns: list[str] = []

    for child_name, agg_spec in aggregate_config.items():
        if child_name not in tables:
            logger.warning(
                "aggregate_from: child table '%s' not available, skipping",
                child_name,
            )
            continue

        # Look up the FK from child → parent
        fk_key = (child_name, table_name)
        if fk_key not in FK_RELATIONSHIPS:
            msg = (
                f"No foreign key relationship defined for "
                f"{child_name} -> {table_name}. "
                f"Known relationships: {list(FK_RELATIONSHIPS.keys())}"
            )
            raise ValueError(msg)

        join_col = FK_RELATIONSHIPS[fk_key]
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
                col_name = f"{child_name}_count_{field}_{val}"
                pivot_exprs.append((pl.col(field) == val).sum().alias(col_name))
                pivot_col_names.append(col_name)

            # Group child rows by parent key and compute counts
            agg_df = child_df.group_by(join_col).agg(pivot_exprs)

            # Left join onto parent and fill nulls with 0
            df = df.join(agg_df, on=join_col, how="left")
            df = df.with_columns([pl.col(c).fill_null(0) for c in pivot_col_names])

            added_columns.extend(pivot_col_names)

            if verbose:
                logger.info(
                    "Added %d pivot_count columns for '%s.%s'",
                    len(pivot_col_names),
                    child_name,
                    field,
                )

    return df, added_columns


def strip_joined_columns(
    df: pl.DataFrame,
    added_columns: list[str],
) -> pl.DataFrame:
    """Remove columns that were temporarily added for imputation.

    Args:
        df: DataFrame with extra columns
        added_columns: List of column names to remove

    Returns:
        DataFrame with added columns removed
    """
    cols_to_drop = [c for c in added_columns if c in df.columns]
    if cols_to_drop:
        df = df.drop(cols_to_drop)
    return df


def enrich_dataframe(
    df: pl.DataFrame,
    table_name: str,
    tables: dict[str, pl.DataFrame] | None,
    join_tables_list: list[str],
    target_columns: list[str],
    categorical_features: list[str] | None,
    aggregate_from_config: dict[str, dict[str, list[str]]] | None = None,
) -> tuple[pl.DataFrame, list[str], list[str] | None]:
    """Enrich a DataFrame with parent joins and child aggregations.

    Handles both directions:
      - Parent→child joins via ``join_tables`` config
      - Child→parent aggregations via ``aggregate_from`` config

    Returns:
        Tuple of (enriched_df, added_column_names, updated_categorical_features)
    """
    added_columns: list[str] = []

    # Parent joins (e.g. persons joining household columns)
    if join_tables_list and tables:
        df, joined_cols = join_parent_tables(df, table_name, tables, join_tables_list)
        added_columns.extend(joined_cols)

        df, agg_cols = add_household_agg_features(df, target_columns)
        added_columns.extend(agg_cols)
        if agg_cols:
            categorical_features = list(categorical_features or []) + agg_cols

    # Child aggregations (e.g. households aggregating from persons)
    if aggregate_from_config and tables:
        df, child_cols = aggregate_from_children(df, table_name, tables, aggregate_from_config)
        added_columns.extend(child_cols)
        if child_cols:
            categorical_features = list(categorical_features or []) + child_cols

    return df, added_columns, categorical_features
