"""Data input/output operations for the survey processing pipeline.

This module provides pipeline steps for loading canonical survey tables from files
and writing them to various output formats.
"""

import inspect
import logging
from pathlib import Path
from typing import get_args, get_origin

import geopandas as gpd
import openpyxl
import polars as pl

from data_canon.core.dataclass import CanonicalData
from data_canon.core.labeled_enum import LabeledEnum
from pipeline.decoration import step

logger = logging.getLogger(__name__)


def _extract_enum_from_type(annotation: object) -> type[LabeledEnum] | None:
    """Extract a LabeledEnum class from a type annotation, handling Unions.

    Args:
        annotation: The field's type annotation (e.g. ``PurposeCategory | None``).

    Returns:
        The LabeledEnum subclass if one is found, otherwise ``None``.
    """
    origin = get_origin(annotation)
    if origin is not None:
        for arg in get_args(annotation):
            if arg is type(None):
                continue
            if inspect.isclass(arg) and issubclass(arg, LabeledEnum):
                return arg
        return None
    if inspect.isclass(annotation) and issubclass(annotation, LabeledEnum):
        return annotation
    return None


_EXCLUDE_CODEBOOK_MODULES = {
    "data_canon.codebook.ctramp",
    "data_canon.codebook.daysim",
}


def _collect_enums_for_tables(
    table_names: list[str],
    canonical_data: CanonicalData,
) -> dict[str, type[LabeledEnum]]:
    """Collect all LabeledEnum classes referenced by the given table models.

    Iterates the Pydantic model for each table and returns a deduplicated
    mapping of ``{class_name: class}`` ordered by insertion.  Enums defined
    in model-specific codebook modules (CT-RAMP, DaySim) are excluded because
    they represent travel-model output codes rather than survey labels.

    Args:
        table_names: Table names whose models should be inspected.
        canonical_data: CanonicalData instance that holds the model mapping.

    Returns:
        Ordered dict of enum class name to enum class.
    """
    seen: dict[str, type[LabeledEnum]] = {}
    for table in table_names:
        model = canonical_data.models.get(table)
        if model is None:
            continue
        for field_info in model.model_fields.values():
            enum_cls = _extract_enum_from_type(field_info.annotation)
            if enum_cls is not None and enum_cls.__module__ not in _EXCLUDE_CODEBOOK_MODULES:
                seen[enum_cls.__name__] = enum_cls
    return seen


def _write_enum_codebook(
    path: str,
    enums: dict[str, type[LabeledEnum]],
) -> None:
    """Write a .xlsx workbook with one sheet per LabeledEnum.

    Each sheet has three columns:
    - ``{EnumName} Value`` — the integer (or string) value
    - ``{EnumName} Label`` — the human-readable label
    - ``{EnumName} Value Label`` — value and label joined with a space

    Args:
        path: Destination path for the .xlsx file.
        enums: Mapping of enum class name to enum class to write.
    """
    wb = openpyxl.Workbook()
    wb.remove(wb.active)  # remove the default empty sheet

    for enum_name, enum_cls in sorted(enums.items()):
        ws = wb.create_sheet(title=enum_name[:31])  # Excel sheet names max 31 chars
        ws.append([f"{enum_name} Value", f"{enum_name} Label", f"{enum_name} Value Label"])
        for member in enum_cls:
            ws.append([member.value, member.label, f"{member.value} {member.label}"])

    Path(path).parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)
    logger.info("Wrote enum codebook (%d sheets) to %s", len(enums), path)


@step()
def load_data(
    input_paths: dict[str, str],
) -> dict[str, pl.DataFrame | gpd.GeoDataFrame]:
    """Load canonical survey tables from file paths into memory.

    Args:
        input_paths: Dictionary mapping table names to file paths.
            Supported formats: CSV, TSV, Parquet, Shapefile, GeoJSON.

    Returns:
        Dictionary of table names to DataFrames (pl.DataFrame or gpd.GeoDataFrame).
        Typical tables include households, persons, days, unlinked_trips, etc.

    Algorithm:
        1. Iterate through each table name and file path in input_paths
        2. Validate file path exists, providing helpful error message with broken path component
        3. Load data based on file extension:
            - .csv/.tsv → polars.read_csv()
            - .parquet → polars.read_parquet()
            - .shp/.shp.zip/.geojson → geopandas.read_file()
        4. Return dictionary of loaded tables

    Notes:
        - All CSV/Parquet files loaded as Polars DataFrames for performance
        - Geospatial files loaded as GeoPandas GeoDataFrames
        - Path validation helps diagnose configuration errors
    """
    data = {}

    for table, path in input_paths.items():
        logger.info("Loading %s...", table)

        # Check if path is correct.
        # If not, trace from the root directory up until broken path
        p = Path(path)
        if not p.exists():
            trace_path = p
            while not trace_path.exists() and trace_path != trace_path.parent:
                broke_at = trace_path.name
                trace_path = trace_path.parent
            msg = (
                f"Path for table {table} does not exist at {path}. "
                f"Possibly broken at: {broke_at} in {trace_path}?"
            )
            raise FileNotFoundError(msg)

        # If .csv file, use polars to read
        if path.endswith(".csv"):
            data[table] = pl.read_csv(path, infer_schema_length=10000)
        elif path.endswith(".tsv"):
            data[table] = pl.read_csv(path, separator="\t", infer_schema_length=10000)
        elif path.endswith(".parquet"):
            data[table] = pl.read_parquet(path)
        elif path.endswith((".shp", ".shp.zip", ".geojson")):
            data[table] = gpd.read_file(path)
        else:
            msg = f"Unsupported file format for table {table}: {path}"
            raise ValueError(msg)

    logger.info("All data loaded successfully.")
    return data


def _write_checks(
    canonical_data: CanonicalData,
    table: str,
) -> None:
    """Perform checks before writing a canonical table."""
    # Check that the table exists at all
    if not hasattr(canonical_data, table):
        msg = f"Missing table {table}; cannot write."
        raise ValueError(msg)

    df = getattr(canonical_data, table)

    # Check that it's not empty
    if df is None or (isinstance(df, pl.DataFrame) and df.is_empty()):
        msg = f"Table {table} is empty; cannot write."
        raise ValueError(msg)

    # If the table is truly canonical, validate it
    if table in canonical_data.models:
        logger.info("Validating %s...", table)
        canonical_data.validate(table)
    else:
        logger.warning(
            "Table %s not in CanonicalData models; skipping validation.",
            table,
        )


def _select_canonical_columns(
    df: pl.DataFrame, table: str, canonical_data: CanonicalData
) -> pl.DataFrame:
    """Narrow *df* to the columns the pipeline stands behind, in their original order.

    Everything else is vendor passthrough or a working column that outlived its
    step. Neither is part of what this pipeline promises, and shipping them made
    the delivered files unreadable: BATS-2023 ``persons`` carries 220 columns of
    which 31 are declared.

    A table with no model is left alone -- it is not ours to narrow.
    """
    if table not in canonical_data.models:
        return df
    public = canonical_data.public_columns(table)
    keep = [c for c in df.columns if c in public]
    dropped = len(df.columns) - len(keep)
    if dropped:
        logger.info("  %s: keeping %d canonical columns, dropping %d", table, len(keep), dropped)
    return df.select(keep)


@step()
def write_data(  # noqa: C901, PLR0912
    output_paths: dict[str, str],
    canonical_data: CanonicalData,
    validate_input: bool,
    write_only_canonical: bool | None = None,
    create_dirs: bool = True,
    enum_codebook_path: str | None = None,
) -> None:
    """Write canonical survey tables to output file paths.

    Args:
        output_paths: Dictionary mapping table names to output file paths.
        canonical_data: CanonicalData instance containing DataFrames to write.
        validate_input: Whether to run validation before writing.
        write_only_canonical: Keep only columns the pipeline stands behind --
            declared model fields plus the config-named columns steps register
            (zone ids, pre-imputation stashes). ``False`` dumps every column on
            the frame, which is what you want for debugging. **Required**: there
            is no sensible default, and silently choosing one would either drop
            columns a project expected or ship hundreds it did not.
        create_dirs: Whether to create parent directories (default: True).
        enum_codebook_path: Optional path for an .xlsx enum codebook.
            When provided, a workbook is written with one worksheet per
            LabeledEnum found in the models for the written tables.
            Each sheet contains ``Value``, ``Label``, and ``Value Label``
            columns.  Must end in ``.xlsx``.

    Algorithm:
        1. If validate_input=True, validate each table using canonical data models
        2. For each table in output_paths:
            - Retrieve DataFrame from canonical_data
            - Create parent directories if needed
            - Write data based on file extension:
                - .csv → DataFrame.write_csv()
                - .parquet → DataFrame.write_parquet()
                - .shp/.shp.zip/.geojson → GeoDataFrame.to_file()
                - .txt → Path.write_text()
        3. If enum_codebook_path is set, discover all LabeledEnum types from
           the models for the written tables and write a codebook workbook.
        4. Log completion status

    Notes:
        - Validation ensures output conforms to canonical data schemas
        - Automatic directory creation prevents path errors
        - Supports multiple output formats for flexibility
    """
    if write_only_canonical is None:
        msg = (
            "write_data requires write_only_canonical to be set explicitly. "
            "Use true to deliver only the documented schema (declared fields "
            "plus registered zone/pre-imputation columns), or false to dump "
            "every column on the frame for debugging."
        )
        raise ValueError(msg)

    for table, path in output_paths.items():
        logger.info("Writing %s to:\n%s...", table, path)

        df = getattr(canonical_data, table)
        file_path = Path(path)

        if write_only_canonical and isinstance(df, pl.DataFrame):
            df = _select_canonical_columns(df, table, canonical_data)

        # Sort by hh_id then person_id where available so related tables
        # (e.g. householdData and personData) are in the same order and
        # can be visually compared or joined without an extra sort step.
        # Only polars frames: text outputs are plain strings, and geo outputs
        # are GeoDataFrames, which sort via a different API.
        if isinstance(df, pl.DataFrame):
            sort_cols = [c for c in ("hh_id", "person_id") if c in df.columns]
            if sort_cols:
                df = df.sort(sort_cols)

        # Perform checks before writing
        if validate_input:
            _write_checks(canonical_data, table)

        if create_dirs:
            file_path.parent.mkdir(parents=True, exist_ok=True)

        if path.endswith(".csv"):
            df.write_csv(path)
        elif path.endswith(".parquet"):
            df.write_parquet(path)
        elif path.endswith((".shp", ".shp.zip", ".geojson")):
            df.to_file(path)
        elif path.endswith(".txt"):
            file_path.write_text(df, encoding="utf-8")
        else:
            msg = f"Unsupported file format for table {table}: {path}"
            raise ValueError(msg)

    if enum_codebook_path is not None:
        if not enum_codebook_path.endswith(".xlsx"):
            msg = f"enum_codebook_path must end in .xlsx, got: {enum_codebook_path}"
            raise ValueError(msg)
        enums = _collect_enums_for_tables(list(output_paths.keys()), canonical_data)
        _write_enum_codebook(enum_codebook_path, enums)

    logger.info("All data written successfully.")
