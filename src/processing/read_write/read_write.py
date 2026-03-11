"""Data input/output operations for the survey processing pipeline.

This module provides pipeline steps for loading canonical survey tables from files
and writing them to various output formats.
"""

import logging
from pathlib import Path

import geopandas as gpd
import polars as pl

from data_canon.core.dataclass import CanonicalData
from pipeline.decoration import step

logger = logging.getLogger(__name__)


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


@step()
def write_data(
    output_paths: dict[str, str],
    canonical_data: CanonicalData,
    validate_input: bool,
    create_dirs: bool = True,
) -> None:
    """Write canonical survey tables to output file paths.

    Args:
        output_paths: Dictionary mapping table names to output file paths.
        canonical_data: CanonicalData instance containing DataFrames to write.
        validate_input: Whether to run validation before writing.
        create_dirs: Whether to create parent directories (default: True).

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
        3. Log completion status

    Notes:
        - Validation ensures output conforms to canonical data schemas
        - Automatic directory creation prevents path errors
        - Supports multiple output formats for flexibility
    """
    for table, path in output_paths.items():
        logger.info("Writing %s to:\n%s...", table, path)

        df = getattr(canonical_data, table)
        file_path = Path(path)

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

    logger.info("All data written successfully.")
