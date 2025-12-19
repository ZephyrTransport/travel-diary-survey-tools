"""Loads all canonical tables from input paths."""

import logging
from pathlib import Path

import geopandas as gpd
import polars as pl

from pipeline.decoration import step

logger = logging.getLogger(__name__)


@step()
def load_data(
    input_paths: dict[str, str],
) -> dict[str, pl.DataFrame | gpd.GeoDataFrame]:
    """Load all canonical tables from input paths."""
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
            data[table] = pl.read_csv(path)
        elif path.endswith(".parquet"):
            data[table] = pl.read_parquet(path)
        elif path.endswith((".shp", ".shp.zip", ".geojson")):
            data[table] = gpd.read_file(path)
        else:
            msg = f"Unsupported file format for table {table}: {path}"
            raise ValueError(msg)

    logger.info("All data loaded successfully.")
    return data


@step()
def write_data(
    output_paths: dict[str, str],
    canonical_data: dict[str, pl.DataFrame | gpd.GeoDataFrame],
    validate_input: bool,
    create_dirs: bool = True,
) -> None:
    """Write all canonical tables to output paths."""
    # Validate all tables first
    for table in output_paths:
        logger.info("Validating %s...", table)

        # If the table is truly canonical, validate it
        if table in canonical_data.__annotations__ and validate_input:
            canonical_data.validate(table)

    for table, path in output_paths.items():
        logger.info("Writing %s to:\n%s...", table, path)

        df = getattr(canonical_data, table)
        file_path = Path(path)

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
