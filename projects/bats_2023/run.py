"""Runner script for the BATS 2023 DaySim processing pipeline."""

import argparse
import logging
import os
import shutil
import sys
from pathlib import Path

import geopandas as gpd
import polars as pl
import yaml

from pipeline.decoration import step
from pipeline.pipeline import Pipeline
from processing import (
    detect_joint_trips,
    extract_tours,
    format_daysim,
    link_trips,
    load_data,
    write_data,
)
from processing.cleaning.clean_bats_2023 import clean_2023_bats

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

logger = logging.getLogger(__name__)

# For MTC network drives that seem to keep unmapping within python VM sessions
# Check if network drives are mapped; if not, map them
drives = {
    "M:": r"\\models.ad.mtc.ca.gov\data\models",
    "X:": r"\\model3-a\Model3A-Share",
}

for drive, path in drives.items():
    if not Path(drive).exists():
        logger.info("Mapping network drive %s to %s", drive, path)
        os.system(f"net use {drive} {path}")  # noqa: S605

# Path to the YAML config file you provided
CONFIG_PATH = Path(__file__).parent / "config.yaml"

# ---------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------
# Read output directory from config to place log file there
with CONFIG_PATH.open() as f:
    config = yaml.safe_load(f)
    output_dir = Path(config.get("output_dir", "output"))
    output_dir.mkdir(parents=True, exist_ok=True)

# Configure logging to both console and file
log_file = output_dir / "pipeline.log"
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler(
    stream=sys.stdout.reconfigure(errors="replace") if hasattr(sys.stdout, "reconfigure") else sys.stdout
)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# Get root logger, clear existing handlers, and configure it
root_logger = logging.getLogger()
root_logger.handlers.clear()
root_logger.setLevel(logging.DEBUG)
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

logger = logging.getLogger(__name__)
logger.info("Log file: %s", log_file)


# Optional: project-specific custom step functions
# You can define or import them here if needed
@step()
def custom_add_zone_ids(
    households: pl.DataFrame,
    persons: pl.DataFrame,
    linked_trips: pl.DataFrame,
    zone_geographies: list[dict],
) -> dict:
    """Add zone IDs for multiple geographic levels based on locations.

    Automatically applies each zone geography to standard locations:
    - households: home_lon/lat → home_{zone_name}
    - persons: work_lon/lat → work_{zone_name},
                school_lon/lat → school_{zone_name}
    - linked_trips: o_lon/lat → o_{zone_name}, d_lon/lat → d_{zone_name}

    Args:
        households: Households dataframe
        persons: Persons dataframe
        linked_trips: Linked trips dataframe
        zone_geographies: List of dicts, each containing:
            - shapefile: Path to shapefile with zone boundaries (str)
            - zone_id_field: Field name in shapefile for zone ID
            - zone_name: Short name for zone type (e.g., 'taz', 'maz', 'county')

    Returns:
        Dictionary with updated dataframes
    """
    def add_zone_to_dataframe(
        df: pl.DataFrame,
        shp: gpd.GeoDataFrame,
        lon_col: str,
        lat_col: str,
        zone_col_name: str,
        zone_id_field: str,
    ) -> pl.DataFrame:
        """Add zone ID to dataframe based on lon/lat coordinates."""
        gdf = gpd.GeoDataFrame(
            df.to_pandas(),
            geometry=gpd.points_from_xy(
                df[lon_col].to_list(), df[lat_col].to_list()
            ),
            crs="EPSG:4326",
        )

        # Prepare shapefile for spatial join and ensure zone ID is string
        shp_prepared = shp.loc[:, [zone_id_field, "geometry"]].copy()
        shp_prepared[zone_id_field] = shp_prepared[zone_id_field].astype(str)
        shp_prepared = shp_prepared.set_index(zone_id_field)

        # Spatial join to find zone containing each point
        gdf_joined = gpd.sjoin(
            gdf, shp_prepared, how="left", predicate="within"
        )
        gdf_joined = gdf_joined.rename(columns={zone_id_field: zone_col_name})
        gdf_joined = gdf_joined.drop(columns="geometry")

        return pl.from_pandas(gdf_joined)

    results = {
        "households": households,
        "persons": persons,
        "linked_trips": linked_trips,
    }

    # Process each zone geography
    for zone_config in zone_geographies:
        shapefile_path = zone_config["shapefile"]
        zone_id_field = zone_config["zone_id_field"]
        zone_name = zone_config["zone_name"]

        logger.info(
            "Adding %s IDs using field '%s' from %s",
            zone_name.upper(),
            zone_id_field,
            shapefile_path,
        )

        # Load the shapefile
        shapefile = gpd.read_file(shapefile_path)

        # Standard location mappings: (table, lon_col, lat_col, location_prefix)
        standard_locations = [
            ("households", "home_lon", "home_lat", "home"),
            ("persons", "work_lon", "work_lat", "work"),
            ("persons", "school_lon", "school_lat", "school"),
            ("linked_trips", "o_lon", "o_lat", "o"),
            ("linked_trips", "d_lon", "d_lat", "d"),
        ]

        # Apply this zone geography to all standard locations
        for table, lon_col, lat_col, location_prefix in standard_locations:
            output_col = f"{location_prefix}_{zone_name}"
            results[table] = add_zone_to_dataframe(
                results[table],
                shapefile,
                lon_col=lon_col,
                lat_col=lat_col,
                zone_col_name=output_col,
                zone_id_field=zone_id_field,
            )

    return results


# Set up custom steps dictionary ----------------------------------
processing_steps = [
    load_data,
    clean_2023_bats,
    custom_add_zone_ids,
    link_trips,
    detect_joint_trips,
    extract_tours,
    format_daysim,
    write_data,
]


# ---------------------------------------------------------------------
if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="BATS 2023 DaySim Processing Pipeline"
    )
    parser.add_argument(
        "--clear-cache",
        action="store_true",
        help="Clear the pipeline cache before running",
    )
    args = parser.parse_args()

    logger.info("Starting BATS 2023 DaySim Processing Pipeline")

    # Clear cache if requested
    cache_dir = Path(".cache")
    if args.clear_cache and cache_dir.exists():
        logger.info("Clearing pipeline cache at %s", cache_dir)
        shutil.rmtree(cache_dir)
        logger.info("Cache cleared")

    pipeline = Pipeline(
        config_path=CONFIG_PATH,
        steps=processing_steps,
        caching=True,
    )
    result = pipeline.run()

    logger.info("Pipeline finished successfully.")
