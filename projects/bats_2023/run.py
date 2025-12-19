"""Runner script for the BATS 2023 DaySim processing pipeline."""

import logging
import os
from pathlib import Path

import geopandas as gpd
import polars as pl

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
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# Optional: project-specific custom step functions
# You can define or import them here if needed
@step()
def custom_add_taz_ids(
    households: pl.DataFrame,
    persons: pl.DataFrame,
    linked_trips: pl.DataFrame,
    taz_shapefile: gpd.GeoDataFrame,
    maz_shapefile: gpd.GeoDataFrame | None = None,
    taz_field_name: str = "TAZ1454",
    maz_field_name: str = "MAZ_ID",
) -> dict:
    """Custom step to add TAZ and MAZ IDs based on locations."""
    # Rename source field to TAZ_ID for consistency
    taz_shapefile = taz_shapefile.rename(columns={taz_field_name: "TAZ_ID"})

    # Rename MAZ field if MAZ shapefile provided
    if maz_shapefile is not None:
        maz_shapefile = maz_shapefile.rename(columns={maz_field_name: "MAZ_ID"})

    # Helper function to add zone ID based on lon/lat columns
    def add_zone_to_dataframe(
        df: pl.DataFrame,
        shp: gpd.GeoDataFrame,
        lon_col: str,
        lat_col: str,
        zone_col_name: str,
        zone_id_field: str = "TAZ_ID",
    ) -> pl.DataFrame:
        """Add TAZ/MAZ zone ID to dataframe based on lon/lat coordinates."""
        gdf = gpd.GeoDataFrame(
            df.to_pandas(),
            geometry=gpd.points_from_xy(
                df[lon_col].to_list(), df[lat_col].to_list()
            ),
            crs="EPSG:4326",
        )

        # Set index zone_id and geometry only for spatial join
        shp = shp.loc[:, [zone_id_field, "geometry"]].set_index(zone_id_field)

        # Spatial join to find zone containing each point
        gdf_joined = gpd.sjoin(gdf, shp, how="left", predicate="within")
        gdf_joined = gdf_joined.rename(columns={zone_id_field: zone_col_name})
        gdf_joined = gdf_joined.drop(columns="geometry")

        return pl.from_pandas(gdf_joined)

    # Add TAZ IDs to all dataframes
    taz_configs = [
        ("households", "home_lon", "home_lat", "home_taz"),
        ("persons", "work_lon", "work_lat", "work_taz"),
        ("persons", "school_lon", "school_lat", "school_taz"),
        ("linked_trips", "o_lon", "o_lat", "o_taz"),
        ("linked_trips", "d_lon", "d_lat", "d_taz"),
    ]
    results = {
        "households": households,
        "persons": persons,
        "linked_trips": linked_trips,
    }

    for df_name, lon_col, lat_col, taz_col_name in taz_configs:
        results[df_name] = add_zone_to_dataframe(
            results[df_name],
            taz_shapefile,
            lon_col=lon_col,
            lat_col=lat_col,
            zone_col_name=taz_col_name,
            zone_id_field="TAZ_ID",
        )

    # Add MAZ IDs if MAZ shapefile provided, otherwise spoof from TAZ
    if maz_shapefile is not None:
        maz_configs = [
            ("households", "home_lon", "home_lat", "home_maz"),
            ("persons", "work_lon", "work_lat", "work_maz"),
            ("persons", "school_lon", "school_lat", "school_maz"),
            ("linked_trips", "o_lon", "o_lat", "o_maz"),
            ("linked_trips", "d_lon", "d_lat", "d_maz"),
        ]

        for df_name, lon_col, lat_col, maz_col_name in maz_configs:
            results[df_name] = add_zone_to_dataframe(
                results[df_name],
                maz_shapefile,
                lon_col=lon_col,
                lat_col=lat_col,
                zone_col_name=maz_col_name,
                zone_id_field="MAZ_ID",
            )
    else:
        # Spoof MAZ from TAZ if no MAZ shapefile provided
        results["households"] = results["households"].with_columns(
            pl.col("home_taz").alias("home_maz")
        )
        results["persons"] = results["persons"].with_columns(
            pl.col("work_taz").alias("work_maz"),
            pl.col("school_taz").alias("school_maz"),
        )
        results["linked_trips"] = results["linked_trips"].with_columns(
            pl.col("o_taz").alias("o_maz"),
            pl.col("d_taz").alias("d_maz"),
        )

    return results


# Set up custom steps dictionary ----------------------------------
processing_steps = [
    load_data,
    clean_2023_bats,
    custom_add_taz_ids,
    link_trips,
    detect_joint_trips,
    extract_tours,
    format_daysim,
    write_data,
]


# ---------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("Starting BATS 2023 DaySim Processing Pipeline")

    pipeline = Pipeline(
        config_path=CONFIG_PATH,
        steps=processing_steps,
        caching=True,
    )
    result = pipeline.run()

    logger.info("Pipeline finished successfully.")
