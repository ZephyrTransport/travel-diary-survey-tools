"""
Daysim Pipeline - Step 1: Spatial Join to TAZ/MAZ Geography

This script performs spatial joins between survey data and transportation
analysis geography (Traffic Analysis Zones - TAZ and Micro Analysis Zones - MAZ).

The script supports multiple agency models:
- SFCTA CHAMP: Uses both TAZ and MAZ zones
- MTC TM1: Uses only TAZ (MAZID set equal to TAZ for downstream compatibility)

Processing steps:
1. Load preprocessed household, person, and trip data
2. Load geographic zone files (TAZ/MAZ) for the specified agency model
3. Perform spatial joins using nearest neighbor with distance buffer
4. Add zone identifiers to each data type:
   - Households: home_taz, home_maz
   - Persons: work_taz, work_maz, school_taz, school_maz
   - Trips: o_taz, o_maz (origin), d_taz, d_maz (destination)

Input: Preprocessed CSV files from step 00
Output: Spatially joined CSV files with TAZ/MAZ identifiers
"""

import argparse
import tomllib
from pathlib import Path

import geopandas as gpd
import pandas as pd


def taz_spatial_join(config):
    """
    Perform spatial joins between survey data and transportation analysis zones.
    
    Args:
        config (dict): Configuration dictionary containing:
                      - File paths and names for input/output directories
                      - Agency model specification (SFCTA_CHAMP or MTC_TM1)
                      - Zone file paths for TAZ/MAZ geography
    
    Returns:
        None: Outputs spatially joined CSV files to 01-taz_spatial_join directory
    """
    preprocess_dir = Path(config["00-preprocess"]["dir"])
    taz_spatial_join_dir = Path(config["01-taz_spatial_join"]["dir"])
    taz_spatial_join_dir.mkdir(exist_ok=True)

    agency_model = config["model"]["agency_model"]

    if agency_model == "SFCTA_CHAMP":
        # SFCTA's CHAMP model has both TAZ and MAZ
        maz = gpd.read_file(config["01-taz_spatial_join"]["maz_filepath"])[
            ["MAZID", "TAZ", "geometry"]
        ]
    elif agency_model == "MTC_TM1":
        # MTC's TM1 only has TAZ but this proces expects both TAZ and MAZ (so MAZID is set to be the same value as TAZ to satisfy downstream code) 
        maz = gpd.read_file(config["01-taz_spatial_join"]["maz_filepath"])[
            ["TAZ1454", "geometry"]
        ].rename(columns={"TAZ1454": "TAZ"}).assign(MAZID=lambda df: df["TAZ"])
    else:
        raise ValueError(f"Unsupported agency_model: {agency_model}")    
    
    hh = pd.read_csv(preprocess_dir / config["hh_filename"])
    person = pd.read_csv(preprocess_dir / config["person_filename"])
    trip = pd.read_csv(preprocess_dir / config["trip_filename"])

    hh_taz_join = sjoin_maz(hh, maz, "hh_id", "home")
    person_taz_join = sjoin_maz(
        sjoin_maz(person, maz, "person_id", "work"), maz, "person_id", "school"
    )
    trip_taz_join = sjoin_maz(sjoin_maz(trip, maz, "trip_id", "o"), maz, "trip_id", "d")
    hh_taz_join.to_csv(taz_spatial_join_dir / config["hh_filename"])
    person_taz_join.to_csv(taz_spatial_join_dir / config["person_filename"])
    trip_taz_join.to_csv(taz_spatial_join_dir / config["trip_filename"])


def sjoin_maz(df: pd.DataFrame, maz: gpd.GeoDataFrame, id_col: str, var_prefix: str):
    """
    Perform spatial join between survey data points and MAZ/TAZ geography.
    
    Uses nearest neighbor spatial join with distance buffer to handle gaps
    in zone coverage. Converts coordinates to projected CRS for accurate
    distance calculations.
    
    Args:
        df (pd.DataFrame): Survey data with lat/lon coordinates
        maz (gpd.GeoDataFrame): Zone geography with MAZID and TAZ columns
        id_col (str): Column name for unique identifier in survey data
        var_prefix (str): Prefix for location type (e.g., 'home', 'work', 'o', 'd')
    
    Returns:
        pd.DataFrame: Survey data with added zone identifiers:
                     {prefix}_maz and {prefix}_taz columns
    """
    survey_crs = "EPSG:4326"
    return (
        gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(
                df[f"{var_prefix}_lon"], df[f"{var_prefix}_lat"]
            ),
            crs=survey_crs,
        )
        .to_crs(maz.crs)  # since sjoin_nearest requires a projected CRS
        # can't just use sjoin(predicate="within"), because the MAZs in the MAZ GIS file
        # aren't contiguous; there's many gaps between MAZs.
        # Q:\GIS\Model\MAZ\MAZ40051.* is in a ft CRS, so this sets max_distance = 1000ft,
        # preventing locations outside the Bay Area from being associated with MAZ/TAZs.
        # HOTFIX Further increased buffer from 1000 to 2000 ft, because a development
        # near Fremont/Newark falls outside of current MAZs.
        .sjoin_nearest(maz, how="left", max_distance=2000)
        # sjoin_nearest gives all matches if they're equidistant, so we just
        # randomly select the first of these equidistant MAZ/TAZs to keep
        .drop_duplicates(subset=id_col, keep="first")
        # column index_right was generated from the sjoin
        .drop(columns=["geometry", "index_right"])
        .astype({"MAZID": "Int32", "TAZ": "Int32"})  # nullable ints
        .rename(columns={"MAZID": f"{var_prefix}_maz", "TAZ": f"{var_prefix}_taz"})
        # casting to int doesn't work since there's NaNs:
        # .astype({f"{var_prefix}_maz": int, f"{var_prefix}_taz": int})
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config_filepath")
    args = parser.parse_args()
    with open(args.config_filepath, "rb") as f:
        config = tomllib.load(f)
    taz_spatial_join(config)
