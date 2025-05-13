"""
Spatial join hh, person, & trip tables (from 00-preprocess dir) to SF CHAMP TAZs & MAZs

The results are saved in the parsed/01-"taz_spatial_join subdirectory,
with the following columns added:
    hh.csv: home_{maz, taz}
    person.csv: {work, school}_{maz, taz}
    trip.csv: {o, d}_{maz, taz}
"""

import argparse
import tomllib
from pathlib import Path

import geopandas as gpd
import pandas as pd


def taz_spatial_join(config):
    preprocess_dir = Path(config["00-preprocess"]["dir"])
    taz_spatial_join_dir = Path(config["01-taz_spatial_join"]["dir"])
    taz_spatial_join_dir.mkdir(exist_ok=True)

    maz = gpd.read_file(config["01-taz_spatial_join"]["maz_filepath"])[
        ["MAZID", "TAZ", "geometry"]
    ]
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
