"""
Daysim Pipeline - Step 0: Data Preprocessing

This script performs initial preprocessing of travel survey data by:
1. Converting raw survey CSV files to a standardized format
2. Adding calculated time fields to trip data (depart_time, arrive_time)
3. Adding person_id references to location data from trip table
4. Copying all files to the 00-preprocess directory for further processing

The preprocessing ensures data consistency and adds derived fields needed
for subsequent pipeline steps while maintaining backward compatibility.

Input: Raw CSV files from travel survey
Output: Preprocessed CSV files ready for spatial joining
"""

import argparse
import tomllib
from pathlib import Path
from shutil import copy2

import pandas as pd


def preprocess(config):
    """
    Main preprocessing function that processes all survey data files.
    
    Args:
        config (dict): Configuration dictionary containing file paths and names
                      from the TOML configuration file
    
    Returns:
        None: Outputs processed CSV files to the 00-preprocess directory
    """
    raw_dir = Path(config["raw"]["dir"])
    preprocess_dir = Path(config["00-preprocess"]["dir"])
    preprocess_dir.mkdir(exist_ok=True)

    trip = preprocess_trip(raw_dir, preprocess_dir, config["trip_filename"])
    _ = preprocess_location(raw_dir, preprocess_dir, config["location_filename"], trip)

    # copying the unchanged files too for ease of use / easy backward compatibility;
    # not sure for logical clarity if we should copy files that are not changed
    day_filename = config["day_filename"]
    hh_filename = config["hh_filename"]
    person_filename = config["person_filename"]
    vehicle_filename = config["vehicle_filename"]
    copy2(raw_dir / day_filename, preprocess_dir / day_filename)
    copy2(raw_dir / hh_filename, preprocess_dir / hh_filename)
    copy2(raw_dir / person_filename, preprocess_dir / person_filename)
    copy2(raw_dir / vehicle_filename, preprocess_dir / vehicle_filename)
    return


def preprocess_trip(raw_dir, preprocess_dir, trip_filename):
    """
    Process trip data by adding formatted time columns.
    
    Converts separate hour, minute, second columns into formatted time strings
    (HH:MM:SS format) for both departure and arrival times.
    
    Args:
        raw_dir (Path): Path to directory containing raw CSV files
        preprocess_dir (Path): Path to output directory for processed files
        trip_filename (str): Name of the trip CSV file
    
    Returns:
        pd.DataFrame: Processed trip dataframe with added time columns
    """
    trip = pd.read_csv(raw_dir / trip_filename)
    print("trip raw len:", len(trip))
    if "depart_seconds" in trip.columns:
        trip.rename(columns={"depart_seconds": "depart_second"}, inplace=True)
    trip["depart_time"] = trip.apply(
        lambda x: "{:02d}:{:02d}:{:02d}".format(
            x["depart_hour"], x["depart_minute"], x["depart_second"]
        ),
        axis=1,
    )
    trip["arrive_time"] = trip.apply(
        lambda x: "{:02d}:{:02d}:{:02d}".format(
            x["arrive_hour"], x["arrive_minute"], x["arrive_second"]
        ),
        axis=1,
    )
    print("trip preprocessed len:", len(trip))
    trip.to_csv(preprocess_dir / trip_filename, index=False)
    return trip


def preprocess_location(raw_dir, preprocess_dir, location_filename, trip):
    """
    Process location data by adding person_id references from trip data.
    
    Merges location data with trip data to add person_id for each location,
    enabling better linkage between person and location records.
    
    Args:
        raw_dir (Path): Path to directory containing raw CSV files
        preprocess_dir (Path): Path to output directory for processed files
        location_filename (str): Name of the location CSV file
        trip (pd.DataFrame): Processed trip dataframe containing person_id mappings
    
    Returns:
        pd.DataFrame: Location dataframe with added person_id column
    """
    location = pd.read_csv(raw_dir / location_filename)
    print("location raw len:", len(location))
    location = pd.merge(
        location,
        trip[["trip_id", "person_id"]],
        on="trip_id",
        how="left",
    )
    print("location preprocessed len:", len(location))
    location.to_csv(preprocess_dir / location_filename, index=False)
    return location


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config_filepath")
    args = parser.parse_args()
    with open(args.config_filepath, "rb") as f:
        config = tomllib.load(f)
    preprocess(config)
