"""
copy all CSV files from raw_dir to 00-preprocess dir with minor changes:
trip: add columns: depart_time, arrive_time
location: add person_id from trip table
"""

import argparse
import tomllib
from pathlib import Path
from shutil import copy2

import pandas as pd


def preprocess(config):
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
