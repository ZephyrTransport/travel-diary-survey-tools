"""Re-run the BATS 2023 pipeline using the vendor (survey-delivered) weights.

The legacy SFCTA pipeline consumes the vendor weights shipped on the raw survey
files (``hh_weight``, ``person_weight``, ``day_weight``, ``trip_weight``) and
transforms them inline. To compare weighted aggregates apples-to-apples, this
script re-runs the new pipeline with those same vendor weights attached via the
``add_existing_weights`` step instead of computing fresh raked weights with
``compute_weights``. Everything else is identical to projects/bats_2023.

The config is derived in memory from projects/bats_2023/config.yaml — nothing
under projects/ is modified. Outputs land in scripts/validate_sfcta/.cache/
vendor_output/ and are read by validation_report.qmd (weighted "Basis A").

Usage (from the repo root):
    uv run python scripts/validate_sfcta/run_vendor_weights.py

Steps up through extract_tours reuse the existing pipeline cache at
projects/bats_2023/.cache/bats_2023, so the run skips the expensive
linking/tour work and does no PUMS/Census work at all.
"""

import logging
import os
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
PROJECT_DIR = REPO / "projects" / "bats_2023"
CACHE_DIR = PROJECT_DIR / ".cache" / "bats_2023"

VENDOR_OUT = HERE / ".cache" / "vendor_output"
VENDOR_CONFIG = HERE / ".cache" / "config_vendor_weights.yaml"

# The project runner (clean_bats_2023) lives outside the installed package
sys.path.insert(0, str(PROJECT_DIR))

load_dotenv(REPO / ".env")

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")

# Survey inputs are read from the M: network drive, same as the project runner
if not Path("M:/").exists():
    logger.info("Mapping network drive M:")
    os.system(r"net use M: \\models.ad.mtc.ca.gov\data\models")  # noqa: S605, S607

from clean_bats_2023 import clean_2023_bats  # noqa: E402

from data_canon.models import daysim as daysim_models  # noqa: E402
from pipeline.pipeline import Pipeline  # noqa: E402
from processing import (  # noqa: E402
    add_existing_weights,
    add_zone_ids,
    detect_joint_trips,
    extract_tours,
    format_daysim,
    imputation,
    link_trips,
    load_data,
    write_data,
)

# Vendor weight columns live directly on the raw survey CSVs. The legacy test
# run used the unsuffixed columns (its pdexpfac reconstructs exactly as
# person_weight / n_complete_TueThu_days), not the *_rmove_only variants.
ADD_EXISTING_WEIGHTS_STEP = {
    "name": "add_existing_weights",
    "validate_input": False,
    "cache": False,
    "params": {
        "derive_missing_weights": True,  # fills linked_trip/joint_trip/tour weights
        "weights": {
            "household_weights": {
                "weight_path": "{{ survey_dir }}/hh.csv",
                "weight_col": "hh_weight",
            },
            "person_weights": {
                "weight_path": "{{ survey_dir }}/person.csv",
                "weight_col": "person_weight",
            },
            "day_weights": {
                "weight_path": "{{ survey_dir }}/day.csv",
                "weight_col": "day_weight",
            },
            "unlinked_trip_weights": {
                "weight_path": "{{ survey_dir }}/trip.csv",
                "weight_id_col": "trip_id",
                "weight_col": "trip_weight",
            },
        },
    },
}


def build_vendor_config() -> Path:
    """Derive the vendor-weights config from the project config, in memory."""
    with (PROJECT_DIR / "config.yaml").open() as f:
        config = yaml.safe_load(f)

    config["output_dir"] = VENDOR_OUT.as_posix()
    config["log_file"] = "{{ output_dir }}/pipeline_vendor_weights.log"

    steps = config["steps"]

    # Swap compute_weights -> add_existing_weights, same position in the flow
    idx = next(i for i, s in enumerate(steps) if s["name"] == "compute_weights")
    steps[idx] = ADD_EXISTING_WEIGHTS_STEP

    # CTRAMP outputs are not part of the SFCTA comparison
    config["steps"] = [s for s in steps if s["name"] != "format_ctramp"]

    write_step = next(s for s in config["steps"] if s["name"] == "write_data")
    write_step["params"]["output_paths"] = {
        key: path
        for key, path in write_step["params"]["output_paths"].items()
        if not key.endswith("_ctramp")
    }

    VENDOR_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    with VENDOR_CONFIG.open("w") as f:
        yaml.safe_dump(config, f, sort_keys=False)
    return VENDOR_CONFIG


if __name__ == "__main__":
    config_path = build_vendor_config()
    VENDOR_OUT.mkdir(parents=True, exist_ok=True)
    logger.info("Vendor-weights config written to %s", config_path)

    pipeline = Pipeline(
        config_path=config_path,
        steps=[
            load_data,
            clean_2023_bats,
            imputation,
            link_trips,
            detect_joint_trips,
            extract_tours,
            add_existing_weights,
            add_zone_ids,
            format_daysim,
            write_data,
        ],
        caching=CACHE_DIR,
        data_models={
            "households_daysim": daysim_models.HouseholdDaysimModel,
            "persons_daysim": daysim_models.PersonDaysimModel,
            "days_daysim": daysim_models.PersonDayDaysimModel,
            "linked_trips_daysim": daysim_models.LinkedTripDaysimModel,
            "tours_daysim": daysim_models.TourDaysimModel,
        },
    )
    pipeline.run()
    logger.info("Vendor-weights pipeline finished. Outputs: %s", VENDOR_OUT)
