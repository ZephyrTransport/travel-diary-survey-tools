"""Runner script for the BATS 2023 DaySim processing pipeline."""

import argparse
import logging
import os
from pathlib import Path

import yaml
from clean_bats_2023 import clean_2023_bats
from dotenv import load_dotenv

from data_canon.models import (
    ctramp as ctramp_models,
)
from data_canon.models import (
    daysim as daysim_models,
)
from pipeline.decoration import Tables, step
from pipeline.pipeline import Pipeline
from processing import (
    add_existing_weights,
    add_zone_ids,
    cascade_usability,
    compute_weights,
    detect_habitual_locations,
    detect_joint_trips,
    extract_tours,
    format_ctramp,
    format_daysim,
    imputation,
    link_trips,
    load_data,
    write_data,
)

# Load environment variables from .env file
# Useful for CENSUS API keys or other sensitive information
# that should not be committed to version control.
# Make sure to create a .env file in the project root with the necessary variables.
load_dotenv()


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


# Optional: project-specific custom step functions
# You can define or import them here if needed
@step(requires={"households": set(), "persons": set(), "linked_trips": set()})
def custom_foo_bar(tables: Tables) -> dict:
    """A custom processing step that does something specific.

    Args:
        tables: The tables in ``requires``, each with the columns it lists.

    Returns:
        Dictionary of processed DataFrames
    """
    # Custom processing logic here
    logger.info("Running custom_foo_bar step")
    # For demonstration, just return the inputs unchanged
    return dict(tables)


# Set up custom steps dictionary ----------------------------------
processing_steps = [
    custom_foo_bar,
    load_data,
    clean_2023_bats,
    add_zone_ids,
    link_trips,
    detect_habitual_locations,
    detect_joint_trips,
    imputation,
    extract_tours,
    cascade_usability,
    format_ctramp,
    format_daysim,
    write_data,
    add_existing_weights,
    compute_weights,
]


new_models = {
    # Daysim models
    "households_daysim": daysim_models.HouseholdDaysimModel,
    "persons_daysim": daysim_models.PersonDaysimModel,
    "days_daysim": daysim_models.PersonDayDaysimModel,
    "linked_trips_daysim": daysim_models.LinkedTripDaysimModel,
    "tours_daysim": daysim_models.TourDaysimModel,
    # CT-RAMP models
    "households_ctramp": ctramp_models.HouseholdCTRAMPModel,
    "persons_ctramp": ctramp_models.PersonCTRAMPModel,
    "mandatory_locations_ctramp": ctramp_models.MandatoryLocationCTRAMPModel,
    "individual_tours_ctramp": ctramp_models.IndividualTourCTRAMPModel,
    "individual_trips_ctramp": ctramp_models.IndividualTripCTRAMPModel,
    "joint_tours_ctramp": ctramp_models.JointTourCTRAMPModel,
    "joint_trips_ctramp": ctramp_models.JointTripCTRAMPModel,
}

# ---------------------------------------------------------------------
if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="BATS 2023 DaySim Processing Pipeline")
    parser.add_argument(
        "--config",
        default=None,
        help="Path to the YAML config file (default: config.yaml next to this script)",
    )
    args = parser.parse_args()

    config_path = Path(args.config) if args.config else CONFIG_PATH

    logger.info("Starting BATS 2023 DaySim Processing Pipeline")
    logger.info("Using config: %s", config_path)

    pipeline = Pipeline(
        config_path=config_path,
        steps=processing_steps,
        data_models=new_models,
    )

    # Save a copy of the config used into the output directory
    output_dir = pipeline.config.get("output_dir")
    if output_dir:
        saved_config = Path(output_dir) / "pipeline_2023.yaml"
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        # The resolved config, not the file. Copying the source would record
        # "{{ MTC_DATA }}/Data/..." as the input used, which is a template
        # rather than a path and says nothing about what this run actually read.
        saved_config.write_text(yaml.safe_dump(pipeline.config, sort_keys=False), encoding="utf-8")
        logger.info("Saved config copy to %s", saved_config)

    result = pipeline.run()

    logger.info("Pipeline finished successfully.")
