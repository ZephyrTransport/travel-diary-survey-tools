"""Runner script for the BATS 2019 DaySim processing pipeline."""

import argparse
import logging
import os
from pathlib import Path

import polars as pl
from clean_bats_2019 import clean_2019_bats

from pipeline.decoration import step
from pipeline.pipeline import Pipeline
from processing import (
    add_existing_weights,
    add_zone_ids,
    compute_weights,
    detect_joint_trips,
    extract_tours,
    format_daysim,
    link_trips,
    load_data,
    write_data,
)

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
@step()
def custom_foo_bar(
    households: pl.DataFrame,
    persons: pl.DataFrame,
    linked_trips: pl.DataFrame,
) -> dict:
    """A custom processing step that does something specific.

    Args:
        households: Households DataFrame
        persons: Persons DataFrame
        linked_trips: Linked trips DataFrame

    Returns:
        Dictionary of processed DataFrames
    """
    # Custom processing logic here
    logger.info("Running custom_foo_bar step")
    # For demonstration, just return the inputs unchanged
    return {
        "households": households,
        "persons": persons,
        "linked_trips": linked_trips,
    }


# Set up custom steps dictionary ----------------------------------
processing_steps = [
    custom_foo_bar,
    load_data,
    clean_2019_bats,
    add_zone_ids,
    link_trips,
    detect_joint_trips,
    extract_tours,
    format_daysim,
    write_data,
    add_existing_weights,
    compute_weights,
]


# ---------------------------------------------------------------------
if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="BATS 2019 DaySim Processing Pipeline")
    parser.add_argument(
        "--clear-cache",
        action="store_true",
        help="Clear the pipeline cache before running",
    )
    args = parser.parse_args()

    logger.info("Starting BATS 2019 DaySim Processing Pipeline")

    cache_dir = Path(".cache/2019")
    pipeline = Pipeline(
        config_path=CONFIG_PATH,
        steps=processing_steps,
        caching=cache_dir,
        log_file_mode="w" if args.clear_cache else "a",
    )

    # Clear cache if requested
    if args.clear_cache and pipeline.cache:
        pipeline.cache.clear()
        logger.info("Cleared pipeline cache at %s", cache_dir)

    result = pipeline.run()

    logger.info("Pipeline finished successfully.")
