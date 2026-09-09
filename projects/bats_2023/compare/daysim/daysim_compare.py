"""Compare old Daysim results to the new pipeline results."""

import logging
import os
from pathlib import Path

from helpers import (
    compare_columns,
    compare_row_counts,
    load_legacy_data,
    load_new_pipeline_data,
    print_summary_statistics,
)
from household_comparison import (
    compare_household_diaries,
    display_household_detail,
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
)

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

# Legacy Daysim output directory
LEGACY_DIR = Path(
    "M:/Data/HomeInterview/Bay Area Travel Study 2023/"
    "Data/Processed/test/03b-assign_day/wt-wkday_3day"
)

# New pipeline config
CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"

# Number of households to sample for detailed comparison
NUM_SAMPLE_HOUSEHOLDS = 1

# ---------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------


if __name__ == "__main__":
    # For MTC network drives that seem to keep unmapping
    # within python VM sessions. Check if network drives are
    # mapped; if not, map them
    drives = {
        "M:": r"\\models.ad.mtc.ca.gov\data\models",
        "X:": r"\\model3-a\Model3A-Share",
    }

    for drive, path in drives.items():
        if not Path(drive).exists():
            logger.info("Mapping network drive %s to %s", drive, path)
            os.system(f"net use {drive} {path}")  # noqa: S605

    # Load data from both sources
    legacy = load_legacy_data(LEGACY_DIR)
    # Cache is in repository root directory
    cache_dir = Path(__file__).parent.parent.parent.parent / ".cache"
    new = load_new_pipeline_data(CONFIG_PATH, cache_dir=cache_dir)

    # Compare row counts
    compare_row_counts(legacy, new)

    # Compare columns
    compare_columns(legacy, new)

    # Sample a few households that exist in both datasets
    # Find households that exist in both datasets
    legacy_hhnos = set(legacy["hh"]["hhno"].unique().to_list())
    new_hhnos = set(new["hh"]["hhno"].unique().to_list())
    common_hhnos = sorted(legacy_hhnos & new_hhnos)
    pct_overlap = len(common_hhnos) / len(legacy_hhnos) * 100

    msg = (
        f"\n{'=' * 80}\n"
        "SAMPLING HOUSEHOLDS FOR DETAILED COMPARISON\n"
        f"{'=' * 80}\n"
        f"Total households in legacy data: {len(legacy_hhnos):,}\n"
        f"Total households in new data:    {len(new_hhnos):,}\n"
        f"Percent overlap:                 {pct_overlap:.2f}%\n"
    )
    logger.info(msg)

    # Check common households for mismatches
    failures, stats = compare_household_diaries(common_hhnos, legacy, new, sample_pct=10.0)

    # Display detailed comparison for sample of failures
    if failures:
        sep = "=" * 80
        num_to_display = min(NUM_SAMPLE_HOUSEHOLDS, len(failures))
        logger.info(
            "\n%s\nDETAILED COMPARISON OF FAILED HOUSEHOLDS (showing %d of %d)\n%s",
            sep,
            num_to_display,
            len(failures),
            sep,
        )
        for failure in failures[:num_to_display]:
            display_household_detail(failure["hhno"], legacy, new)

    # Print summary statistics
    print_summary_statistics(legacy, new)
