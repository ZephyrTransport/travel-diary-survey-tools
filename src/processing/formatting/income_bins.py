"""Dollar midpoints of the broad income bins, for the model formatters."""

from data_canon.codebook.households import IncomeBroad
from utils.helpers import get_income_midpoint


def income_bin_midpoints() -> dict[int, int]:
    """Each ``IncomeBroad`` code's midpoint in dollars; refusals and missing have none."""
    return {
        bin_cat.value: int(get_income_midpoint(bin_cat))
        for bin_cat in IncomeBroad
        if "Prefer not to answer" not in bin_cat.label and "Missing" not in bin_cat.label
    }
