"""Fixtures shared across the unit tests.

pytest finds this file on its own and makes every fixture here available to
every test module under ``tests/``, so a module asks for one by naming it as an
argument, with no import.

Only fixtures that were defined identically in several modules live here. A
helper that shares a name with another module's but builds a different frame
stays in its own module.
"""

import polars as pl
import pytest

from processing.formatting.ctramp.ctramp_config import CTRAMPConfig
from processing.weighting.controls.registry import CONTROLS


@pytest.fixture
def standard_config() -> CTRAMPConfig:
    """The CT-RAMP config every formatter test runs under."""
    return CTRAMPConfig(
        usability_profile="test",
        income_low_threshold=30000,  # $30k ($2000, MTC)
        income_med_threshold=60000,  # $60k ($2000, MTC)
        income_high_threshold=100000,  # $100k ($2000, MTC)
        income_survey_year_to_ctramp_year=0.5319148936,
        taz_field="taz",
        age_adult=4,  # AGE_18_TO_24 = category 4 (18+ are adults)
        gender_default_for_missing="f",
    )


@pytest.fixture
def pums_households() -> pl.DataFrame:
    """Minimal PUMS household DataFrame."""
    return pl.DataFrame(
        {
            "SERIALNO": ["HH1", "HH2", "HH3"],
            "PUMA": ["00100", "00100", "00200"],
            "ST": ["06", "06", "06"],
            "WGTP": [100.0, 200.0, 150.0],
            "NP": [2, 4, 1],
            "HINCP": [55_000.0, 120_000.0, 15_000.0],
            "VEH": [1, 2, 0],
            "NOC": [0, 2, 0],
            "TYPEHUGQ": [1, 1, 1],
            "ADJINC": [1.019518, 1.019518, 1.019518],
        }
    )


@pytest.fixture
def pums_persons() -> pl.DataFrame:
    """Minimal PUMS person DataFrame matching the household fixture."""
    return pl.DataFrame(
        {
            "SERIALNO": ["HH1", "HH1", "HH2", "HH2", "HH2", "HH2", "HH3"],
            "SPORDER": [1, 2, 1, 2, 3, 4, 1],
            "PUMA": ["00100", "00100", "00100", "00100", "00100", "00100", "00200"],
            "ST": ["06", "06", "06", "06", "06", "06", "06"],
            # Not the household weight: the ACS weights persons on their own, so a
            # target summed on the wrong one comes out different.
            "PWGTP": [90.0, 130.0, 180.0, 230.0, 220.0, 190.0, 160.0],
            "AGEP": [35, 33, 40, 38, 10, 7, 65],
            "SEX": [1, 2, 1, 2, 1, 2, 2],
            "ESR": [1, 1, 1, 6, 0, 0, 6],  # 0 = under 16
            "WKHP": [40, 35, 40, 0, 0, 0, 0],
            "JWTRNS": [1, 2, 11, None, None, None, None],
            "JWRIP": [1, None, None, None, None, None, None],
            "SCHG": [None, None, None, None, 7, 5, None],
            "SCHL": [21, 22, 24, 16, 5, 3, 17],
            "RAC1P": [1, 6, 2, 1, 1, 1, 9],
            "HISP": [1, 1, 1, 3, 1, 1, 2],
        }
    )


@pytest.fixture
def _clean_registry():
    """Remove cross-tab controls a test registered, once it finishes.

    Registration mutates the module-level ``CONTROLS`` dict, so without this a
    cross-tab from one test would still be registered in the next. Not autouse:
    a module that registers controls opts in, with a module-level
    ``pytestmark`` or per test.
    """
    before = set(CONTROLS.keys())
    yield
    for name in list(CONTROLS.keys()):
        if name not in before:
            del CONTROLS[name]
