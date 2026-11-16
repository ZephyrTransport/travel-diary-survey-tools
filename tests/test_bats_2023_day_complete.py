"""The bats_2023 cleaner's completeness rules: the 995 trip flag and children's days."""

import polars as pl
import pytest

from projects.bats_2023 import clean_bats_2023 as cleaner


def test_trip_flag_995_has_no_verdict():
    """1 is surveyed, 0 is not, and the vendor's NA is neither."""
    trips = pl.DataFrame({"survey_complete": [1, 0, 995]})
    out = trips.select(cleaner.trip_survey_verdict("survey_complete"))
    assert out["survey_complete"].to_list() == [True, False, None]


def _days() -> tuple[pl.DataFrame, pl.DataFrame]:
    """An adult the vendor rejected, and four children's days.

    Day 1: adult, vendor incomplete. Day 2: child with a proxy report. Day 3: child,
    copied trips all surveyed. Day 4: child, no trips, a no-travel reason. Day 5:
    child, one trip without a trip survey. Only day 2 is complete to the vendor.
    """
    person_days = pl.DataFrame(
        {
            "person_day_id": [1, 2, 3, 4, 5],
            "is_participant": [1, 0, 0, 0, 0],
            "is_complete": [0, 1, 0, 0, 0],
            "proxy_complete": [995, 1, 995, 995, 995],
            "num_trips": [2, 1, 2, 0, 1],
            "num_reasons_no_travel": [0, 0, 0, 1, 0],
        }
    )
    trips = pl.DataFrame(
        {
            "person_day_id": [1, 1, 2, 3, 3, 5],
            "survey_complete": [True, True, True, True, None, False],
        }
    )
    return person_days, trips


@pytest.mark.parametrize(
    ("setting", "expected"),
    [
        ("vendor", [False, True, False, False, False]),
        # The adult keeps the vendor's verdict; day 3's NA trip does not decide it.
        ("observed_trips", [False, True, True, True, False]),
    ],
)
def test_child_day_complete(setting, expected):
    """Adults always take the vendor's verdict; the setting decides children's days."""
    person_days, trips = _days()
    out = cleaner.day_survey_complete(person_days, trips, setting)
    assert out.sort("person_day_id")["survey_complete"].to_list() == expected


def test_unknown_setting_raises():
    """A misspelled setting must not quietly pick a rule."""
    person_days, trips = _days()
    with pytest.raises(ValueError, match="child_day_complete"):
        cleaner.day_survey_complete(person_days, trips, "observed")
