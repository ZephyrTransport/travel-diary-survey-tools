"""The bats_2019 cleaner's exclusions, which follow the vendor's weekday weights."""

import polars as pl

from projects.bats_2019 import clean_bats_2019 as cleaner


def _exclude(subregions: list[int | None], statuses: list[int | None]):
    households = pl.DataFrame(
        {"hh_id": list(range(1, len(subregions) + 1)), "subregion": subregions},
        schema_overrides={"subregion": pl.Int64},
    )
    person_days = pl.DataFrame(
        {"person_day_id": list(range(1, len(statuses) + 1)), "day_status": statuses},
        schema_overrides={"day_status": pl.Int64},
    )
    return cleaner.exclude_as_vendor_weekday_weights(households, person_days)


def test_a_household_with_no_subregion_is_excluded():
    """The vendor weighted no household without a subregion."""
    households, _ = _exclude([3, None], [9])
    assert households["exclusion_reason"].to_list() == [None, "no subregion"]


def test_a_day_below_status_9_is_excluded():
    """The vendor's weekday weights take only days of status 9 or 10."""
    _, person_days = _exclude([3], [7, 8, 9, 10])
    assert person_days["exclusion_reason"].to_list() == [
        "day_status below 9",
        "day_status below 9",
        None,
        None,
    ]


def test_a_day_with_no_status_is_not_excluded():
    """A day with no status has no record behind it; it is already incomplete."""
    _, person_days = _exclude([3], [None])
    assert person_days["exclusion_reason"].to_list() == [None]
