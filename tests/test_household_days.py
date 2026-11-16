"""The household-day table, built from person-days."""

from datetime import date

import polars as pl
import pytest

from utils.create_ids import create_household_days


def _days(day_num: list[int], travel_date: list[date]) -> pl.DataFrame:
    """Two members of household 7 on the given day numbers and dates."""
    return pl.DataFrame(
        {
            "person_day_id": [70101, 70102, 70201, 70202][: len(day_num)],
            "hh_id": [7] * len(day_num),
            "day_num": day_num,
            "travel_date": travel_date,
        }
    )


D1, D2 = date(2023, 5, 2), date(2023, 5, 3)


def test_one_row_per_household_date():
    """Two members over two dates make two household-days, each member's day on one."""
    person_days, household_days = create_household_days(_days([1, 2, 1, 2], [D1, D2, D1, D2]))
    assert household_days.to_dicts() == [
        {"hh_day_id": 701, "hh_id": 7, "travel_date": D1},
        {"hh_day_id": 702, "hh_id": 7, "travel_date": D2},
    ]
    assert person_days["hh_day_id"].to_list() == [701, 702, 701, 702]


@pytest.mark.parametrize(
    ("day_num", "travel_date"),
    [
        pytest.param([1, 2, 1, 1], [D1, D2, D1, D2], id="one_date_two_numbers"),
        pytest.param([1, 2, 1, 2], [D1, D2, D1, D1], id="one_number_two_dates"),
    ],
)
def test_numbers_and_dates_must_be_one_to_one(day_num, travel_date):
    """Otherwise the encoding would merge or split a household's dates."""
    with pytest.raises(ValueError, match="cannot name household-days"):
        create_household_days(_days(day_num, travel_date))
