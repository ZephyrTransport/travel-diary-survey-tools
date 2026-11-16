"""The bats_2023 cleaner's one race column, from the vendor's race boxes."""

import polars as pl

from data_canon.codebook.persons import Race
from projects.bats_2023 import clean_bats_2023 as cleaner

_BOXES = ["race_1", "race_2", "race_3", "race_4", "race_5", "race_997"]


def _race(ticked: list[list[str]]) -> list[int | None]:
    """The race of one person per entry, each ticking the named boxes."""
    persons = pl.DataFrame({box: [int(box in t) for t in ticked] for box in _BOXES})
    return persons.select(cleaner.race_from_boxes())["race"].to_list()


def test_one_box_is_that_race():
    """A single box is the race it names."""
    assert _race([["race_1"], ["race_3"], ["race_5"], ["race_997"]]) == [
        Race.AFAM.value,
        Race.ASIAN.value,
        Race.WHITE.value,
        Race.OTHER.value,
    ]


def test_two_or_more_boxes_is_multiple_races():
    """Not the first box ticked: the census counts these as two or more races."""
    assert (
        _race([["race_1", "race_5"], ["race_3", "race_4"], ["race_5", "race_997"]])
        == [Race.MULTI.value] * 3
    )


def test_no_box_is_null():
    """No box is left for imputation to fill."""
    assert _race([[]]) == [None]
