"""The bats_2019 cleaner's usual work mode, from a person's own work trips."""

import polars as pl

from projects.bats_2019 import clean_bats_2019 as cleaner

CAR, TRANSIT, WALK = 8, 12, 1


def _trips(modes_by_person: dict[int, list[int]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "person_id": [p for p, modes in modes_by_person.items() for _ in modes],
            "mode_type": [m for modes in modes_by_person.values() for m in modes],
        }
    )


def _modes(trips: pl.DataFrame) -> dict[int, int]:
    return dict(cleaner.most_common_mode(trips).sort("person_id").iter_rows())


def test_the_most_common_mode_wins():
    """More car trips than transit: car."""
    assert _modes(_trips({1: [CAR, CAR, TRANSIT]})) == {1: CAR}


def test_a_tie_goes_to_the_lowest_mode_code():
    """Two car and two transit trips: the lower code wins, not the first row."""
    assert _modes(_trips({1: [TRANSIT, TRANSIT, CAR, CAR], 2: [CAR, WALK]})) == {1: CAR, 2: WALK}


def test_row_order_does_not_change_the_answer():
    """Shuffling the rows does not change the winner of a tie."""
    trips = _trips({1: [TRANSIT, CAR, TRANSIT, CAR], 2: [WALK, CAR], 3: [CAR, TRANSIT, WALK]})
    expected = _modes(trips)
    for seed in range(20):
        assert _modes(trips.sample(fraction=1.0, shuffle=True, seed=seed)) == expected
