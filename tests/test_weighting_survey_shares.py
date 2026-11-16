"""Whether each group has enough survey records to be weighted to its target."""

import polars as pl
import pytest

from processing.weighting.core.specs import ControlTotals
from processing.weighting.validation.survey_shares import (
    check_survey_shares,
    format_table,
    survey_share_table,
)


def _seed(size_1: list[int], males: list[int]) -> pl.DataFrame:
    """Ten households in area A, each with two persons."""
    n = len(size_1)
    return pl.DataFrame(
        {
            "hh_id": list(range(n)),
            "zone_group_id": ["A"] * n,
            "h_total": [1] * n,
            "p_total": [2] * n,
            "h_size__size_1": size_1,
            "h_size__size_2": [1 - s for s in size_1],
            "p_gender__male": males,
            "p_gender__female": [2 - m for m in males],
        }
    )


@pytest.fixture
def totals() -> ControlTotals:
    """Area A: 1,000 households (half of them one-person), 2,000 persons (half male)."""
    rows = [
        ("h_total", "total", 1000.0),
        ("p_total", "total", 2000.0),
        ("h_size", "size_1", 500.0),
        ("h_size", "size_2", 500.0),
        ("p_gender", "male", 1000.0),
        ("p_gender", "female", 1000.0),
        ("h_size", "size_3", 0.0),
    ]
    return ControlTotals(
        totals=pl.DataFrame(
            {
                "zone_group_id": ["A"] * len(rows),
                "control_name": [r[0] for r in rows],
                "category": [r[1] for r in rows],
                "target_total": [r[2] for r in rows],
            }
        ),
        pums_hh_count=0,
        pums_person_count=0,
        zone_group_ids=["A"],
    )


def _row(table: pl.DataFrame, target: str, group: str) -> dict:
    return table.filter((pl.col("target") == target) & (pl.col("group") == group)).row(
        0, named=True
    )


def test_ratio_is_survey_share_over_pums_share(totals):
    """1 of 10 households is one-person (10%) where the PUMS has 50%: ratio 0.2."""
    table = survey_share_table({"p": _seed([1] + [0] * 9, [1] * 10)}, totals)
    row = _row(table, "h_size", "size_1")
    assert (row["survey"], row["pums"]) == (1.0, 500.0)
    assert row["survey_share"] == pytest.approx(0.1)
    assert row["pums_share"] == pytest.approx(0.5)
    assert row["ratio"] == pytest.approx(0.2)


def test_person_groups_count_persons(totals):
    """Males are 10 of 20 survey persons, half, as in the PUMS: ratio 1.0."""
    table = survey_share_table({"p": _seed([1] * 5 + [0] * 5, [1] * 10)}, totals)
    row = _row(table, "p_gender", "male")
    assert (row["survey"], row["ratio"]) == (10.0, pytest.approx(1.0))


def test_each_group_takes_its_lowest_profile(totals):
    """One table for the run: the profile where the group is shortest."""
    seeds = {"loose": _seed([1] * 5 + [0] * 5, [1] * 10), "strict": _seed([1] + [0] * 9, [1] * 10)}
    table = survey_share_table(seeds, totals)
    assert _row(table, "h_size", "size_1")["ratio"] == pytest.approx(0.2)
    assert table.filter(pl.col("target") == "h_size").height == 2


def test_groups_without_a_target_and_the_totals_are_left_out(totals):
    """size_3 has no target; h_total and p_total are the denominators, not groups."""
    table = survey_share_table({"p": _seed([1] * 5 + [0] * 5, [1] * 10)}, totals)
    assert sorted(table["group"].to_list()) == ["female", "male", "size_1", "size_2"]


def test_a_group_below_the_limit_stops_the_run(totals):
    """The message names the group, its counts and both shares."""
    table = survey_share_table({"p": _seed([1] + [0] * 9, [1] * 10)}, totals)
    with pytest.raises(ValueError, match="1 group") as err:
        check_survey_shares(table, 0.25)
    assert "size_1" in str(err.value)
    assert "10.0%" in str(err.value)
    assert "min_survey_share_ratio" in str(err.value)


def test_nothing_below_the_limit_passes(totals):
    """Lowering the limit is how an analyst accepts the miss."""
    table = survey_share_table({"p": _seed([1] + [0] * 9, [1] * 10)}, totals)
    check_survey_shares(table, 0.15)


def test_the_text_table_lines_up(totals):
    """One line per group under one header, every line the same width."""
    table = survey_share_table({"p": _seed([1] * 5 + [0] * 5, [1] * 10)}, totals)
    lines = format_table(table).splitlines()
    assert len(lines) == table.height + 1
    assert len({len(line) for line in lines}) == 1
