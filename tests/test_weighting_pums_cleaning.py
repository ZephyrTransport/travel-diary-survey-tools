"""The PUMS rules: which records count, in which dollars, on which weights."""

import polars as pl
import pytest

from processing.weighting.core.specs import HouseholdIncome, PumsWeights
from processing.weighting.data_prep.pums_cleaning import (
    adjust_income,
    apply_pums_weights,
    clean_pums,
    drop_group_quarters,
    drop_vacant_homes,
    exclude_nonrelative_income,
)

ROOMMATE, OTHER_NONRELATIVE, SPOUSE, UNMARRIED_PARTNER, FOSTER_CHILD = 34, 36, 21, 22, 35


@pytest.fixture
def households() -> pl.DataFrame:
    """Two occupied homes, one vacant, one institutional and one noninstitutional GQ."""
    return pl.DataFrame(
        {
            "SERIALNO": ["H1", "H2", "VAC", "GQI", "GQN"],
            "NP": [2, 1, 0, 1, 1],
            "TYPEHUGQ": [1, 1, 1, 2, 3],
            "WGTP": [100.0, 50.0, 80.0, 0.0, 0.0],
            "WGTP1": [110.0, 40.0, 70.0, 0.0, 0.0],
            "HINCP": [50_000.0, 20_000.0, -60_000.0, -60_000.0, -60_000.0],
            "ADJINC": [1.02, 1.01, 1.02, 1.02, 1.02],
        }
    )


@pytest.fixture
def persons() -> pl.DataFrame:
    """H1 has two members, H2 one; each GQ record is one person."""
    return pl.DataFrame(
        {
            "SERIALNO": ["H1", "H1", "H2", "GQI", "GQN"],
            "SPORDER": [1, 2, 1, 1, 1],
            "PWGTP": [90.0, 130.0, 60.0, 40.0, 30.0],
            "PWGTP1": [80.0, 120.0, 70.0, 50.0, 20.0],
        }
    )


def test_vacant_homes_are_dropped(households, persons):
    """A vacant home is not a household; nobody lives there to drop."""
    hh, per = drop_vacant_homes(households, persons)
    assert "VAC" not in hh["SERIALNO"].to_list()
    assert per.equals(persons)


def test_group_quarters_are_dropped_with_their_residents(households, persons):
    """Institutional and noninstitutional alike, for now."""
    hh, per = drop_group_quarters(households, persons)
    assert sorted(hh["SERIALNO"].to_list()) == ["H1", "H2", "VAC"]
    assert sorted(per["SERIALNO"].unique().to_list()) == ["H1", "H2"]


def test_income_is_in_the_pums_years_dollars(households):
    """HINCP x ADJINC, the factor as the API sends it."""
    hh = adjust_income(households.head(2))
    assert hh["HINCP"].to_list() == pytest.approx([51_000.0, 20_200.0])


@pytest.mark.parametrize(
    "adjinc",
    [
        pytest.param([1.02, None], id="blank"),
        pytest.param([1_019_518.0, 1_019_518.0], id="a_downloaded_files_integer_form"),
    ],
)
def test_income_stops_on_a_factor_that_is_not_one(households, adjinc):
    """A blank, or a factor a million times too big, is never applied."""
    hh = households.head(2).with_columns(pl.Series("ADJINC", adjinc, dtype=pl.Float64))
    with pytest.raises(ValueError, match="ADJINC is not an income factor"):
        adjust_income(hh)


@pytest.fixture
def occupied(households, persons) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Only H1 and H2, as the weights rule sees them after the drops."""
    return households.filter(pl.col("SERIALNO").is_in(["H1", "H2"])), persons.filter(
        pl.col("SERIALNO").is_in(["H1", "H2"])
    )


def test_household_option_gives_persons_their_households_weights(occupied):
    """Replicates follow the weight."""
    hh, per = apply_pums_weights(*occupied, PumsWeights.HOUSEHOLD)
    per = per.sort("SERIALNO", "SPORDER")
    assert per["PWGTP"].to_list() == [100.0, 100.0, 50.0]
    assert per["PWGTP1"].to_list() == [110.0, 110.0, 40.0]
    assert hh.equals(occupied[0])


def test_person_option_gives_households_their_members_mean(occupied):
    """H1: (90 + 130) / 2 = 110, replicate (80 + 120) / 2 = 100; H2 its one member."""
    hh, per = apply_pums_weights(*occupied, PumsWeights.PERSON)
    hh = hh.sort("SERIALNO")
    assert hh["WGTP"].to_list() == [110.0, 60.0]
    assert hh["WGTP1"].to_list() == [100.0, 70.0]
    assert per.equals(occupied[1])


def test_independent_option_changes_nothing(occupied):
    """Each level keeps its own weight."""
    hh, per = apply_pums_weights(*occupied, PumsWeights.INDEPENDENT)
    assert hh.equals(occupied[0])
    assert per.equals(occupied[1])


def test_replicates_must_pair_up(occupied):
    """A replicate on one side only cannot be carried across."""
    hh, per = occupied
    with pytest.raises(ValueError, match="replicate weights differ"):
        apply_pums_weights(hh, per.drop("PWGTP1"), PumsWeights.HOUSEHOLD)


def _household(hincp: float, members: list[tuple[int, float]]) -> tuple[pl.DataFrame, pl.DataFrame]:
    """One household with ADJINC 1.1, already adjusted; *members* are (RELSHIPP, PINCP)."""
    hh = pl.DataFrame({"SERIALNO": ["H"], "HINCP": [hincp], "ADJINC": [1.1]})
    per = pl.DataFrame(
        {
            "SERIALNO": ["H"] * len(members),
            "RELSHIPP": [r for r, _ in members],
            "PINCP": [float(p) for _, p in members],
        }
    )
    return hh, per


@pytest.mark.parametrize(
    ("hincp", "members", "expected"),
    [
        pytest.param(110_000, [(20, 60_000), (ROOMMATE, 40_000)], 66_000, id="roommate_removed"),
        pytest.param(
            110_000, [(20, 60_000), (OTHER_NONRELATIVE, 10_000)], 99_000, id="other_nonrelative"
        ),
        pytest.param(
            110_000,
            [(SPOUSE, 40_000), (UNMARRIED_PARTNER, 10_000), (FOSTER_CHILD, 5_000)],
            110_000,
            id="relatives_kept",
        ),
        pytest.param(
            50_000,
            [(ROOMMATE, -5_000), (ROOMMATE, -19_999)],
            50_000,
            id="loss_and_under_15_add_nothing",
        ),
        pytest.param(30_000, [(ROOMMATE, 40_000)], 0, id="floored_at_zero"),
        pytest.param(-2_000, [(ROOMMATE, 40_000)], -2_000, id="a_loss_stays_where_it_was"),
    ],
)
def test_nonrelatives_income_is_left_out(hincp, members, expected):
    """Each nonrelative's PINCP x ADJINC comes off HINCP; relatives' income stays."""
    hh = exclude_nonrelative_income(*_household(float(hincp), members))
    assert hh["HINCP"][0] == pytest.approx(expected)
    assert hh.columns == ["SERIALNO", "HINCP", "ADJINC"]


@pytest.mark.parametrize(
    ("household_income", "expected"),
    [
        pytest.param(HouseholdIncome.ALL_MEMBERS, [51_000.0, 20_200.0], id="all_members"),
        pytest.param(HouseholdIncome.RELATED_MEMBERS, [40_800.0, 20_200.0], id="related_members"),
    ],
)
def test_clean_pums_runs_every_rule(households, persons, household_income, expected):
    """Vacant and group quarters gone, income adjusted (H1's roommate out if asked), weights set."""
    persons = persons.with_columns(
        pl.Series("RELSHIPP", [20, ROOMMATE, 20, 37, 38]),
        pl.Series("PINCP", [40_000.0, 10_000.0, 20_000.0, 0.0, 0.0]),
    )
    hh, per = clean_pums(
        households,
        persons,
        pums_weights=PumsWeights.HOUSEHOLD,
        household_income=household_income,
    )
    assert sorted(hh["SERIALNO"].to_list()) == ["H1", "H2"]
    assert hh.sort("SERIALNO")["HINCP"].to_list() == pytest.approx(expected)
    assert per.sort("SERIALNO", "SPORDER")["PWGTP"].to_list() == [100.0, 100.0, 50.0]


def test_a_missing_column_is_named(households, persons):
    """No rule runs on a table that lacks what it reads."""
    with pytest.raises(KeyError, match="ADJINC"):
        clean_pums(
            households.drop("ADJINC"),
            persons,
            pums_weights=PumsWeights.INDEPENDENT,
            household_income=HouseholdIncome.ALL_MEMBERS,
        )
