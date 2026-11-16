"""The placement check: every weight sits on a record its consumer will receive.

Three ways weight has gone missing, each pinned so it raises rather than
reaching a model short:

* weight left on a record the profile does not admit;
* a supplied total that shrank in the redistribution;
* weight on a record the formatter removes -- a usable person in an unusable
  household, which no per-table flag reveals.
"""

import polars as pl
import pytest

from processing.weighting.validation.weight_checks import (
    check_days_carry_their_person,
    check_persons_carry_their_household,
    check_weight_placement,
)

FLAG = "usable_p"


def _tables(*, person_flags: list[bool], person_weights: list[float]) -> dict[str, pl.DataFrame]:
    """Two households; household 2 is unusable. Persons 1 and 2 are in 1, person 3 in 2."""
    return {
        "households": pl.DataFrame(
            {"hh_id": [1, 2], "hh_weight": [10.0, 0.0], FLAG: [True, False]}
        ),
        "persons": pl.DataFrame(
            {
                "person_id": [1, 2, 3],
                "hh_id": [1, 1, 2],
                "person_weight": person_weights,
                FLAG: person_flags,
            }
        ),
    }


class TestItPasses:
    """Clean tables go through without a sound."""

    def test_weight_only_on_kept_records(self):
        """Household 2 and its person carry nothing, so nothing is lost."""
        check_weight_placement(
            _tables(person_flags=[True, True, False], person_weights=[5.0, 5.0, 0.0]),
            FLAG,
            supplied_totals={"households": 10.0, "persons": 10.0},
        )


class TestItRaises:
    """Each failure names the table and the amount."""

    def test_weight_on_a_record_the_profile_rejects(self):
        """Person 3 is unusable yet carries 4."""
        with pytest.raises(ValueError, match=r"1 persons record.*not admitted"):
            check_weight_placement(
                _tables(person_flags=[True, True, False], person_weights=[5.0, 5.0, 4.0]),
                FLAG,
            )

    def test_a_supplied_total_that_shrank(self):
        """12 was supplied for persons; only 10 is there."""
        with pytest.raises(ValueError, match=r"persons: person_weight sums to 10.0000 but 12.0000"):
            check_weight_placement(
                _tables(person_flags=[True, True, False], person_weights=[5.0, 5.0, 0.0]),
                FLAG,
                supplied_totals={"persons": 12.0},
            )

    def test_a_usable_person_in_an_unusable_household(self):
        """Person 3 passes on their own, carries 4, and their household does not pass.

        Every per-table flag is satisfied. Only running the formatter's own
        record selection shows it: the person references a household the
        profile does not keep, which the cascade should have carried down.
        """
        with pytest.raises(ValueError, match=r"1 persons record\(s\) .* reference a households"):
            check_weight_placement(
                _tables(person_flags=[True, True, True], person_weights=[5.0, 5.0, 4.0]),
                FLAG,
            )


class TestPartialCalls:
    """A table present without a verdict is a partial call, not a failure."""

    def test_an_unflagged_table_skips_the_kept_records_check(self, caplog):
        """The redistribution already skipped it; the check says so rather than failing."""
        tables = _tables(person_flags=[True, True, True], person_weights=[5.0, 5.0, 4.0])
        tables["persons"] = tables["persons"].drop(FLAG)
        with caplog.at_level("DEBUG"):
            check_weight_placement(tables, FLAG)
        assert "Skipping the kept-records total check: persons" in caplog.text


def _people_and_days(person_day_weights: list[float]) -> dict[str, pl.DataFrame]:
    """Person 1 (weight 10) owns days 10 and 11; person 2 (weight 6) owns day 20.

    Person 3 (weight 0) owns no day at all.
    """
    return {
        "persons": pl.DataFrame(
            {"person_id": [1, 2, 3], "hh_id": [1, 1, 1], "person_weight": [10.0, 6.0, 0.0]}
        ),
        "person_days": pl.DataFrame(
            {
                "person_day_id": [10, 11, 20],
                "person_id": [1, 1, 2],
                "person_day_weight": person_day_weights,
            }
        ),
    }


class TestDaysCarryTheirPerson:
    """Every person's days sum to that person's weight, with no exemptions.

    The supplied-weights path moves a dropped day's weight to the person's other
    days, so the identity is exact. A person carrying weight that no day carries
    is weight the day table has lost.
    """

    def test_days_that_sum_to_their_person_pass(self):
        """7 + 3 = 10 and 6 = 6; person 3 carries nothing and owns nothing."""
        check_days_carry_their_person(_people_and_days([7.0, 3.0, 6.0]))

    def test_a_person_whose_days_fall_short_is_named(self):
        """Person 1's days sum to 8, not 10."""
        with pytest.raises(ValueError, match=r"1 person\(s\) .* person_id: \[1\]"):
            check_days_carry_their_person(_people_and_days([4.0, 4.0, 6.0]))

    def test_a_weighted_person_with_no_day_rows_is_named(self):
        """Person 3 carries 5 and owns no day: none of the 5 reaches the days."""
        tables = _people_and_days([7.0, 3.0, 6.0])
        tables["persons"] = tables["persons"].with_columns(
            pl.Series("person_weight", [10.0, 6.0, 5.0])
        )
        with pytest.raises(ValueError, match=r"person_id: \[3\]"):
            check_days_carry_their_person(tables)

    def test_it_reads_the_profiles_columns(self):
        """Under a profile, the suffixed columns are the ones checked."""
        tables = {
            name: df.rename({c: f"{c}_p" for c in df.columns if c.endswith("_weight")})
            for name, df in _people_and_days([4.0, 4.0, 6.0]).items()
        }
        with pytest.raises(ValueError, match=r"person_day_weight_p"):
            check_days_carry_their_person(tables, profile="p")


def _households_and_persons(person_weights: list[float]) -> dict[str, pl.DataFrame]:
    """Household 1 (weight 10) has persons 1 and 2; household 2 (weight 0) has person 3."""
    return {
        "households": pl.DataFrame({"hh_id": [1, 2], "hh_weight": [10.0, 0.0]}),
        "persons": pl.DataFrame(
            {"person_id": [1, 2, 3], "hh_id": [1, 1, 2], "person_weight": person_weights}
        ),
    }


class TestPersonsCarryTheirHousehold:
    """Household weight x number of persons = the sum of its persons' weights."""

    def test_a_consistent_household_passes(self):
        """5 + 15 = 10 x 2: persons need not be equal, only sum to the household."""
        check_persons_carry_their_household(_households_and_persons([5.0, 15.0, 0.0]))

    def test_a_household_whose_persons_fall_short_is_named(self):
        """Household 1's persons sum to 18, not 20."""
        with pytest.raises(ValueError, match=r"1 household\(s\) .* hh_id: \[1\]"):
            check_persons_carry_their_household(_households_and_persons([9.0, 9.0, 0.0]))

    def test_a_dropped_household_whose_person_carries_weight_is_named(self):
        """Household 2 carries 0, so its person must too."""
        with pytest.raises(ValueError, match=r"hh_id: \[2\]"):
            check_persons_carry_their_household(_households_and_persons([10.0, 10.0, 4.0]))
