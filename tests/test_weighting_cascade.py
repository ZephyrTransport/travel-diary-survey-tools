"""Tests for the weight-cascade diagnostics.

The cascade describes what carries weight below the household, so the cases that
matter are the ones where a level disagrees with its parent: a record the profile
admits whose household was dropped, and a parent carrying weight with no usable
child at all. Both are correct behaviour that the report exists to surface.
"""

import polars as pl

from processing.weighting.diagnostics.data import (
    redistribution,
    split_identity,
    weight_cascade,
)

PROFILE = "ctramp"
FLAG = "usable_ctramp"


def _tables(
    *,
    hh_usable: list[bool],
    hh_weight: list[float],
    per_usable: list[bool],
    per_weight: list[float],
    per_hh: list[int],
) -> dict[str, pl.DataFrame]:
    """Minimal households/persons pair carrying one profile's columns."""
    return {
        "households": pl.DataFrame(
            {
                "hh_id": list(range(1, len(hh_usable) + 1)),
                FLAG: hh_usable,
                f"hh_weight_{PROFILE}": hh_weight,
            }
        ),
        "persons": pl.DataFrame(
            {
                "person_id": list(range(1, len(per_usable) + 1)),
                "hh_id": per_hh,
                FLAG: per_usable,
                f"person_weight_{PROFILE}": per_weight,
            }
        ),
    }


class TestWeightCascade:
    """Counts per level, split by why a record carries no weight."""

    def test_counts_split_exhaustively(self):
        """Gated + weighted + unweighted must account for every row."""
        tables = _tables(
            hh_usable=[True, False],
            hh_weight=[10.0, 0.0],
            per_usable=[True, True, False],
            per_weight=[10.0, 0.0, 0.0],
            per_hh=[1, 2, 1],
        )
        cascade = weight_cascade(tables, profile=PROFILE, usability_flag_col=FLAG)
        rows = {r.table: r for r in cascade}

        households = rows["households"]
        assert (households.rows, households.usable, households.weighted) == (2, 1, 1)
        assert households.gated == 1
        assert households.unweighted == 0

        persons = rows["persons"]
        assert persons.gated + persons.weighted + persons.unweighted == persons.rows

    def test_usable_person_in_dropped_household_is_unweighted(self):
        """A person the profile admits still carries nothing if their household fell.

        Person and household reduce differently -- a person needs one usable day,
        a household needs a date on which every member was usable -- so the two
        verdicts legitimately disagree. The count has to show it rather than
        assume the flag implies a weight.
        """
        tables = _tables(
            hh_usable=[False],
            hh_weight=[0.0],
            per_usable=[True],
            per_weight=[0.0],
            per_hh=[1],
        )
        persons = next(
            r
            for r in weight_cascade(tables, profile=PROFILE, usability_flag_col=FLAG)
            if r.table == "persons"
        )
        assert persons.usable == 1
        assert persons.weighted == 0
        assert persons.unweighted == 1
        assert persons.gated == 0

    def test_level_without_this_profile_is_skipped(self):
        """A table carrying another profile's column contributes no row."""
        tables = _tables(
            hh_usable=[True],
            hh_weight=[1.0],
            per_usable=[True],
            per_weight=[1.0],
            per_hh=[1],
        )
        tables["persons"] = tables["persons"].rename(
            {f"person_weight_{PROFILE}": "person_weight_x"}
        )
        reported = {
            r.table for r in weight_cascade(tables, profile=PROFILE, usability_flag_col=FLAG)
        }
        assert reported == {"households"}

    def test_missing_table_is_skipped(self):
        """A partial call reports only what it was given."""
        tables = _tables(
            hh_usable=[True],
            hh_weight=[1.0],
            per_usable=[True],
            per_weight=[1.0],
            per_hh=[1],
        )
        del tables["persons"]
        cascade = weight_cascade(tables, profile=PROFILE, usability_flag_col=FLAG)
        reported = [r.table for r in cascade]
        assert reported == ["households"]


class TestRedistribution:
    """The factor a survivor absorbs for the siblings the gate removed."""

    def test_ratio_is_one_when_every_sibling_is_usable(self):
        """Nothing was removed, so nothing is being carried."""
        tables = _tables(
            hh_usable=[True],
            hh_weight=[10.0],
            per_usable=[True, True],
            per_weight=[10.0, 10.0],
            per_hh=[1, 1],
        )
        row = next(iter(redistribution(tables, profile=PROFILE, usability_flag_col=FLAG)))
        assert row.p50 == 1.0
        assert row.maximum == 1.0
        assert row.share_above == 0.0

    def test_survivor_carries_the_whole_household(self):
        """One usable member of four stands in for all of them."""
        tables = _tables(
            hh_usable=[True],
            hh_weight=[10.0],
            per_usable=[True, False, False, False],
            per_weight=[40.0, 0.0, 0.0, 0.0],
            per_hh=[1, 1, 1, 1],
        )
        row = next(iter(redistribution(tables, profile=PROFILE, usability_flag_col=FLAG)))
        assert row.maximum == 4.0
        assert row.share_above == 100.0

    def test_dropped_households_are_excluded(self):
        """A rejected household's members say nothing about redistribution."""
        tables = _tables(
            hh_usable=[True, False],
            hh_weight=[10.0, 0.0],
            per_usable=[True, True],
            per_weight=[10.0, 0.0],
            per_hh=[1, 2],
        )
        row = next(iter(redistribution(tables, profile=PROFILE, usability_flag_col=FLAG)))
        assert row.maximum == 1.0


class TestSplitIdentity:
    """Days divide their person's weight; they must sum back to it."""

    @staticmethod
    def _with_days(day_weight: list[float], day_person: list[int]) -> dict[str, pl.DataFrame]:
        tables = _tables(
            hh_usable=[True],
            hh_weight=[10.0],
            per_usable=[True, True],
            per_weight=[10.0, 10.0],
            per_hh=[1, 1],
        )
        tables["days"] = pl.DataFrame(
            {
                "day_id": list(range(1, len(day_weight) + 1)),
                "person_id": day_person,
                FLAG: [True] * len(day_weight),
                f"day_weight_{PROFILE}": day_weight,
            }
        )
        return tables

    def test_days_summing_to_the_person_leave_no_residual(self):
        """Two usable days at half the person weight each."""
        tables = self._with_days([5.0, 5.0, 10.0], [1, 1, 2])
        row = next(iter(split_identity(tables, profile=PROFILE)))
        assert row.max_residual == 0.0
        assert row.stranded_parents == 0
        assert row.parents_checked == 2

    def test_residual_is_reported_not_swallowed(self):
        """A split that does not reconcile is arithmetic we control -- surface it."""
        tables = self._with_days([5.0, 3.0, 10.0], [1, 1, 2])
        row = next(iter(split_identity(tables, profile=PROFILE)))
        assert row.max_residual == 2.0

    def test_person_with_no_usable_day_is_stranded(self):
        """Their weight is unrepresented below, never pooled onto other people."""
        tables = self._with_days([10.0], [2])
        row = next(iter(split_identity(tables, profile=PROFILE)))
        assert row.stranded_parents == 1
        assert row.stranded_weight == 10.0
