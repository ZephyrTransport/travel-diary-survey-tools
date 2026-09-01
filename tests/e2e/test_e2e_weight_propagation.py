"""Weight must be conserved as it moves down the hierarchy, and none may vanish.

Only household weights are supplied; everything below is derived. So a run
exercises the copy rule down to persons and the split rule from a person across
their days, on a hierarchy the pipeline built rather than one assembled by hand
in a unit test.

The assertions are the pipeline's own conservation checks, not a
reimplementation of them. ``_check_hierarchy`` and ``_check_joint_sums`` read
the identities off the same ``HIERARCHY`` the propagation walks, so a test
calling them cannot drift from the rule it is testing. They are also, until now,
reachable only from ``compute_weights`` -- which fetches PUMS and cannot run
here -- so nothing integration-level had ever executed them. A signature change
to ``_check_hierarchy`` consequently reached two projects before it reached the
suite.

The split is the rule worth the trouble. A person's weight divides between the
days that survive the usability gate, so the divisor changes when a day is
dropped: household 26 exists to make that concrete, with one member whose second
day never closes its tour. Everywhere else in the fixture a person has a single
day and weight passes through untouched, where a split dividing by *all* days
rather than the usable ones would look perfectly correct.
"""

import polars as pl
import pytest

from processing.weighting.validation.weight_checks import _check_hierarchy, _check_joint_sums

FLAG = "ctramp_usable"


@pytest.fixture(scope="module")
def weighted(full_result) -> dict[str, pl.DataFrame]:
    """Canonical tables after weights have been propagated."""
    return (
        full_result.as_dict_non_null()
        if hasattr(full_result, "as_dict_non_null")
        else dict(full_result)
    )


class TestNothingIsLost:
    """The conservation identities, asserted with the pipeline's own checks."""

    def test_the_hierarchy_reconciles(self, weighted):
        """Every DOWN edge sums to what its parents represent.

        Copy levels conserve ``sum(child) == sum(parent x n_children)``; the
        split level conserves a person's days summing to their own weight. Both
        are read from HIERARCHY rather than restated here.
        """
        _check_hierarchy(weighted, FLAG)

    def test_the_joint_groupings_reconcile(self, weighted):
        """A joint entity equals the member records it kept."""
        _check_joint_sums(weighted, FLAG)


class TestTheWeightsAreActuallyThere:
    """A conservation check over an empty column would pass and mean nothing."""

    @pytest.mark.parametrize(
        ("table", "column"),
        [("households", "hh_weight"), ("persons", "person_weight"), ("days", "day_weight")],
    )
    def test_every_record_carries_a_weight(self, weighted, table, column):
        assert column in weighted[table].columns, f"{table} has no {column}"
        assert weighted[table][column].null_count() == 0

    def test_the_supplied_weights_are_distinct(self, weighted):
        """Identical weights hide arithmetic: halve one, double another, same sum."""
        assert weighted["households"]["hh_weight"].n_unique() > 1

    def test_derived_weights_are_not_all_equal_either(self, weighted):
        """Which would mean the copy rule flattened rather than carried them."""
        assert weighted["persons"]["person_weight"].n_unique() > 1


class TestTheSplitDividesAmongUsableDays:
    """The rule the fixture was extended to make non-trivial.

    A person's weight is divided between the days that survive the gate. When
    one is dropped the remaining days must absorb its share, so the person's
    total is unchanged and the dropped day carries nothing.
    """

    def _multi_day(self, weighted: dict[str, pl.DataFrame]) -> pl.DataFrame:
        days = weighted["days"]
        counts = days.group_by("person_id").agg(pl.len().alias("n_days"))
        return days.join(counts.filter(pl.col("n_days") > 1), on="person_id", how="inner")

    def test_the_fixture_has_someone_with_several_days(self, weighted):
        """Guards the guard: without this the two tests below are vacuous."""
        assert self._multi_day(weighted)["person_id"].n_unique() >= 1

    def test_a_persons_days_sum_to_their_own_weight(self, weighted):
        """The split identity, stated per person rather than in aggregate."""
        multi = self._multi_day(weighted)
        summed = multi.group_by("person_id").agg(pl.col("day_weight").sum().alias("days_total"))
        joined = summed.join(
            weighted["persons"].select("person_id", "person_weight"), on="person_id"
        )

        adrift = joined.filter((pl.col("days_total") - pl.col("person_weight")).abs() > 0.01)

        assert adrift.height == 0, f"days do not sum to person weight:\n{adrift}"

    def test_a_dropped_day_carries_no_weight(self, weighted):
        """Its share moves to the days that survived, rather than disappearing."""
        unusable = weighted["days"].filter(~pl.col(FLAG).fill_null(value=False))
        if unusable.is_empty():
            pytest.skip("no day was gated out in this run")

        assert unusable.filter(pl.col("day_weight") > 0).height == 0


class TestTheGateDecidesWhoCarriesWeight:
    """Weight follows the usability flag the run was given, not completeness."""

    @pytest.mark.parametrize("table", ["persons", "days"])
    def test_no_unusable_record_carries_weight(self, weighted, table):
        unusable = weighted[table].filter(~pl.col(FLAG).fill_null(value=False))
        if unusable.is_empty():
            pytest.skip(f"every {table} record was usable in this run")

        weight_col = {"persons": "person_weight", "days": "day_weight"}[table]

        assert unusable.filter(pl.col(weight_col) > 0).height == 0
