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

Every identity below is asserted once per weighted profile. The run weights a
strict profile and a relaxed one, so the same tables carry two independent column
sets, and a rule that quietly read the wrong one would still reconcile against
itself -- it is the *pairing* of a gate with its own columns that these check.
"""

import polars as pl
import pytest

from processing.weighting.core.hierarchy import weight_col_for
from processing.weighting.validation.weight_checks import _check_hierarchy, _check_joint_sums

# The profiles tests/e2e/conftest.py hands add_existing_weights. STRICT gates out
# records the relaxed one keeps, which is what makes their columns differ.
STRICT = "ctramp_usable"
RELAXED = "analysis_usable"
PROFILES = (STRICT, RELAXED)


@pytest.fixture(scope="module")
def weighted(full_result) -> dict[str, pl.DataFrame]:
    """Canonical tables after weights have been propagated."""
    return (
        full_result.as_dict_non_null()
        if hasattr(full_result, "as_dict_non_null")
        else dict(full_result)
    )


def _weight(table: str, profile: str) -> str:
    """The weight column *table* carries for *profile*."""
    bases = {
        "households": "hh_weight",
        "persons": "person_weight",
        "days": "day_weight",
    }
    return weight_col_for(bases[table], profile)


@pytest.mark.parametrize("profile", PROFILES)
class TestNothingIsLost:
    """The conservation identities, asserted with the pipeline's own checks."""

    def test_the_hierarchy_reconciles(self, weighted, profile):
        """Every DOWN edge sums to what its parents represent.

        Copy levels conserve ``sum(child) == sum(parent x n_children)``; the
        split level conserves a person's days summing to their own weight. Both
        are read from HIERARCHY rather than restated here.
        """
        _check_hierarchy(weighted, profile, profile)

    def test_the_joint_groupings_reconcile(self, weighted, profile):
        """A joint entity equals the member records it kept."""
        _check_joint_sums(weighted, profile, profile)


@pytest.mark.parametrize("profile", PROFILES)
class TestTheWeightsAreActuallyThere:
    """A conservation check over an empty column would pass and mean nothing."""

    @pytest.mark.parametrize("table", ["households", "persons", "days"])
    def test_every_record_carries_a_weight(self, weighted, table, profile):
        """The profile's own column exists and is populated on every row."""
        column = _weight(table, profile)
        assert column in weighted[table].columns, f"{table} has no {column}"
        assert weighted[table][column].null_count() == 0

    def test_the_supplied_weights_are_distinct(self, weighted, profile):
        """Identical weights hide arithmetic: halve one, double another, same sum."""
        assert weighted["households"][_weight("households", profile)].n_unique() > 1

    def test_derived_weights_are_not_all_equal_either(self, weighted, profile):
        """Which would mean the copy rule flattened rather than carried them."""
        assert weighted["persons"][_weight("persons", profile)].n_unique() > 1


class TestTheProfilesDoNotCollide:
    """Two fits over one set of tables must not read or overwrite each other."""

    def test_no_unsuffixed_weight_survives(self, weighted):
        """Two columns for one weight leave a reader no way to tell which counts."""
        assert "hh_weight" not in weighted["households"].columns

    def test_the_relaxed_profile_keeps_more_records(self, weighted):
        """Otherwise the two column sets describe the same universe and prove nothing."""
        days = weighted["days"]
        strict_kept = days.filter(pl.col(STRICT).fill_null(value=False)).height
        relaxed_kept = days.filter(pl.col(RELAXED).fill_null(value=False)).height
        assert relaxed_kept > strict_kept

    def test_a_record_the_strict_gate_drops_is_weighted_by_the_relaxed_one(self, weighted):
        """The zero belongs to the profile that excluded it, not to the record."""
        days = weighted["days"]
        gated_out = days.filter(
            ~pl.col(STRICT).fill_null(value=False) & pl.col(RELAXED).fill_null(value=False)
        )
        if gated_out.is_empty():
            pytest.skip("no day separates the two profiles in this run")

        assert gated_out.filter(pl.col(_weight("days", STRICT)) > 0).height == 0
        assert gated_out.filter(pl.col(_weight("days", RELAXED)) > 0).height == gated_out.height


def test_the_fixture_has_someone_with_several_days(weighted):
    """Guards the guard: without this the split tests below are vacuous."""
    days = weighted["days"]
    counts = days.group_by("person_id").agg(pl.len().alias("n_days"))
    assert counts.filter(pl.col("n_days") > 1).height >= 1


@pytest.mark.parametrize("profile", PROFILES)
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

    def test_a_persons_days_sum_to_their_own_weight(self, weighted, profile):
        """The split identity, stated per person rather than in aggregate."""
        day_wt, person_wt = _weight("days", profile), _weight("persons", profile)
        multi = self._multi_day(weighted)
        summed = multi.group_by("person_id").agg(pl.col(day_wt).sum().alias("days_total"))
        joined = summed.join(weighted["persons"].select("person_id", person_wt), on="person_id")

        adrift = joined.filter((pl.col("days_total") - pl.col(person_wt)).abs() > 0.01)

        assert adrift.height == 0, f"days do not sum to person weight under {profile}:\n{adrift}"

    def test_a_dropped_day_carries_no_weight(self, weighted, profile):
        """Its share moves to the days that survived, rather than disappearing."""
        unusable = weighted["days"].filter(~pl.col(profile).fill_null(value=False))
        if unusable.is_empty():
            pytest.skip(f"no day was gated out by {profile} in this run")

        assert unusable.filter(pl.col(_weight("days", profile)) > 0).height == 0


@pytest.mark.parametrize("profile", PROFILES)
class TestTheGateDecidesWhoCarriesWeight:
    """Weight follows the usability flag the run was given, not completeness."""

    @pytest.mark.parametrize("table", ["persons", "days"])
    def test_no_unusable_record_carries_weight(self, weighted, table, profile):
        """Each profile's zeros line up with that profile's own verdict."""
        unusable = weighted[table].filter(~pl.col(profile).fill_null(value=False))
        if unusable.is_empty():
            pytest.skip(f"every {table} record was usable under {profile} in this run")

        assert unusable.filter(pl.col(_weight(table, profile)) > 0).height == 0
