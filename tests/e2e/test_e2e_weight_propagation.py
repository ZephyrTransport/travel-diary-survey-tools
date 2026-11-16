"""Weight is conserved as it moves down the hierarchy, and no unusable record carries any.

Only household weights are supplied, so the run derives every level below them. The
identities are checked with the pipeline's own ``_check_hierarchy`` and
``_check_joint_sums``, once per weighted profile: a strict one and a relaxed one.
"""

import polars as pl
import pytest

from processing.usability import usable_col_for
from processing.weighting.core.hierarchy import weight_col_for
from processing.weighting.validation.weight_checks import _check_hierarchy, _check_joint_sums

# Two of the weight_profiles conftest.py gives add_existing_weights. STRICT gates out
# records RELAXED keeps, which is what makes their columns differ.
STRICT = "ctramp"
RELAXED = "analysis"
WEIGHT_PROFILES = (STRICT, RELAXED)

# Every weighted table and its base weight column.
_ALL_WEIGHTS = {
    "households": "hh_weight",
    "persons": "person_weight",
    "person_days": "person_day_weight",
    "unlinked_trips": "unlinked_trip_weight",
    "linked_trips": "linked_trip_weight",
    "tours": "tour_weight",
    "joint_trips": "joint_trip_weight",
    "joint_tours": "joint_tour_weight",
}

# Relative tolerance for a held total: floating-point drift, nothing more.
_TOTAL_REL_TOL = 1e-6


def _weight(table: str, profile: str) -> str:
    """The weight column *table* carries for *profile*."""
    return weight_col_for(_ALL_WEIGHTS[table], profile)


@pytest.mark.parametrize("profile", WEIGHT_PROFILES)
class TestNothingIsLost:
    """The conservation identities, asserted with the pipeline's own checks."""

    def test_the_hierarchy_reconciles(self, full_tables, profile):
        """Copy levels sum to the level above times its size; a person's days sum to them."""
        _check_hierarchy(full_tables, usable_col_for(profile), profile)

    def test_the_joint_groupings_reconcile(self, full_tables, profile):
        """A joint entity equals the member records it kept."""
        _check_joint_sums(full_tables, usable_col_for(profile), profile)


@pytest.mark.parametrize("profile", WEIGHT_PROFILES)
class TestTheWeightsAreActuallyThere:
    """A conservation check over an empty column would pass and mean nothing."""

    @pytest.mark.parametrize("table", ["households", "persons", "person_days"])
    def test_every_record_carries_a_weight(self, full_tables, table, profile):
        """The profile's own column exists and is populated on every row."""
        column = _weight(table, profile)
        assert column in full_tables[table].columns, f"{table} has no {column}"
        assert full_tables[table][column].null_count() == 0

    def test_the_supplied_weights_are_distinct(self, full_tables, profile):
        """Identical weights hide arithmetic: halve one, double another, same sum."""
        assert full_tables["households"][_weight("households", profile)].n_unique() > 1

    def test_derived_weights_are_not_all_equal_either(self, full_tables, profile):
        """Which would mean the copy rule flattened rather than carried them."""
        assert full_tables["persons"][_weight("persons", profile)].n_unique() > 1


class TestTheProfilesDoNotCollide:
    """Two fits over one set of tables must not read or overwrite each other."""

    def test_no_unsuffixed_weight_survives(self, full_tables):
        """Two columns for one weight leave a reader no way to tell which counts."""
        assert "hh_weight" not in full_tables["households"].columns

    def test_the_relaxed_profile_keeps_more_records(self, full_tables):
        """Otherwise the two column sets describe the same universe and prove nothing."""
        person_days = full_tables["person_days"]
        strict_kept = person_days.filter(
            pl.col(usable_col_for(STRICT)).fill_null(value=False)
        ).height
        relaxed_kept = person_days.filter(
            pl.col(usable_col_for(RELAXED)).fill_null(value=False)
        ).height
        assert relaxed_kept > strict_kept

    def test_a_record_the_strict_gate_drops_is_weighted_by_the_relaxed_one(self, full_tables):
        """The zero belongs to the profile that excluded it, not to the record."""
        person_days = full_tables["person_days"]
        gated_out = person_days.filter(
            ~pl.col(usable_col_for(STRICT)).fill_null(value=False)
            & pl.col(usable_col_for(RELAXED)).fill_null(value=False)
        )
        if gated_out.is_empty():
            pytest.skip("no day separates the two profiles in this run")

        assert gated_out.filter(pl.col(_weight("person_days", STRICT)) > 0).height == 0
        assert (
            gated_out.filter(pl.col(_weight("person_days", RELAXED)) > 0).height == gated_out.height
        )


def test_the_fixture_has_someone_with_several_days(full_tables):
    """Guards the guard: without this the split test below is vacuous."""
    person_days = full_tables["person_days"]
    counts = person_days.group_by("person_id").agg(pl.len().alias("n_days"))
    assert counts.filter(pl.col("n_days") > 1).height >= 1


@pytest.mark.parametrize("profile", WEIGHT_PROFILES)
def test_a_persons_days_sum_to_their_own_weight(full_tables, profile):
    """A person's weight divides between their usable days, so the days sum to it."""
    day_wt, person_wt = _weight("person_days", profile), _weight("persons", profile)
    person_days = full_tables["person_days"]
    counts = person_days.group_by("person_id").agg(pl.len().alias("n_days"))
    multi = person_days.join(counts.filter(pl.col("n_days") > 1), on="person_id", how="inner")
    summed = multi.group_by("person_id").agg(pl.col(day_wt).sum().alias("days_total"))
    joined = summed.join(full_tables["persons"].select("person_id", person_wt), on="person_id")

    adrift = joined.filter((pl.col("days_total") - pl.col(person_wt)).abs() > 0.01)

    assert adrift.height == 0, f"days do not sum to person weight under {profile}:\n{adrift}"


@pytest.mark.parametrize("profile", WEIGHT_PROFILES)
class TestTheTargetsAreHeld:
    """Households and persons are known populations; the gate must not shrink them.

    Computed here from the supplied weight file and the tables, not through the
    pipeline's own checks, so a rule and its check cannot agree on a wrong answer.
    """

    def test_the_household_total_is_the_supplied_total(self, full_tables, full_input_dir, profile):
        supplied = pl.read_csv(full_input_dir / "weights" / "hh_weights.csv")["hh_weight"].sum()
        held = full_tables["households"][_weight("households", profile)].sum()
        assert held == pytest.approx(supplied, rel=_TOTAL_REL_TOL)

    def test_the_person_total_is_what_the_households_represent(self, full_tables, profile):
        """Each household stands for all its members, so persons sum to hh_weight x size."""
        hh_wt, person_wt = _weight("households", profile), _weight("persons", profile)
        sizes = full_tables["persons"].group_by("hh_id").agg(pl.len().alias("_n_persons"))
        represented = (
            full_tables["households"]
            .join(sizes, on="hh_id", how="left")
            .select((pl.col(hh_wt) * pl.col("_n_persons").fill_null(0)).sum())
            .item()
        )
        held = full_tables["persons"][person_wt].sum()
        assert held == pytest.approx(represented, rel=_TOTAL_REL_TOL)


@pytest.mark.parametrize("profile", WEIGHT_PROFILES)
@pytest.mark.parametrize("table", list(_ALL_WEIGHTS))
def test_no_unusable_record_in_any_table_carries_weight(full_tables, table, profile):
    """A weight on a record the gate drops is lost, in every table."""
    df = full_tables.get(table)
    column = _weight(table, profile)
    if df is None or column not in df.columns:
        pytest.skip(f"{table} carries no {column} in this run")
    stray = df.filter(
        ~pl.col(usable_col_for(profile)).fill_null(value=False) & (pl.col(column) > 0)
    )
    assert stray.is_empty(), f"{stray.height} unusable {table} carry {column}"
