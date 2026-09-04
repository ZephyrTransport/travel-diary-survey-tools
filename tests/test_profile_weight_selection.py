"""A consumer reads the weights belonging to the profile it names.

The weighting may write one column set per profile. A consumer already names the
profile it gates on, and the suffix is that name, so one setting answers both
questions. What matters is that the pairing is enforced: reading another profile's
weights, or falling back to a bare column, publishes expansion factors for a
universe the output is not describing.
"""

import polars as pl
import pytest

from processing.formatting.usable_records import select_profile_weights

STRICT = "ctramp_usable"
RELAXED = "analysis_usable"


def _households(**columns) -> dict[str, pl.DataFrame]:
    """A one-row households table carrying the given weight columns."""
    return {"households": pl.DataFrame({"hh_id": [1], **columns})}


class TestResolution:
    """The named profile's column becomes the base name; the others go."""

    def test_the_named_profiles_column_is_renamed_in(self):
        """The suffix comes off, so every downstream read is unchanged."""
        tables = _households(**{f"hh_weight_{STRICT}": [7.0]})
        out = select_profile_weights(tables, STRICT)["households"]
        assert out["hh_weight"].to_list() == [7.0]
        assert f"hh_weight_{STRICT}" not in out.columns

    def test_another_profiles_column_is_dropped(self):
        """It is not this consumer's to deliver, and invites the wrong read."""
        tables = _households(**{f"hh_weight_{STRICT}": [7.0], f"hh_weight_{RELAXED}": [9.0]})
        out = select_profile_weights(tables, STRICT)["households"]
        assert out["hh_weight"].to_list() == [7.0]
        assert f"hh_weight_{RELAXED}" not in out.columns

    def test_each_consumer_gets_its_own_numbers(self):
        """The same tables, read under two profiles, give two different weights."""
        tables = _households(**{f"hh_weight_{STRICT}": [7.0], f"hh_weight_{RELAXED}": [9.0]})
        assert select_profile_weights(tables, STRICT)["households"]["hh_weight"][0] == 7.0
        assert select_profile_weights(tables, RELAXED)["households"]["hh_weight"][0] == 9.0

    def test_every_level_is_resolved_not_just_households(self):
        """One rule over the whole hierarchy, not a per-table special case."""
        tables = {
            "days": pl.DataFrame({"day_id": [1], f"day_weight_{STRICT}": [3.0]}),
            "tours": pl.DataFrame({"tour_id": [1], f"tour_weight_{STRICT}": [4.0]}),
        }
        out = select_profile_weights(tables, STRICT)
        assert out["days"]["day_weight"].to_list() == [3.0]
        assert out["tours"]["tour_weight"].to_list() == [4.0]

    def test_the_input_is_not_mutated(self):
        """The caller's frames are left alone, as everywhere else in the gate."""
        tables = _households(**{f"hh_weight_{STRICT}": [7.0]})
        select_profile_weights(tables, STRICT)
        assert f"hh_weight_{STRICT}" in tables["households"].columns


class TestNoSuffixedColumns:
    """A run that weighted a single profile writes the bare names, and is untouched."""

    def test_a_bare_column_is_left_alone(self):
        """Nothing to resolve, and nothing that needs resolving."""
        tables = _households(hh_weight=[5.0])
        out = select_profile_weights(tables, STRICT)["households"]
        assert out["hh_weight"].to_list() == [5.0]

    def test_a_table_with_no_weight_at_all_is_left_alone(self):
        """A table can reach the gate before any weighting step ran."""
        tables = _households()
        assert select_profile_weights(tables, STRICT)["households"].columns == ["hh_id"]

    def test_a_non_weight_table_is_ignored(self):
        """Only the levels that carry weights are considered."""
        tables = {"zones": pl.DataFrame({"taz": [1], "hh_weight_x": [1.0]})}
        assert "hh_weight_x" in select_profile_weights(tables, STRICT)["zones"].columns


class TestTheUnweightedProfile:
    """The case this raises on, because falling back is worse than stopping."""

    def test_a_profile_that_was_never_weighted_raises(self):
        """Otherwise the bare column is read -- whatever it happens to hold.

        This is the failure the e2e caught: a formatter reading a profile the
        weighting was not told to fit, publishing a loader placeholder as an
        expansion factor.
        """
        tables = _households(**{f"hh_weight_{RELAXED}": [9.0]})
        with pytest.raises(ValueError, match="but not for 'ctramp_usable'"):
            select_profile_weights(tables, STRICT)

    def test_the_message_names_the_profiles_that_were_weighted(self):
        """So the fix -- add it to weight_profiles -- is obvious from the error."""
        tables = _households(**{f"hh_weight_{RELAXED}": [9.0]})
        with pytest.raises(ValueError, match="but not for") as excinfo:
            select_profile_weights(tables, STRICT)
        message = str(excinfo.value)
        assert RELAXED in message
        assert "weight_profiles" in message

    def test_a_stale_bare_column_does_not_rescue_it(self):
        """A bare column beside another profile's is still not this profile's."""
        tables = _households(hh_weight=[1.0], **{f"hh_weight_{RELAXED}": [9.0]})
        with pytest.raises(ValueError, match="but not for"):
            select_profile_weights(tables, STRICT)

    def test_both_present_prefers_the_profile_and_warns(self, caplog):
        """A bare column can be legitimate loader input, so this is not fatal."""
        tables = _households(hh_weight=[1.0], **{f"hh_weight_{STRICT}": [7.0]})
        with caplog.at_level("WARNING"):
            out = select_profile_weights(tables, STRICT)["households"]
        assert out["hh_weight"].to_list() == [7.0]
        assert any("carries both" in record.message for record in caplog.records)
