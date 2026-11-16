"""The per-profile column contract, from config to consumer.

A config names its universes; each universe's columns are the base name with
the profile appended verbatim; a consumer reads only its own profile's columns.
"""

from pathlib import Path

import polars as pl
import pytest

from processing.formatting.usable_records import select_profile_columns
from processing.weighting.core.hierarchy import (
    HIERARCHY,
    LEVELS,
    seed_col_for,
    weight_col_for,
    weight_columns_for,
)
from processing.weighting.core.specs import (
    HouseholdIncome,
    PumsWeights,
    WeightingConfig,
    resolve_fitted_profiles,
)

STRICT = "ctramp"
RELAXED = "analysis"
PROFILE = "analysis"


def _config(**overrides) -> WeightingConfig:
    """A WeightingConfig with only the universe settings that matter here."""
    return WeightingConfig(
        geography={},
        state_fips="06",
        pums_year=2023,
        pums_weights=PumsWeights.INDEPENDENT,
        household_income=HouseholdIncome.ALL_MEMBERS,
        use_imputed_values=True,
        census_dir=Path("census"),
        **overrides,
    )


class TestResolvingTheUniverse:
    """What a profile list resolves to, and which lists are refused."""

    def test_profiles_are_returned_in_the_order_given(self):
        """Fits run in this order, and their reports are read in it."""
        assert resolve_fitted_profiles(["b", "a"]) == ("b", "a")

    @pytest.mark.parametrize(
        "profiles",
        [
            pytest.param([], id="an_empty_list"),
            pytest.param(None, id="nothing_at_all"),
        ],
    )
    def test_an_unstated_universe_is_refused(self, profiles):
        """Both spellings of "no opinion" reach the same refusal."""
        with pytest.raises(ValueError, match="needs a universe"):
            resolve_fitted_profiles(profiles)

    def test_a_repeated_profile_is_refused(self):
        """The second fit would overwrite the first under the same column names."""
        with pytest.raises(ValueError, match="same profile twice"):
            resolve_fitted_profiles(["a", "b", "a"])


class TestTheConfigUsesIt:
    """WeightingConfig delegates to the same resolution."""

    def test_a_profile_config_exposes_one_fit_each(self):
        """One balancing run per profile, in the order configured."""
        assert _config(weight_profiles=("a", "b")).weight_profiles == ("a", "b")

    def test_an_unstated_universe_is_refused_by_the_config(self):
        """The config resolves its profiles on construction, so it refuses the same lists."""
        with pytest.raises(ValueError, match="needs a universe"):
            _config(weight_profiles=())


class TestColumnNaming:
    """The suffix is appended as given -- no stripping, no case change."""

    @pytest.mark.parametrize("level", HIERARCHY, ids=lambda level: level.table)
    def test_every_level_resolves(self, level):
        """Both spellings agree, with and without a profile, for every table."""
        assert weight_col_for(level.weight_col, None) == level.weight_col
        assert level.weight_col_for(None) == level.weight_col
        assert weight_columns_for(None)[level.table] == level.weight_col

        assert weight_col_for(level.weight_col, PROFILE) == f"{level.weight_col}_{PROFILE}"
        assert level.weight_col_for(PROFILE) == f"{level.weight_col}_{PROFILE}"
        assert weight_columns_for(PROFILE)[level.table] == f"{level.weight_col}_{PROFILE}"

    @pytest.mark.parametrize(
        ("resolve", "base", "profile", "expected"),
        [
            # a profile name is a label, not something to normalise
            pytest.param(
                weight_col_for, "tour_weight", "Weird_Name_v2", "tour_weight_Weird_Name_v2"
            ),
            # base_weight belongs to a fit, so it is suffixed like the weights
            pytest.param(seed_col_for, "base_weight", None, "base_weight"),
            pytest.param(seed_col_for, "base_weight", PROFILE, "base_weight_analysis"),
        ],
        ids=[
            "case_and_underscores_survive",
            "seed_column_unsuffixed",
            "seed_column_takes_the_same_suffix",
        ],
    )
    def test_the_profile_is_appended_verbatim(self, resolve, base, profile, expected):
        """The resolved name is the base plus the profile, spelled as given."""
        assert resolve(base, profile) == expected

    @pytest.mark.parametrize(
        ("resolve", "base", "match"),
        [
            pytest.param(weight_col_for, "hh_wt", "not a hierarchy weight column", id="a_typo"),
            pytest.param(
                weight_col_for,
                "base_weight",
                "not a hierarchy weight column",
                id="seed_column_is_not_a_weight_column",
            ),
            pytest.param(
                seed_col_for,
                "hh_weight",
                "not a seed column",
                id="weight_column_is_not_a_seed_column",
            ),
        ],
    )
    def test_an_undeclared_name_raises(self, resolve, base, match):
        """A stem the hierarchy does not declare is a caller's typo, not a new level."""
        with pytest.raises(ValueError, match=match):
            resolve(base, PROFILE)

    def test_the_key_says_which_side_of_the_edge_it_is_read_from(self):
        """Households are the root and have no key; a DOWN edge carries the parent's id.

        An UP edge carries its own id, identifying the grouping.
        """
        assert LEVELS["households"].key is None
        assert LEVELS["persons"].key == "hh_id"
        assert LEVELS["joint_tours"].key == "joint_tour_id"


def _households(**columns) -> dict[str, pl.DataFrame]:
    """A one-row households table carrying the given weight columns."""
    return {"households": pl.DataFrame({"hh_id": [1], **columns})}


class TestResolution:
    """The named profile's column becomes the base name; the others go."""

    def test_each_consumer_gets_its_own_numbers(self):
        """The same tables, read under two profiles, give two different weights.

        The suffix comes off the profile asked for, and the other profile's column goes.
        """
        tables = _households(**{f"hh_weight_{STRICT}": [7.0], f"hh_weight_{RELAXED}": [9.0]})

        strict = select_profile_columns(tables, STRICT)["households"]
        assert strict["hh_weight"].to_list() == [7.0]
        assert f"hh_weight_{STRICT}" not in strict.columns
        assert f"hh_weight_{RELAXED}" not in strict.columns

        relaxed = select_profile_columns(tables, RELAXED)["households"]
        assert relaxed["hh_weight"].to_list() == [9.0]
        assert f"hh_weight_{STRICT}" not in relaxed.columns
        assert f"hh_weight_{RELAXED}" not in relaxed.columns

    def test_every_level_is_resolved_not_just_households(self):
        """One rule over the whole hierarchy, not a per-table special case."""
        tables = {
            "person_days": pl.DataFrame(
                {"person_day_id": [1], f"person_day_weight_{STRICT}": [3.0]}
            ),
            "tours": pl.DataFrame({"tour_id": [1], f"tour_weight_{STRICT}": [4.0]}),
        }
        out = select_profile_columns(tables, STRICT)
        assert out["person_days"]["person_day_weight"].to_list() == [3.0]
        assert out["tours"]["tour_weight"].to_list() == [4.0]

    def test_the_input_is_not_mutated(self):
        """The caller's frames are left alone, as everywhere else in the gate."""
        tables = _households(**{f"hh_weight_{STRICT}": [7.0]})
        select_profile_columns(tables, STRICT)
        assert f"hh_weight_{STRICT}" in tables["households"].columns


class TestNoSuffixedColumns:
    """A run that weighted a single profile writes the bare names, and is untouched."""

    def test_a_bare_column_is_left_alone(self):
        """Nothing to resolve, and nothing that needs resolving."""
        tables = _households(hh_weight=[5.0])
        out = select_profile_columns(tables, STRICT)["households"]
        assert out["hh_weight"].to_list() == [5.0]

    def test_a_table_with_no_weight_at_all_is_left_alone(self):
        """A table can reach the gate before any weighting step ran."""
        tables = _households()
        assert select_profile_columns(tables, STRICT)["households"].columns == ["hh_id"]

    def test_a_non_weight_table_is_ignored(self):
        """Only the levels that carry weights are considered."""
        tables = {"zones": pl.DataFrame({"taz": [1], "hh_weight_x": [1.0]})}
        assert "hh_weight_x" in select_profile_columns(tables, STRICT)["zones"].columns


class TestTheUnweightedProfile:
    """The case this raises on, because falling back is worse than stopping."""

    def test_a_profile_that_was_never_weighted_raises(self):
        """Rather than reading the bare column, whatever it holds; the message names the fix."""
        tables = _households(**{f"hh_weight_{RELAXED}": [9.0]})
        with pytest.raises(ValueError, match="but not for 'ctramp'") as excinfo:
            select_profile_columns(tables, STRICT)
        message = str(excinfo.value)
        assert RELAXED in message
        assert "weight_profiles" in message

    def test_a_stale_bare_column_does_not_rescue_it(self):
        """A bare column beside another profile's is still not this profile's."""
        tables = _households(hh_weight=[1.0], **{f"hh_weight_{RELAXED}": [9.0]})
        with pytest.raises(ValueError, match="but not for"):
            select_profile_columns(tables, STRICT)

    def test_both_present_prefers_the_profile_and_warns(self, caplog):
        """A bare column can be legitimate loader input, so this is not fatal."""
        tables = _households(hh_weight=[1.0], **{f"hh_weight_{STRICT}": [7.0]})
        with caplog.at_level("WARNING"):
            out = select_profile_columns(tables, STRICT)["households"]
        assert out["hh_weight"].to_list() == [7.0]
        assert any("carries both" in record.message for record in caplog.records)
