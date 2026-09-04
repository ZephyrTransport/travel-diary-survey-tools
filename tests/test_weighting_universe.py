"""Which universes a weighting run is being asked to weight.

Both weighting entry points resolve this through one function, so they cannot
disagree about what naming a flag, a profile list, or both is meant to mean. The
cases that must be refused are all ones where the config is ambiguous rather than
wrong in a way the run would notice.
"""

import pytest

from processing.weighting.core.specs import WeightingConfig, resolve_fitted_profiles


def _config(**overrides) -> WeightingConfig:
    """A WeightingConfig with only the universe settings that matter here."""
    return WeightingConfig(geography={}, state_fips="06", pums_year=2023, **overrides)


class TestResolving:
    """What the two spellings resolve to."""

    def test_a_single_flag_means_one_unsuffixed_set(self):
        """``(None,)`` is what makes every column keep its declared name."""
        assert resolve_fitted_profiles("ctramp_usable", None) == (None,)

    def test_profiles_are_returned_in_the_order_given(self):
        """Fits run in this order, and their reports are read in it."""
        assert resolve_fitted_profiles(None, ["b", "a"]) == ("b", "a")

    def test_an_empty_profile_list_is_not_a_universe(self):
        """An empty list reads as "no opinion", which is the unstated case."""
        with pytest.raises(ValueError, match="needs a universe"):
            resolve_fitted_profiles(None, [])


class TestRefusals:
    """An ambiguous universe is refused rather than picked for the user."""

    def test_naming_both_is_refused(self):
        """They say different things about what gets fitted and what gets written."""
        with pytest.raises(ValueError, match="Name either usability_flag_col"):
            resolve_fitted_profiles("ctramp_usable", ["ctramp_usable"])

    def test_naming_neither_is_refused(self):
        """With several profiles stamped there is no defensible default."""
        with pytest.raises(ValueError, match="needs a universe"):
            resolve_fitted_profiles(None, None)

    def test_a_repeated_profile_is_refused(self):
        """The second fit would overwrite the first under the same column names."""
        with pytest.raises(ValueError, match="same profile twice"):
            resolve_fitted_profiles(None, ["a", "b", "a"])

    def test_the_message_names_what_was_given(self):
        """So the fix is visible without going back to the config."""
        with pytest.raises(ValueError, match="Name either") as excinfo:
            resolve_fitted_profiles("x", ["y"])
        assert "'x'" in str(excinfo.value)
        assert "y" in str(excinfo.value)


class TestTheConfigUsesIt:
    """WeightingConfig delegates, and adds only the rule that is its own."""

    def test_a_single_flag_config_exposes_one_unsuffixed_fit(self):
        """Today's config, whose columns keep their declared names."""
        assert _config(usability_flag_col="ctramp_usable").fitted_profiles == (None,)

    def test_a_profile_config_exposes_one_fit_each(self):
        """One balancing run per profile, in the order configured."""
        assert _config(weight_profiles=("a", "b")).fitted_profiles == ("a", "b")

    def test_flag_for_answers_both_spellings(self):
        """A profile gates on itself; the single-flag case gates on the flag."""
        assert _config(weight_profiles=("a",)).flag_for("a") == "a"
        assert _config(usability_flag_col="x").flag_for(None) == "x"

    def test_profiles_without_the_gate_are_refused(self):
        """Every fit would then see the same seed and differ in name only.

        That is the one outcome this whole change exists to prevent: weights
        indistinguishable from fitted ones that match no control total.
        """
        with pytest.raises(ValueError, match="requires exclude_incompletes"):
            _config(weight_profiles=("a",), exclude_incompletes=False)

    def test_a_single_flag_without_the_gate_is_still_allowed(self):
        """It weights every complete household, which is a coherent thing to ask."""
        assert _config(
            usability_flag_col="complete", exclude_incompletes=False
        ).fitted_profiles == (None,)
