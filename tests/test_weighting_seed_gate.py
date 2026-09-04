"""Who the balancer fits: the profile's flag, floored by ``complete``.

This is the expression the whole per-profile change turns on. Filtering the seed
by the profile is what makes each fit spread its zone's population over the
households that will keep a weight; gating afterwards instead deletes fitted mass
that nothing re-spreads, because households are the hierarchy anchor.

It is unit-tested here because the only callers are inside ``compute_weights``,
which fetches PUMS and so cannot run in the e2e. A mutation replacing this with
``complete`` alone survived the entire suite until these existed.
"""

import polars as pl
import pytest

from processing.weighting.core.propagation import is_usable, seed_admits

# Every combination of the two inputs, nulls included.
FRAME = pl.DataFrame(
    {
        "case": ["both", "flag only", "complete only", "neither", "null flag", "null complete"],
        "profile": [True, True, False, False, None, True],
        "complete": [True, False, True, False, True, None],
    }
)


def _admitted(flag: str = "profile") -> list[str]:
    """The cases *flag* admits into the seed."""
    return FRAME.filter(seed_admits(flag))["case"].to_list()


class TestTheGate:
    """Both conditions must hold, and a null is not a yes."""

    def test_a_complete_household_the_profile_admits_is_seeded(self):
        """The one case that should reach the balancer."""
        assert "both" in _admitted()

    def test_the_profile_alone_is_not_enough(self):
        """The floor holds even where the cascade would never break it.

        A profile is a subset of ``complete`` by construction, so this case cannot
        come from the cascade -- but a hand-written flag is not bound by that.
        """
        assert "flag only" not in _admitted()

    def test_completeness_alone_is_not_enough(self):
        """The whole point: the seed is the profile's universe, not the survey's."""
        assert "complete only" not in _admitted()

    def test_neither_is_refused(self):
        """Neither condition met, so nothing to weight."""
        assert "neither" not in _admitted()

    @pytest.mark.parametrize("case", ["null flag", "null complete"])
    def test_a_null_is_not_a_yes(self, case):
        """A null means the cascade never reached the row, not that it passed."""
        assert case not in _admitted()

    def test_only_the_fully_qualified_case_is_admitted(self):
        """Stated as the whole set, so a new admission cannot slip in unnoticed."""
        assert _admitted() == ["both"]


class TestGatingOnCompletenessItself:
    """``complete`` is a legitimate thing to weight, and is not floored by itself."""

    def test_every_complete_household_is_seeded(self):
        """Including ones no profile admits -- that is what asking for it means."""
        assert _admitted("complete") == ["both", "complete only", "null flag"]

    def test_it_does_not_require_a_profile_column(self):
        """A survey-analysis run may have no profile columns at all."""
        frame = pl.DataFrame({"complete": [True, False]})
        assert frame.filter(seed_admits("complete")).height == 1


class TestAgainstTheZeroingPredicate:
    """The seed and the zeroing must agree, or a null appears where a zero belongs."""

    def test_it_is_stricter_than_the_usability_flag_alone(self):
        """The seed gate is the one that has to carry completeness itself.

        ``is_usable`` gates records below the anchor, where completeness has
        already been settled by the time the weight arrives.
        """
        usable = FRAME.filter(is_usable("profile")).height
        seeded = FRAME.filter(seed_admits("profile")).height
        assert usable > seeded

    def test_everything_it_admits_is_also_usable(self):
        """So a seeded household is never zeroed by the propagation that follows."""
        seeded = FRAME.filter(seed_admits("profile"))["case"].to_list()
        usable = FRAME.filter(is_usable("profile"))["case"].to_list()
        assert set(seeded) <= set(usable)
