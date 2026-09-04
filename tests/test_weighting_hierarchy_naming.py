"""The per-profile weight-column naming rule.

A run may weight several usability profiles, and each profile's columns are its
base name with the profile appended verbatim. These tests pin the two properties
that make that safe: the suffix is not transformed on the way in, and it never
reaches the id columns that are derived from ``weight_col``.
"""

import pytest

from processing.weighting.core.hierarchy import (
    HIERARCHY,
    LEVELS,
    WEIGHT_COLUMNS,
    seed_col_for,
    weight_col_for,
    weight_columns_for,
)

PROFILE = "analysis_usable"


class TestNoProfile:
    """``None`` must reproduce the single un-suffixed column set exactly."""

    @pytest.mark.parametrize("level", HIERARCHY, ids=lambda level: level.table)
    def test_base_name_is_returned_unchanged(self, level):
        """Both spellings of the resolver give back the declared name."""
        assert weight_col_for(level.weight_col, None) == level.weight_col
        assert level.weight_col_for(None) == level.weight_col

    def test_weight_columns_projection_matches(self):
        """The published projection is the profile-less resolution."""
        assert weight_columns_for(None) == WEIGHT_COLUMNS

    def test_seed_column_is_returned_unchanged(self):
        """base_weight is un-suffixed when no profile was fitted."""
        assert seed_col_for("base_weight", None) == "base_weight"


class TestVerbatimSuffix:
    """The profile name is appended as given -- no stripping, no case change."""

    def test_profile_is_not_abbreviated(self):
        """The trailing _usable every profile name happens to carry is kept."""
        # The obvious "tidier" rule would drop the _usable that every profile
        # name in practice ends with. It must not.
        assert weight_col_for("day_weight", PROFILE) == "day_weight_analysis_usable"
        assert weight_col_for("hh_weight", "ctramp_usable") == "hh_weight_ctramp_usable"

    def test_case_and_underscores_survive(self):
        """A profile name is a label, not something to normalise."""
        assert weight_col_for("tour_weight", "Weird_Name_v2") == "tour_weight_Weird_Name_v2"

    def test_seed_column_takes_the_same_suffix(self):
        """base_weight belongs to a fit, so it is suffixed like the weights."""
        assert seed_col_for("base_weight", PROFILE) == "base_weight_analysis_usable"

    @pytest.mark.parametrize("level", HIERARCHY, ids=lambda level: level.table)
    def test_every_level_resolves(self, level):
        """Both spellings agree, for every table in the hierarchy."""
        assert level.weight_col_for(PROFILE) == f"{level.weight_col}_{PROFILE}"
        assert weight_columns_for(PROFILE)[level.table] == f"{level.weight_col}_{PROFILE}"


class TestUndeclaredNames:
    """A stem the hierarchy does not declare is a caller's typo, not a new level."""

    def test_unknown_weight_base_raises(self):
        """An undeclared stem is rejected rather than suffixed."""
        with pytest.raises(ValueError, match="not a hierarchy weight column"):
            weight_col_for("hh_wt", PROFILE)

    def test_seed_column_is_not_a_weight_column(self):
        """base_weight is not a level, so the weight resolver refuses it."""
        with pytest.raises(ValueError, match="not a hierarchy weight column"):
            weight_col_for("base_weight", PROFILE)

    def test_weight_column_is_not_a_seed_column(self):
        """And the seed resolver refuses a level's weight, symmetrically."""
        with pytest.raises(ValueError, match="not a seed column"):
            seed_col_for("hh_weight", PROFILE)


class TestIdColumnsAreUnaffected:
    """The suffix must not reach ``id_col`` or ``key``.

    Both are read off ``weight_col`` by stripping ``_weight``, so a suffixed
    stem would silently produce ``hh_weight_ctramp_usable_id``. Keeping the
    suffix out of ``Level`` is what prevents that.
    """

    @pytest.mark.parametrize("profile", [None, PROFILE, "ctramp_usable"])
    @pytest.mark.parametrize("level", HIERARCHY, ids=lambda level: level.table)
    def test_id_col_and_key_do_not_move(self, level, profile):
        """Resolving a weight column leaves the id columns where they were."""
        level.weight_col_for(profile)
        assert level.id_col == LEVELS[level.table].id_col
        assert "_weight" not in level.id_col
        assert level.key is None or "_weight" not in level.key

    def test_ids_are_the_table_keys_we_expect(self):
        """Spot-check the derivation against the keys we know."""
        assert LEVELS["households"].id_col == "hh_id"
        assert LEVELS["unlinked_trips"].id_col == "unlinked_trip_id"
        assert LEVELS["joint_tours"].id_col == "joint_tour_id"
