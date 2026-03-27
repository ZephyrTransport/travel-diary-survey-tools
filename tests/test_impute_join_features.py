"""Tests for cross-table join features in imputation."""

import polars as pl
import pytest

from processing.imputation.impute_utils import (
    FK_RELATIONSHIPS,
    add_household_agg_features,
    aggregate_from_children,
    join_parent_tables,
    strip_joined_columns,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_households():
    return pl.DataFrame(
        {
            "hh_id": [1, 2, 3],
            "income_bin": [2, 5, 3],
            "residence_type": [1, 2, 1],
            "home_lat": [37.7, 37.8, 37.9],
        }
    )


def _make_persons():
    return pl.DataFrame(
        {
            "person_id": [10, 11, 20, 30],
            "hh_id": [1, 1, 2, 3],
            "age": [3, 4, 5, 2],
            "race": [1, 1, 2, None],
            "gender": [1, 2, 1, None],
            "employment": [1, 1, 2, 3],
            "student": [0, 1, 0, 1],
        }
    )


# ---------------------------------------------------------------------------
# join_parent_tables
# ---------------------------------------------------------------------------


class TestJoinParentTables:
    """Tests for join_parent_tables."""

    def test_joins_household_columns_to_persons(self):
        """Should add household columns to persons."""
        hh = _make_households()
        persons = _make_persons()
        tables = {"households": hh, "persons": persons}

        result, added = join_parent_tables(persons, "persons", tables, ["households"])

        # Should have added household columns (minus hh_id which already exists)
        assert "income_bin" in result.columns
        assert "residence_type" in result.columns
        assert "home_lat" in result.columns
        assert set(added) == {"income_bin", "residence_type", "home_lat"}

        # Values should match via hh_id
        row_p10 = result.filter(pl.col("person_id") == 10)
        assert row_p10["income_bin"][0] == 2
        assert row_p10["residence_type"][0] == 1

        row_p20 = result.filter(pl.col("person_id") == 20)
        assert row_p20["income_bin"][0] == 5

    def test_no_duplicate_columns(self):
        """Should skip columns that already exist on child."""
        hh = pl.DataFrame({"hh_id": [1], "age": [99], "extra": [42]})
        persons = pl.DataFrame({"person_id": [10], "hh_id": [1], "age": [3]})
        tables = {"households": hh, "persons": persons}

        result, added = join_parent_tables(persons, "persons", tables, ["households"])

        # age already exists on persons, so only extra should be added
        assert "extra" in added
        assert "age" not in added
        # Persons' own age should be preserved
        assert result["age"][0] == 3

    def test_missing_parent_table_warns(self):
        """Should warn and skip when parent table is missing."""
        persons = _make_persons()
        tables = {"persons": persons}  # no households

        result, added = join_parent_tables(persons, "persons", tables, ["households"])

        assert added == []
        assert result.shape == persons.shape

    def test_unknown_relationship_raises(self):
        """Should raise ValueError for undefined FK relationship."""
        persons = _make_persons()
        tables = {"persons": persons, "tours": pl.DataFrame({"tour_id": [1]})}

        with pytest.raises(ValueError, match="No foreign key relationship"):
            join_parent_tables(persons, "persons", tables, ["tours"])

    def test_empty_join_tables(self):
        """Should return unchanged df when join_tables is empty."""
        persons = _make_persons()
        tables = {"persons": persons, "households": _make_households()}

        result, added = join_parent_tables(persons, "persons", tables, [])

        assert added == []
        assert result.equals(persons)


# ---------------------------------------------------------------------------
# add_household_agg_features
# ---------------------------------------------------------------------------


class TestAddHouseholdAggFeatures:
    """Tests for add_household_agg_features."""

    def test_mode_excludes_self(self):
        """Mode should be computed from other household members only."""
        df = pl.DataFrame(
            {
                "hh_id": [1, 1, 1],
                "person_id": [10, 11, 12],
                "race": [1, 2, 2],
            }
        )

        result, added = add_household_agg_features(df, ["race"])

        assert "hh_mode_race" in added
        # Person 10: other members have [2, 2] => mode = 2
        assert result.filter(pl.col("person_id") == 10)["hh_mode_race"][0] == 2
        # Person 11: other members have [1, 2] => mode could be 1 or 2
        # Person 12: other members have [1, 2] => mode could be 1 or 2

    def test_single_person_household_null(self):
        """Single-person households should get null for hh_mode."""
        df = pl.DataFrame(
            {
                "hh_id": [1, 2],
                "person_id": [10, 20],
                "race": [1, 2],
            }
        )

        result, added = add_household_agg_features(df, ["race"])

        assert "hh_mode_race" in added
        # Both are single-person households, so hh_mode should be null
        assert result["hh_mode_race"].null_count() == 2

    def test_all_null_target_column(self):
        """Should produce null agg when all target values are null."""
        df = pl.DataFrame(
            {
                "hh_id": [1, 1],
                "person_id": [10, 11],
                "race": [None, None],
            }
        )

        result, added = add_household_agg_features(df, ["race"])

        assert "hh_mode_race" in added
        assert result["hh_mode_race"].null_count() == 2

    def test_missing_hh_or_person_id_skips(self):
        """Should skip gracefully if hh_id or person_id is missing."""
        df = pl.DataFrame({"some_col": [1, 2, 3]})

        result, added = add_household_agg_features(df, ["some_col"])

        assert added == []
        assert result.equals(df)

    def test_multiple_target_columns(self):
        """Should add agg features for each target column."""
        df = pl.DataFrame(
            {
                "hh_id": [1, 1],
                "person_id": [10, 11],
                "race": [1, 2],
                "ethnicity": [3, 4],
            }
        )

        _, added = add_household_agg_features(df, ["race", "ethnicity"])

        assert "hh_mode_race" in added
        assert "hh_mode_ethnicity" in added
        assert len(added) == 2

    def test_nonexistent_target_column_skipped(self):
        """Should skip target columns that don't exist in df."""
        df = pl.DataFrame(
            {
                "hh_id": [1, 1],
                "person_id": [10, 11],
                "race": [1, 2],
            }
        )

        _, added = add_household_agg_features(df, ["race", "nonexistent"])

        assert "hh_mode_race" in added
        assert "hh_mode_nonexistent" not in added


# ---------------------------------------------------------------------------
# strip_joined_columns
# ---------------------------------------------------------------------------


class TestStripJoinedColumns:
    """Tests for strip_joined_columns."""

    def test_removes_added_columns(self):
        """Should remove specified columns."""
        df = pl.DataFrame(
            {
                "id": [1, 2],
                "keep": [10, 20],
                "temp_a": [100, 200],
                "temp_b": [300, 400],
            }
        )

        result = strip_joined_columns(df, ["temp_a", "temp_b"])

        assert "temp_a" not in result.columns
        assert "temp_b" not in result.columns
        assert "id" in result.columns
        assert "keep" in result.columns

    def test_handles_missing_column_gracefully(self):
        """Should not fail if a column to strip doesn't exist."""
        df = pl.DataFrame({"id": [1], "val": [2]})

        result = strip_joined_columns(df, ["nonexistent", "also_missing"])

        assert result.equals(df)


# ---------------------------------------------------------------------------
# FK_RELATIONSHIPS
# ---------------------------------------------------------------------------


class TestFKRelationships:
    """Tests for the FK_RELATIONSHIPS constant."""

    def test_persons_to_households(self):
        """Should have correct FK relationship from persons to households."""
        assert FK_RELATIONSHIPS[("persons", "households")] == "hh_id"

    def test_days_to_persons(self):
        """Should have correct FK relationship from days to persons."""
        assert FK_RELATIONSHIPS[("days", "persons")] == "person_id"

    def test_trips_to_households(self):
        """Should have correct FK relationship from trips to households."""
        assert FK_RELATIONSHIPS[("unlinked_trips", "households")] == "hh_id"
        assert FK_RELATIONSHIPS[("linked_trips", "households")] == "hh_id"

    def test_tours_to_persons(self):
        """Should have correct FK relationship from tours to persons."""
        assert FK_RELATIONSHIPS[("tours", "persons")] == "person_id"


# ---------------------------------------------------------------------------
# End-to-end: join → impute → strip
# ---------------------------------------------------------------------------


class TestJoinImputeStripLifecycle:
    """Integration test for the full join → impute → strip lifecycle."""

    def test_full_lifecycle_preserves_original_schema(self):
        """After join and strip, df should have same columns as original."""
        hh = _make_households()
        persons = _make_persons()
        tables = {"households": hh, "persons": persons}

        original_cols = set(persons.columns)

        # Join
        enriched, added = join_parent_tables(persons, "persons", tables, ["households"])
        assert len(enriched.columns) > len(original_cols)

        # Add agg features
        enriched, agg_added = add_household_agg_features(enriched, ["race", "gender"])
        added.extend(agg_added)

        # Strip
        result = strip_joined_columns(enriched, added)

        assert set(result.columns) == original_cols
        assert len(result) == len(persons)


# ---------------------------------------------------------------------------
# aggregate_from_children (child → parent pivot counts)
# ---------------------------------------------------------------------------


class TestAggregateFromChildren:
    """Tests for aggregate_from_children."""

    def test_basic_pivot_count(self):
        """Should create one column per unique value in the child field."""
        hh = _make_households()
        persons = _make_persons()
        tables = {"households": hh, "persons": persons}

        config = {"persons": {"pivot_count": ["employment"]}}
        result, added = aggregate_from_children(hh, "households", tables, config)

        # employment has values 1, 2, 3 → 3 columns
        assert "persons_count_employment_1" in added
        assert "persons_count_employment_2" in added
        assert "persons_count_employment_3" in added
        assert len(added) == 3

        # hh 1 has persons 10 (emp=1) and 11 (emp=1) → count_1=2, count_2=0, count_3=0
        row_hh1 = result.filter(pl.col("hh_id") == 1)
        assert row_hh1["persons_count_employment_1"][0] == 2
        assert row_hh1["persons_count_employment_2"][0] == 0
        assert row_hh1["persons_count_employment_3"][0] == 0

        # hh 2 has person 20 (emp=2) → count_1=0, count_2=1, count_3=0
        row_hh2 = result.filter(pl.col("hh_id") == 2)
        assert row_hh2["persons_count_employment_1"][0] == 0
        assert row_hh2["persons_count_employment_2"][0] == 1

        # hh 3 has person 30 (emp=3) → count_3=1
        row_hh3 = result.filter(pl.col("hh_id") == 3)
        assert row_hh3["persons_count_employment_3"][0] == 1

    def test_pivot_count_sum_equals_household_size(self):
        """Sum of pivot counts should equal the number of persons in household."""
        hh = _make_households()
        persons = _make_persons()
        tables = {"households": hh, "persons": persons}

        config = {"persons": {"pivot_count": ["employment"]}}
        result, added = aggregate_from_children(hh, "households", tables, config)

        # hh 1 has 2 persons, hh 2 has 1, hh 3 has 1
        sums = result.select(
            pl.col("hh_id"),
            pl.sum_horizontal([pl.col(c) for c in added]).alias("total"),
        )
        assert sums.filter(pl.col("hh_id") == 1)["total"][0] == 2
        assert sums.filter(pl.col("hh_id") == 2)["total"][0] == 1
        assert sums.filter(pl.col("hh_id") == 3)["total"][0] == 1

    def test_multiple_pivot_count_fields(self):
        """Should handle multiple fields in pivot_count."""
        hh = _make_households()
        persons = _make_persons()
        tables = {"households": hh, "persons": persons}

        config = {"persons": {"pivot_count": ["employment", "student"]}}
        _, added = aggregate_from_children(hh, "households", tables, config)

        emp_cols = [c for c in added if "employment" in c]
        stu_cols = [c for c in added if "student" in c]
        assert len(emp_cols) == 3  # values 1, 2, 3
        assert len(stu_cols) == 2  # values 0, 1

    def test_missing_child_table_warns(self):
        """Should warn and skip when child table is not available."""
        hh = _make_households()
        tables = {"households": hh}  # no persons

        config = {"persons": {"pivot_count": ["employment"]}}
        result, added = aggregate_from_children(hh, "households", tables, config)

        assert added == []
        assert result.shape == hh.shape

    def test_missing_field_warns(self):
        """Should warn and skip fields that don't exist on child table."""
        hh = _make_households()
        persons = _make_persons()
        tables = {"households": hh, "persons": persons}

        config = {"persons": {"pivot_count": ["nonexistent"]}}
        _, added = aggregate_from_children(hh, "households", tables, config)

        assert added == []

    def test_unknown_relationship_raises(self):
        """Should raise ValueError for undefined FK relationship."""
        hh = _make_households()
        tours = pl.DataFrame({"tour_id": [1]})
        tables = {"households": hh, "tours": tours}

        # tours -> households is defined, but tours -> tours is not
        # Let's try households -> tours which is not defined
        config = {"households": {"pivot_count": ["hh_id"]}}
        with pytest.raises(ValueError, match="No foreign key relationship"):
            aggregate_from_children(hh, "persons", tables, config)

    def test_households_with_no_children_get_zeros(self):
        """Parent rows with no matching children should have 0 counts."""
        hh = pl.DataFrame({"hh_id": [1, 2, 99]})  # hh 99 has no persons
        persons = pl.DataFrame(
            {
                "person_id": [10, 20],
                "hh_id": [1, 2],
                "employment": [1, 2],
            }
        )
        tables = {"households": hh, "persons": persons}

        config = {"persons": {"pivot_count": ["employment"]}}
        result, added = aggregate_from_children(hh, "households", tables, config)

        row_99 = result.filter(pl.col("hh_id") == 99)
        for col in added:
            assert row_99[col][0] == 0

    def test_strip_after_aggregate(self):
        """Added pivot columns should be cleanly strippable."""
        hh = _make_households()
        persons = _make_persons()
        tables = {"households": hh, "persons": persons}
        original_cols = set(hh.columns)

        config = {"persons": {"pivot_count": ["employment", "student"]}}
        enriched, added = aggregate_from_children(hh, "households", tables, config)
        assert len(enriched.columns) > len(original_cols)

        result = strip_joined_columns(enriched, added)
        assert set(result.columns) == original_cols
