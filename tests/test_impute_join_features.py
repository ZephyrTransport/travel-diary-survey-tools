"""Tests for cross-table join features in imputation."""

import polars as pl
import pytest

from processing.imputation.impute_utils import (
    add_household_agg_features,
    aggregate_from_children,
    enrich_dataframe,
    join_parent_tables,
)


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


@pytest.fixture
def tables() -> dict:
    """Three households and the four persons hanging off them."""
    return {"households": _make_households(), "persons": _make_persons()}


class TestJoinParentTables:
    """Tests for join_parent_tables."""

    def test_joins_household_columns_to_persons(self, tables):
        """Household columns other than the key are joined on hh_id and listed as added."""
        persons = tables["persons"]
        result, added = join_parent_tables(persons, "persons", tables, ["households"])

        assert set(added) == {"income_bin", "residence_type", "home_lat"}
        assert set(result.columns) == set(persons.columns) | set(added)

        row_p10 = result.filter(pl.col("person_id") == 10)
        assert row_p10["income_bin"][0] == 2
        assert row_p10["residence_type"][0] == 1

        row_p20 = result.filter(pl.col("person_id") == 20)
        assert row_p20["income_bin"][0] == 5

    def test_no_duplicate_columns(self):
        """A household column the person frame already has is not joined over it."""
        hh = pl.DataFrame({"hh_id": [1], "age": [99], "extra": [42]})
        persons = pl.DataFrame({"person_id": [10], "hh_id": [1], "age": [3]})
        tables = {"households": hh, "persons": persons}

        result, added = join_parent_tables(persons, "persons", tables, ["households"])

        assert "extra" in added
        assert "age" not in added
        assert result["age"][0] == 3

    def test_missing_parent_table_warns(self):
        """A join table that is not loaded is skipped."""
        persons = _make_persons()
        tables = {"persons": persons}

        result, added = join_parent_tables(persons, "persons", tables, ["households"])

        assert added == []
        assert result.shape == persons.shape

    def test_unknown_relationship_raises(self):
        """Joining a table with no declared foreign key raises."""
        persons = _make_persons()
        tables = {"persons": persons, "tours": pl.DataFrame({"tour_id": [1]})}

        with pytest.raises(ValueError, match="No foreign key relationship"):
            join_parent_tables(persons, "persons", tables, ["tours"])

    def test_empty_join_tables(self):
        """No join tables returns the frame unchanged."""
        persons = _make_persons()
        tables = {"persons": persons, "households": _make_households()}

        result, added = join_parent_tables(persons, "persons", tables, [])

        assert added == []
        assert result.equals(persons)


class TestAddHouseholdAggFeatures:
    """Tests for add_household_agg_features."""

    def test_counts_the_other_members(self):
        """Each person counts the other members' codes, never their own."""
        df = pl.DataFrame(
            {
                "hh_id": [1, 1, 1],
                "person_id": [10, 11, 12],
                "race": [1, 2, 2],
            }
        )

        result, added = add_household_agg_features(df, ["race"])

        assert added == ["n_hh_race=1", "n_hh_race=2"]
        counts = result.sort("person_id").select(added).rows()
        assert counts == [(0, 2), (1, 1), (1, 1)]

    def test_alone_or_with_unknown_members_counts_zero(self):
        """Nobody else, or only members with a null value, counts zero."""
        df = pl.DataFrame(
            {
                "hh_id": [1, 2, 2],
                "person_id": [10, 20, 21],
                "race": [1, 2, None],
            }
        )

        result, added = add_household_agg_features(df, ["race"])

        counts = result.sort("person_id").select(added).rows()
        assert counts == [(0, 0), (0, 0), (0, 1)]

    def test_all_null_target_adds_nothing(self):
        """A column with no values has no codes to count."""
        df = pl.DataFrame(
            {
                "hh_id": [1, 1],
                "person_id": [10, 11],
                "race": pl.Series([None, None], dtype=pl.Int64),
            }
        )

        result, added = add_household_agg_features(df, ["race"])

        assert added == []
        assert result.equals(df)

    def test_row_order_does_not_change_the_counts(self):
        """The same persons in another order get the same counts."""
        df = pl.DataFrame(
            {
                "hh_id": [1, 1, 1, 2, 2],
                "person_id": [10, 11, 12, 20, 21],
                "race": [1, 2, 2, 3, 1],
            }
        )

        result, _ = add_household_agg_features(df, ["race"])
        shuffled, _ = add_household_agg_features(
            df.sample(fraction=1.0, shuffle=True, seed=7), ["race"]
        )

        assert shuffled.sort("person_id").equals(result.sort("person_id"))

    def test_missing_hh_or_person_id_skips(self):
        """A frame without hh_id and person_id is returned unchanged."""
        df = pl.DataFrame({"some_col": [1, 2, 3]})

        result, added = add_household_agg_features(df, ["some_col"])

        assert added == []
        assert result.equals(df)

    @pytest.mark.parametrize(
        ("targets", "expected_added"),
        [
            pytest.param(["race"], ["n_hh_race=1", "n_hh_race=2"], id="one-column"),
            pytest.param(
                ["race", "ethnicity"],
                [
                    "n_hh_race=1",
                    "n_hh_race=2",
                    "n_hh_ethnicity=3",
                    "n_hh_ethnicity=4",
                ],
                id="two-columns",
            ),
            pytest.param(
                ["race", "nonexistent"],
                ["n_hh_race=1", "n_hh_race=2"],
                id="one-column-absent",
            ),
        ],
    )
    def test_one_count_column_per_code_of_each_existing_target(
        self, targets: list[str], expected_added: list[str]
    ):
        """One count column per code of each target present; absent targets are skipped."""
        df = pl.DataFrame(
            {
                "hh_id": [1, 1],
                "person_id": [10, 11],
                "race": [1, 2],
                "ethnicity": [3, 4],
            }
        )

        result, added = add_household_agg_features(df, targets)

        assert added == expected_added
        assert set(result.columns) == set(df.columns) | set(expected_added)


class TestAggregateFromChildren:
    """Tests for aggregate_from_children."""

    def test_basic_pivot_count(self, tables):
        """Each household counts its persons per employment code, one column per code."""
        config = {"persons": {"pivot_count": ["employment"]}}
        result, added = aggregate_from_children(tables["households"], "households", tables, config)

        assert added == [
            "persons_count_employment=1",
            "persons_count_employment=2",
            "persons_count_employment=3",
        ]
        assert result.sort("hh_id").select(added).rows() == [(2, 0, 0), (0, 1, 0), (0, 0, 1)]

    def test_missing_child_table_warns(self):
        """A pivot table that is not loaded is skipped."""
        hh = _make_households()
        tables = {"households": hh}

        config = {"persons": {"pivot_count": ["employment"]}}
        result, added = aggregate_from_children(hh, "households", tables, config)

        assert added == []
        assert result.shape == hh.shape

    def test_missing_field_warns(self, tables):
        """A pivot field the table does not have is skipped."""
        hh = tables["households"]

        config = {"persons": {"pivot_count": ["nonexistent"]}}
        _, added = aggregate_from_children(hh, "households", tables, config)

        assert added == []

    def test_unknown_relationship_raises(self):
        """Aggregating households onto persons, a direction with no foreign key, raises."""
        hh = _make_households()
        tables = {"households": hh, "tours": pl.DataFrame({"tour_id": [1]})}

        config = {"households": {"pivot_count": ["hh_id"]}}
        with pytest.raises(ValueError, match="No foreign key relationship"):
            aggregate_from_children(hh, "persons", tables, config)

    def test_households_with_no_children_get_zeros(self):
        """A household with no persons counts zero."""
        hh = pl.DataFrame({"hh_id": [1, 2, 99]})
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

    def test_multiple_pivot_count_fields_are_added_then_strippable(self, tables):
        """Every field in pivot_count adds its own columns, and only those."""
        hh = tables["households"]
        original_cols = set(hh.columns)

        config = {"persons": {"pivot_count": ["employment", "student"]}}
        enriched, added = aggregate_from_children(hh, "households", tables, config)

        assert [c for c in added if "employment" in c] == [
            "persons_count_employment=1",
            "persons_count_employment=2",
            "persons_count_employment=3",
        ]
        assert [c for c in added if "student" in c] == [
            "persons_count_student=0",
            "persons_count_student=1",
        ]

        assert set(enriched.columns) == original_cols | set(added)


def test_household_counts_survive_the_validation_pass():
    """Imputation and its validation pass build the same household count columns."""
    from processing.imputation.impute import imputation  # noqa: PLC0415

    n_hh = 30
    hh_ids = [h for h in range(1, n_hh + 1) for _ in range(3)]
    # 995 (missing) and 4 (non-binary) are imputed.
    genders = [[2, 1, 1], [1, 2, 995], [2, 2, 4], [1, 1, 2], [995, 2, 1]]
    persons = pl.DataFrame(
        {
            "hh_id": hh_ids,
            "person_id": [h * 10 + i for h in range(1, n_hh + 1) for i in range(3)],
            "gender": [g for h in range(n_hh) for g in genders[h % len(genders)]],
            "age": [(h + i) % 6 + 1 for h in range(n_hh) for i in range(3)],
        }
    )
    households = pl.DataFrame({"hh_id": list(range(1, n_hh + 1)), "residence_type": [1, 2] * 15})

    result = imputation(
        households=households,
        persons=persons,
        impute_columns={
            "persons": [
                {
                    "column": "gender",
                    "method": "rf",
                    "missing_values": ["NON_BINARY", "MISSING"],
                    "join_tables": ["households"],
                    "categorical_features": ["age", "residence_type"],
                }
            ]
        },
        stash_preimputed=False,
        random_state=42,
        validate_imputation={"enabled": True, "n_folds": 2, "sample_pct": 50.0},
        validate_input=False,
    )

    out = result["persons"]
    assert set(out["gender"].unique().to_list()) <= {1, 2}
    assert not [c for c in out.columns if c.startswith("n_hh_")]


@pytest.mark.parametrize(
    ("table", "config", "prefix"),
    [
        pytest.param(
            "persons", {"join_tables": ["households"]}, "n_hh_race=", id="household-counts"
        ),
        pytest.param(
            "households",
            {"aggregate_from": {"persons": {"pivot_count": ["race"]}}},
            "persons_count_race=",
            id="pivot-counts",
        ),
    ],
)
def test_counts_are_numeric_features(table, config, prefix):
    """Added counts go in as numbers, not as one 0/1 column per count value."""
    tables = {
        "households": pl.DataFrame({"hh_id": [1, 2], "residence_type": [1, 2]}),
        "persons": pl.DataFrame({"hh_id": [1, 1, 2], "person_id": [10, 11, 20], "race": [1, 2, 2]}),
    }

    _, added, numeric = enrich_dataframe(
        tables[table],
        table,
        tables,
        config.get("join_tables", []),
        ["race"],
        config.get("aggregate_from"),
        ["num_trips"],
    )

    counts = [c for c in added if c.startswith(prefix)]
    assert counts
    assert numeric == ["num_trips", *counts]
