"""Tests for cross-tab control targets.

Exercises the full cross-tab pipeline: dynamic registration from YAML config,
composite expression recode, incidence pivot, and N-D merges.
"""

import polars as pl
import pytest

from data_canon.codebook.households import IncomeBroad
from processing.weighting.controls.base import CrosstabControlTarget
from processing.weighting.controls.enums import HHSizeCategory
from processing.weighting.controls.registry import CONTROLS, register_crosstab
from processing.weighting.data_prep.incidence import (
    aggregate_control_totals,
    build_incidence_table,
)
from processing.weighting.data_prep.merges import apply_1d_merges, apply_crosstab_merges
from processing.weighting.data_prep.seed_data import recode_survey_households
from processing.weighting.specs import ControlRegistryConfig, MergeSpec
from processing.weighting.validation.control_validation import warn_crosstab_sparsity


@pytest.fixture(autouse=True)
def _clean_registry():
    """Remove dynamically registered cross-tabs after each test."""
    before = set(CONTROLS.keys())
    yield
    for name in list(CONTROLS.keys()):
        if name not in before:
            del CONTROLS[name]


@pytest.fixture
def size_income_xtab():
    """Register and return a h_size x h_income cross-tab control."""
    return register_crosstab("h_size_x_income", ["h_size", "h_income"])


@pytest.fixture
def households() -> pl.DataFrame:
    """Synthetic canonical households (3 HHs, 2 zones)."""
    return pl.DataFrame(
        {
            "hh_id": [1, 2, 3, 4],
            "income_bin": [1, 5, 3, 2],  # UNDER25, 100TO200, 50TO75, 25TO50
            "ctrl_geoid": ["Z1", "Z1", "Z2", "Z2"],
        }
    )


@pytest.fixture
def persons() -> pl.DataFrame:
    """Synthetic persons — defines household size."""
    return pl.DataFrame(
        {
            "hh_id": [1, 2, 2, 3, 3, 3, 4],
            "person_id": [101, 201, 202, 301, 302, 303, 401],
            "age": [5, 5, 5, 5, 5, 5, 5],  # adult ages (AgeCategory values)
            "gender": [2, 1, 2, 1, 2, 1, 2],  # canonical Gender values
            "employment": [5, 5, 5, 5, 5, 5, 5],  # not employed
            "student": [2, 2, 2, 2, 2, 2, 2],  # non-student
            "school_type": [None, None, None, None, None, None, None],
            "work_mode": [None, None, None, None, None, None, None],
        }
    )


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("size_income_xtab")
class TestCrosstabRegistration:
    """Tests for dynamic cross-tab control registration."""

    def test_register_creates_control(self):
        """Cross-tab is registered in the global CONTROLS dict."""
        assert "h_size_x_income" in CONTROLS
        assert isinstance(CONTROLS["h_size_x_income"], CrosstabControlTarget)

    def test_register_sets_dimensions(self):
        """Dimension controls are correctly linked."""
        ctrl = CONTROLS["h_size_x_income"]
        assert len(ctrl.dim_controls) == 2
        assert ctrl.dim_controls[0].name == "h_size"
        assert ctrl.dim_controls[1].name == "h_income"

    def test_register_creates_composite_enum(self):
        """Composite enum has full cartesian product of dimension members."""
        ctrl = CONTROLS["h_size_x_income"]
        n_size = len(list(HHSizeCategory))
        n_income = len([m for m in IncomeBroad if m.name not in ("MISSING", "PNTA")])
        assert len(ctrl.valid_members) == n_size * n_income

    def test_register_duplicate_raises(self):
        """Re-registering the same name raises ValueError."""
        with pytest.raises(ValueError, match="already exists"):
            register_crosstab("h_size_x_income", ["h_size", "h_income"])

    def test_register_with_merges_reduces_cell_count(self):
        """Pre-merge at registration reduces the enum to effective cell count."""
        xtab = register_crosstab(
            "h_size_income_merged",
            ["h_size", "h_income"],
            merges={
                "h_income": {
                    "income_under_100": [
                        "income_under25",
                        "income_25to50",
                        "income_50to75",
                        "income_75to100",
                    ],
                },
                "h_size": {
                    "size_3_plus": [
                        "size_3",
                        "size_4",
                        "size_5",
                        "size_6",
                        "size_7",
                        "size_8",
                        "size_9",
                        "size_10_plus",
                    ],
                },
            },
        )
        # 3 size bins x 3 income bins = 9 cells
        assert len(xtab.valid_members) == 9

    def test_register_with_merges_enum_names(self):
        """Merged enum members have composite names from effective groups."""
        xtab = register_crosstab(
            "h_size_income_named",
            ["h_size", "h_income"],
            merges={
                "h_income": {
                    "income_under_100": [
                        "income_under25",
                        "income_25to50",
                        "income_50to75",
                        "income_75to100",
                    ],
                },
                "h_size": {
                    "size_3_plus": [
                        "size_3",
                        "size_4",
                        "size_5",
                        "size_6",
                        "size_7",
                        "size_8",
                        "size_9",
                        "size_10_plus",
                    ],
                },
            },
        )
        member_names = [name for _, name in xtab.valid_members]
        # Should include merged group names like SIZE_3_PLUS_INCOME_UNDER_100
        assert any("SIZE_3_PLUS" in n for n in member_names)
        assert any("INCOME_UNDER_100" in n for n in member_names)

    def test_from_yaml_parses_dimensions(self):
        """Cross-tab specs parse dimensions from YAML config."""
        config = [
            {"name": "h_size"},
            {"name": "h_income"},
            {"name": "h_size_by_income", "dimensions": ["h_size", "h_income"]},
        ]
        parsed = ControlRegistryConfig.from_yaml(config)
        xtab_spec = next(s for s in parsed.specs if s.name == "h_size_by_income")
        assert xtab_spec.dimensions == ["h_size", "h_income"]
        # Non-crosstab specs have None
        size_spec = next(s for s in parsed.specs if s.name == "h_size")
        assert size_spec.dimensions is None

    def test_from_yaml_stores_merges_on_spec(self):
        """Cross-tab merges are stored on ControlSpec, not in crosstab_merges list.

        They are applied at registration time, not post-pivot.
        """
        config = [
            {"name": "h_size"},
            {"name": "h_income"},
            {
                "name": "h_size_by_income",
                "dimensions": ["h_size", "h_income"],
                "merges": {
                    "h_income": {
                        "income_under_100": [
                            "income_under25",
                            "income_25to50",
                            "income_50to75",
                            "income_75to100",
                        ]
                    }
                },
            },
        ]
        parsed = ControlRegistryConfig.from_yaml(config)
        # Cross-tab merges are NOT added to crosstab_merges (pre-merge at registration)
        assert len(parsed.crosstab_merges) == 0
        # Instead, merges are stored on the ControlSpec itself
        xtab_spec = next(s for s in parsed.specs if s.name == "h_size_by_income")
        assert xtab_spec.merges is not None
        assert "h_income" in xtab_spec.merges


# ---------------------------------------------------------------------------
# Composite expression
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("size_income_xtab")
class TestCrosstabExpression:
    """Tests for composite key expression building."""

    def test_expression_produces_valid_indices(self):
        """Composite expr maps (h_size, h_income) to sequential indices."""
        ctrl = CONTROLS["h_size_x_income"]
        n_valid = len(ctrl.valid_members)

        # Build a small DataFrame with pre-recoded dimension columns
        df = pl.DataFrame(
            {
                "h_size": [1, 2, 3, 1],  # SIZE_1, SIZE_2, SIZE_3, SIZE_1
                "h_income": [1, 1, 5, 6],  # UNDER25, UNDER25, 100TO200, 200+
            }
        ).cast({"h_size": pl.Int16, "h_income": pl.Int16})

        result = df.with_columns(ctrl.survey_expr().alias("xtab"))
        assert result["xtab"].null_count() == 0
        # All indices should be in range [0, n_valid)
        assert result["xtab"].min() >= 0
        assert result["xtab"].max() < n_valid

    def test_expression_null_for_sentinel(self):
        """Sentinel (MISSING/PNTA) values in dimensions produce null composite."""
        ctrl = CONTROLS["h_size_x_income"]
        df = pl.DataFrame(
            {
                "h_size": [1, 1],
                "h_income": [995, 999],  # MISSING, PNTA
            }
        ).cast({"h_size": pl.Int16, "h_income": pl.Int16})

        result = df.with_columns(ctrl.survey_expr().alias("xtab"))
        assert result["xtab"].null_count() == 2

    def test_distinct_combos_get_distinct_indices(self):
        """Every unique (size, income) pair maps to a unique index."""
        ctrl = CONTROLS["h_size_x_income"]
        combos = [
            (v1, v2)
            for v1, _ in ctrl.dim_controls[0].valid_members
            for v2, _ in ctrl.dim_controls[1].valid_members
        ]
        df = pl.DataFrame(
            {"h_size": [c[0] for c in combos], "h_income": [c[1] for c in combos]}
        ).cast({"h_size": pl.Int16, "h_income": pl.Int16})

        result = df.with_columns(ctrl.survey_expr().alias("xtab"))
        assert result["xtab"].n_unique() == len(combos)

    def test_merged_expression_maps_multiple_values(self):
        """Merged groups correctly map multiple original values to one index."""
        xtab = register_crosstab(
            "size_income_expr_test",
            ["h_size", "h_income"],
            merges={
                "h_income": {
                    "income_under_100": [
                        "income_under25",
                        "income_25to50",
                        "income_50to75",
                        "income_75to100",
                    ],
                },
                "h_size": {
                    "size_3_plus": [
                        "size_3",
                        "size_4",
                        "size_5",
                        "size_6",
                        "size_7",
                        "size_8",
                        "size_9",
                        "size_10_plus",
                    ],
                },
            },
        )
        # All income values 1-4 should map to the same cell when paired
        # with the same size value
        df = pl.DataFrame(
            {
                "h_size": [1, 1, 1, 1],
                "h_income": [1, 2, 3, 4],  # All under 100
            }
        ).cast({"h_size": pl.Int16, "h_income": pl.Int16})

        result = df.with_columns(xtab.survey_expr().alias("xtab"))
        assert result["xtab"].null_count() == 0
        assert result["xtab"].n_unique() == 1  # All map to same cell

        # Total should be 9 valid indices
        assert len(xtab.valid_members) == 9


# ---------------------------------------------------------------------------
# Incidence pivot
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("size_income_xtab")
class TestCrosstabIncidence:
    """Tests for cross-tab incidence table construction."""

    def test_incidence_has_crosstab_columns(self, households, persons):
        """Incidence table includes {xtab}__{composite} columns."""
        targets = ["h_size", "h_income", "h_size_x_income"]
        hh_recoded = recode_survey_households(households, persons, targets)
        per = persons  # No person targets in this test

        incidence = build_incidence_table(hh_recoded, per, targets).incidence
        xtab_cols = [c for c in incidence.columns if c.startswith("h_size_x_income__")]
        assert len(xtab_cols) > 0

    def test_incidence_crosstab_is_binary(self, households, persons):
        """Each household belongs to exactly one cross-tab cell: 0/1 indicators."""
        targets = ["h_size", "h_income", "h_size_x_income"]
        hh_recoded = recode_survey_households(households, persons, targets)
        incidence = build_incidence_table(hh_recoded, persons, targets).incidence
        xtab_cols = [c for c in incidence.columns if c.startswith("h_size_x_income__")]

        # Row sums should be 0 (null recode) or 1 (exactly one cell)
        row_sums = incidence.select(pl.sum_horizontal(*xtab_cols).alias("row_sum"))["row_sum"]
        assert (row_sums <= 1).all()

    def test_premerge_incidence_has_reduced_columns(self, households, persons):
        """Pre-merged cross-tab produces fewer incidence columns than full cartesian."""
        register_crosstab(
            "h_size_income_pre",
            ["h_size", "h_income"],
            merges={
                "h_income": {
                    "income_under_100": [
                        "income_under25",
                        "income_25to50",
                        "income_50to75",
                        "income_75to100",
                    ],
                },
                "h_size": {
                    "size_3_plus": [
                        "size_3",
                        "size_4",
                        "size_5",
                        "size_6",
                        "size_7",
                        "size_8",
                        "size_9",
                        "size_10_plus",
                    ],
                },
            },
        )
        targets = ["h_size", "h_income", "h_size_income_pre"]
        hh_recoded = recode_survey_households(households, persons, targets)
        incidence = build_incidence_table(hh_recoded, persons, targets).incidence
        xtab_cols = [c for c in incidence.columns if c.startswith("h_size_income_pre__")]
        # 3 size bins x 3 income bins = 9 incidence columns
        assert len(xtab_cols) == 9


# ---------------------------------------------------------------------------
# Cross-tab merges
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("size_income_xtab")
class TestCrosstabMerges:
    """Tests for N-D merge operations on cross-tab incidence columns."""

    def test_merge_collapses_dimension(self, households, persons):
        """Merging income categories reduces the number of xtab columns."""
        targets = ["h_size", "h_income", "h_size_x_income"]
        hh_recoded = recode_survey_households(households, persons, targets)
        incidence = build_incidence_table(hh_recoded, persons, targets).incidence

        before_cols = [c for c in incidence.columns if c.startswith("h_size_x_income__")]

        merge_spec = MergeSpec(
            control="h_size_x_income",
            groups={
                "h_income": {
                    "income_under_100": [
                        "income_under25",
                        "income_25to50",
                        "income_50to75",
                        "income_75to100",
                    ],
                },
            },
        )
        merged = apply_crosstab_merges(incidence, [merge_spec])
        after_cols = [c for c in merged.columns if c.startswith("h_size_x_income__")]

        # Should have fewer columns after merging 4 income bins → 1
        assert len(after_cols) < len(before_cols)

    def test_merge_preserves_row_sums(self, households, persons):
        """Merging doesn't change the total incidence per household."""
        targets = ["h_size", "h_income", "h_size_x_income"]
        hh_recoded = recode_survey_households(households, persons, targets)
        incidence = build_incidence_table(hh_recoded, persons, targets).incidence

        before_cols = [c for c in incidence.columns if c.startswith("h_size_x_income__")]
        before_sums = incidence.select(pl.sum_horizontal(*before_cols).alias("s"))["s"]

        merge_spec = MergeSpec(
            control="h_size_x_income",
            groups={
                "h_income": {
                    "income_under_100": [
                        "income_under25",
                        "income_25to50",
                        "income_50to75",
                        "income_75to100",
                    ],
                },
            },
        )
        merged = apply_crosstab_merges(incidence, [merge_spec])
        after_cols = [c for c in merged.columns if c.startswith("h_size_x_income__")]
        after_sums = merged.select(pl.sum_horizontal(*after_cols).alias("s"))["s"]

        assert before_sums.to_list() == after_sums.to_list()

    def test_multi_dim_merge(self, households, persons):
        """Merging both dimensions simultaneously produces expected cell count."""
        targets = ["h_size", "h_income", "h_size_x_income"]
        hh_recoded = recode_survey_households(households, persons, targets)
        incidence = build_incidence_table(hh_recoded, persons, targets).incidence

        # Merge: 4 income -> 1 merged + 2 kept = 3 income bins
        # Merge: 8 size -> 1 merged + 2 kept = 3 size bins
        # Result: 3 x 3 = 9 cells
        merge_spec = MergeSpec(
            control="h_size_x_income",
            groups={
                "h_income": {
                    "income_under_100": [
                        "income_under25",
                        "income_25to50",
                        "income_50to75",
                        "income_75to100",
                    ],
                },
                "h_size": {
                    "size_3_plus": [
                        "size_3",
                        "size_4",
                        "size_5",
                        "size_6",
                        "size_7",
                        "size_8",
                        "size_9",
                        "size_10_plus",
                    ],
                },
            },
        )
        merged = apply_crosstab_merges(incidence, [merge_spec])
        after_cols = [c for c in merged.columns if c.startswith("h_size_x_income__")]

        # 3 size bins (size_1, size_2, size_3_plus) x 3 income bins
        # (income_under_100, income_100to200, income_200_or_more) = 9
        assert len(after_cols) == 9

    def test_1d_merges_independent_of_crosstab(self, households, persons):
        """Global 1-D merges on h_size don't affect cross-tab columns."""
        targets = ["h_size", "h_income", "h_size_x_income"]
        hh_recoded = recode_survey_households(households, persons, targets)
        incidence = build_incidence_table(hh_recoded, persons, targets).incidence

        xtab_before = sorted(c for c in incidence.columns if c.startswith("h_size_x_income__"))

        # Apply a 1-D merge on h_size (should not touch cross-tab columns)
        merge_1d = MergeSpec(
            control="h_size",
            groups={
                "size_4_plus": [
                    "size_4",
                    "size_5",
                    "size_6",
                    "size_7",
                    "size_8",
                    "size_9",
                    "size_10_plus",
                ]
            },
        )
        merged = apply_1d_merges(incidence, [merge_1d])
        xtab_after = sorted(c for c in merged.columns if c.startswith("h_size_x_income__"))

        assert xtab_before == xtab_after


# ---------------------------------------------------------------------------
# PUMS-style aggregation
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("size_income_xtab")
class TestCrosstabAggregation:
    """Tests for cross-tab control total aggregation."""

    def test_aggregate_includes_crosstab_totals(self, households, persons):
        """Aggregated control totals include cross-tab categories."""
        targets = ["h_size", "h_income", "h_size_x_income"]
        hh_recoded = recode_survey_households(households, persons, targets)
        incidence = build_incidence_table(hh_recoded, persons, targets).incidence

        # Add a weight column and geo column (simulate PUMS incidence)
        pums_inc = incidence.with_columns(
            pl.lit(1.0).alias("WGTP"),
            pl.col("hh_id").cast(pl.Utf8).alias("SERIALNO"),
        ).join(
            households.select("hh_id", "ctrl_geoid"),
            on="hh_id",
            how="left",
        )

        totals = aggregate_control_totals(
            pums_inc, targets, weight_col="WGTP", geo_col="ctrl_geoid"
        )
        xtab_rows = totals.totals.filter(pl.col("control_name") == "h_size_x_income")
        assert len(xtab_rows) > 0


# ---------------------------------------------------------------------------
# Sparsity warnings
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("size_income_xtab")
class TestCrosstabSparsity:
    """Tests for cross-tab cell sparsity warnings."""

    def test_no_warning_when_all_cells_above_threshold(self, caplog):
        """No warning when every cross-tab cell has enough records."""
        ctrl = CONTROLS["h_size_x_income"]
        prefix = f"{ctrl.name}__"
        cols = [
            f"{prefix}{m.name.lower()}"
            for m in ctrl.categories
            if m.name not in ("MISSING", "PNTA")
        ]
        data: dict[str, list] = {"ctrl_geoid": ["Z1"] * 50}
        for col in cols:
            data[col] = [1] * 50  # 50 records per cell
        seed = pl.DataFrame(data)

        with caplog.at_level("WARNING"):
            warn_crosstab_sparsity(seed, [ctrl], threshold=30)
        assert "sparse" not in caplog.text.lower()

    def test_warning_when_cells_below_threshold(self, caplog):
        """Warning logged when cross-tab cells have fewer than threshold records."""
        ctrl = CONTROLS["h_size_x_income"]
        prefix = f"{ctrl.name}__"
        members = [m for m in ctrl.categories if m.name not in ("MISSING", "PNTA")]
        cols = [f"{prefix}{m.name.lower()}" for m in members]

        data: dict[str, list] = {"ctrl_geoid": ["Z1"] * 5}
        for i, col in enumerate(cols):
            # First cell has only 5 records (sparse), rest have 0
            data[col] = [1 if i == 0 else 0] * 5
        seed = pl.DataFrame(data)

        with caplog.at_level("WARNING"):
            warn_crosstab_sparsity(seed, [ctrl], threshold=30)
        assert "sparse" in caplog.text.lower()
        assert ctrl.name in caplog.text

    def test_no_warning_for_1d_controls(self, caplog):
        """1-D controls are not checked by warn_crosstab_sparsity."""
        ctrl = CONTROLS["h_size"]
        data: dict[str, list] = {
            "ctrl_geoid": ["Z1"],
            "h_size__size_1": [1],
        }
        seed = pl.DataFrame(data)

        with caplog.at_level("WARNING"):
            warn_crosstab_sparsity(seed, [ctrl], threshold=30)
        assert "sparse" not in caplog.text.lower()

    def test_multiple_zones_reports_zone_count(self, caplog):
        """Warning message includes how many zones are below threshold."""
        ctrl = CONTROLS["h_size_x_income"]
        prefix = f"{ctrl.name}__"
        members = [m for m in ctrl.categories if m.name not in ("MISSING", "PNTA")]
        cols = [f"{prefix}{m.name.lower()}" for m in members]

        # Two zones, both with sparse first cell
        data: dict[str, list] = {"ctrl_geoid": ["Z1", "Z2"]}
        for i, col in enumerate(cols):
            data[col] = [1 if i == 0 else 0, 1 if i == 0 else 0]
        seed = pl.DataFrame(data)

        with caplog.at_level("WARNING"):
            warn_crosstab_sparsity(seed, [ctrl], threshold=30)
        assert "2 zone(s)" in caplog.text
