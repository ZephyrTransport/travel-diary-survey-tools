"""The controls a weighting run fits to: cross-tabs, totals, merges and checksums."""

import logging
import re

import polars as pl
import pytest

from data_canon.codebook.households import IncomeBroad
from data_canon.codebook.persons import Gender
from processing.weighting.controls.base import ControlLevel, CrosstabControlTarget
from processing.weighting.controls.registry import CONTROLS, register_crosstab
from processing.weighting.core.pipeline import WeightingPipeline
from processing.weighting.core.specs import (
    ControlRegistryConfig,
    ControlTotals,
    MergeSpec,
)
from processing.weighting.data_prep.control_data import (
    recode_pums_households,
    recode_pums_persons,
)
from processing.weighting.data_prep.incidence import (
    aggregate_control_totals,
    build_incidence_table,
)
from processing.weighting.data_prep.merges import (
    apply_1d_merges,
    merge_control_totals,
)
from processing.weighting.data_prep.seed_data import (
    recode_survey_households,
    recode_survey_persons,
    restore_original_answers,
)
from processing.weighting.diagnostics.data import apply_fit_merges
from processing.weighting.validation.checksums import check_incidence_sums, check_recode_nulls
from processing.weighting.validation.control_validation import warn_crosstab_sparsity

# A person table for tests with household targets only.
_NO_PERSONS = pl.DataFrame(schema={"SERIALNO": pl.Utf8, "SPORDER": pl.Int64})

# Income to three bins (under 100k, 100-200k, 200k+) and size to three (1, 2,
# 3 or more), so the 10 x 6 cartesian product becomes 3 x 3 = 9 cells.
PREMERGE_GROUPS = {
    "h_income": {
        "income_under_100": ["income_under25", "income_25to50", "income_50to75", "income_75to100"]
    },
    "h_size": {
        "size_3_plus": [f"size_{n}" for n in range(3, 10)] + ["size_10_plus"],
    },
}


@pytest.fixture(autouse=True)
def _isolate_registry(_clean_registry):
    """Clean the registry after every test.

    Autouse so the cleanup's snapshot is taken before fixtures such as
    ``size_income_xtab`` register anything.
    """


@pytest.fixture
def size_income_xtab():
    """Register and return a h_size x h_income cross-tab control."""
    return register_crosstab("h_size_x_income", ["h_size", "h_income"])


def _incidence(households: pl.DataFrame, persons: pl.DataFrame, targets: list[str]):
    """The survey incidence table for *targets*."""
    hh_recoded = recode_survey_households(households, persons, targets)
    return build_incidence_table(hh_recoded, persons, targets).incidence


@pytest.fixture
def households() -> pl.DataFrame:
    """Four households in two zones."""
    return pl.DataFrame(
        {
            "hh_id": [1, 2, 3, 4],
            "income_bin": [1, 5, 3, 2],  # UNDER25, 100TO200, 50TO75, 25TO50
            "zone_group_id": ["Z1", "Z1", "Z2", "Z2"],
        }
    )


@pytest.fixture
def persons() -> pl.DataFrame:
    """Persons, which set household size: 1, 2, 3 and 1."""
    return pl.DataFrame(
        {
            "hh_id": [1, 2, 2, 3, 3, 3, 4],
            "person_id": [101, 201, 202, 301, 302, 303, 401],
            "age": [5] * 7,  # adults
            "gender": [2, 1, 2, 1, 2, 1, 2],
            "employment": [5] * 7,  # not employed
            "student": [2] * 7,  # not a student
            "school_type": [None] * 7,
            "work_mode": [None] * 7,
        }
    )


@pytest.mark.usefixtures("size_income_xtab")
class TestCrosstabRegistration:
    """Tests for dynamic cross-tab control registration."""

    def test_register_creates_control(self):
        """Cross-tab is registered in the global CONTROLS dict, with its dimensions."""
        ctrl = CONTROLS["h_size_x_income"]
        assert isinstance(ctrl, CrosstabControlTarget)
        assert [d.name for d in ctrl.dim_controls] == ["h_size", "h_income"]

    def test_register_duplicate_raises(self):
        """Re-registering the same name raises ValueError."""
        with pytest.raises(ValueError, match="already exists"):
            register_crosstab("h_size_x_income", ["h_size", "h_income"])

    def test_from_yaml_parses_dimensions_and_keeps_merges_on_the_spec(self):
        """A cross-tab spec carries its dimensions and its own merges, not 1-D merges."""
        config = [
            {"name": "h_size"},
            {"name": "h_income"},
            {
                "name": "h_size_by_income",
                "dimensions": ["h_size", "h_income"],
                "merges": {"h_income": PREMERGE_GROUPS["h_income"]},
            },
        ]
        parsed = ControlRegistryConfig.from_yaml(config)
        xtab_spec = next(s for s in parsed.specs if s.name == "h_size_by_income")
        assert xtab_spec.dimensions == ["h_size", "h_income"]
        assert next(s for s in parsed.specs if s.name == "h_size").dimensions is None
        assert parsed.merges_1d == []
        assert xtab_spec.merges is not None
        assert "h_income" in xtab_spec.merges


@pytest.mark.usefixtures("size_income_xtab")
class TestCrosstabExpression:
    """Tests for composite key expression building."""

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
        assert result["xtab"].null_count() == 0
        assert result["xtab"].n_unique() == len(combos)
        # the indices are the dense range over the valid members
        assert sorted(result["xtab"].to_list()) == list(range(len(ctrl.valid_members)))

    def test_merged_expression_maps_multiple_values(self):
        """Pre-merged groups map several values to one of 3 x 3 = 9 named cells."""
        xtab = register_crosstab(
            "size_income_expr_test",
            ["h_size", "h_income"],
            merges=PREMERGE_GROUPS,
        )
        df = pl.DataFrame({"h_size": [1, 1, 1, 1], "h_income": [1, 2, 3, 4]}).cast(
            {"h_size": pl.Int16, "h_income": pl.Int16}
        )

        result = df.with_columns(xtab.survey_expr().alias("xtab"))
        assert result["xtab"].null_count() == 0
        assert result["xtab"].n_unique() == 1
        assert len(xtab.valid_members) == 9
        assert "SIZE_3_PLUS_INCOME_UNDER_100" in [name for _, name in xtab.valid_members]


@pytest.mark.usefixtures("size_income_xtab")
class TestCrosstabIncidence:
    """Tests for cross-tab incidence table construction."""

    def test_incidence_crosstab_is_binary(self, households, persons):
        """Each household belongs to exactly one cross-tab cell: 0/1 indicators."""
        ctrl = CONTROLS["h_size_x_income"]
        incidence = _incidence(households, persons, ["h_size", "h_income", "h_size_x_income"])
        xtab_cols = [c for c in incidence.columns if c.startswith("h_size_x_income__")]

        assert len(xtab_cols) == len(ctrl.valid_members)
        # 0 for a null recode, else exactly one cell
        row_sums = incidence.select(pl.sum_horizontal(*xtab_cols).alias("row_sum"))["row_sum"]
        assert (row_sums <= 1).all()

    def test_premerge_incidence_has_reduced_columns(self, households, persons):
        """Pre-merged cross-tab produces fewer incidence columns than full cartesian."""
        register_crosstab(
            "h_size_income_pre",
            ["h_size", "h_income"],
            merges=PREMERGE_GROUPS,
        )
        incidence = _incidence(households, persons, ["h_size", "h_income", "h_size_income_pre"])
        xtab_cols = [c for c in incidence.columns if c.startswith("h_size_income_pre__")]
        assert len(xtab_cols) == 9


@pytest.mark.usefixtures("size_income_xtab")
class TestCrosstabMerges:
    """1-D merges alongside cross-tab incidence columns."""

    def test_1d_merges_independent_of_crosstab(self, households, persons):
        """Global 1-D merges on h_size don't affect cross-tab columns."""
        incidence = _incidence(households, persons, ["h_size", "h_income", "h_size_x_income"])
        xtab_before = sorted(c for c in incidence.columns if c.startswith("h_size_x_income__"))

        merge_1d = MergeSpec(
            control="h_size",
            groups={"size_4_plus": [f"size_{n}" for n in range(4, 10)] + ["size_10_plus"]},
        )
        merged = apply_1d_merges(incidence, [merge_1d])
        xtab_after = sorted(c for c in merged.columns if c.startswith("h_size_x_income__"))

        assert xtab_before == xtab_after


@pytest.mark.usefixtures("size_income_xtab")
class TestCrosstabAggregation:
    """Tests for cross-tab control total aggregation."""

    def test_aggregate_includes_crosstab_totals(self, households, persons):
        """Aggregated control totals include cross-tab categories."""
        targets = ["h_size", "h_income", "h_size_x_income"]
        # the survey incidence, given a weight and a zone, stands in for PUMS
        pums_inc = (
            _incidence(households, persons, targets)
            .with_columns(
                pl.lit(1.0).alias("WGTP"),
                pl.col("hh_id").cast(pl.Utf8).alias("SERIALNO"),
            )
            .join(
                households.select("hh_id", "zone_group_id"),
                on="hh_id",
                how="left",
            )
        )

        totals = aggregate_control_totals(pums_inc, _NO_PERSONS, targets, geo_col="zone_group_id")
        xtab_rows = totals.totals.filter(pl.col("control_name") == "h_size_x_income")
        assert len(xtab_rows) > 0


@pytest.mark.usefixtures("size_income_xtab")
class TestCrosstabSparsity:
    """Tests for cross-tab cell sparsity warnings."""

    @pytest.mark.parametrize(
        ("n_rows", "other_cells", "warned"),
        [
            pytest.param(50, 1, False, id="every_cell_above_threshold"),
            pytest.param(5, 0, True, id="cells_below_threshold"),
        ],
    )
    def test_warns_only_below_threshold(self, caplog, n_rows, other_cells, warned):
        """A cross-tab cell with fewer than *threshold* records is named."""
        ctrl = CONTROLS["h_size_x_income"]
        members = [m for m in ctrl.categories if m.name not in ("MISSING", "PNTA")]
        seed = pl.DataFrame(
            {
                "zone_group_id": ["Z1"] * n_rows,
                **{
                    f"{ctrl.name}__{m.name.lower()}": [1 if i == 0 else other_cells] * n_rows
                    for i, m in enumerate(members)
                },
            }
        )

        with caplog.at_level("WARNING"):
            warn_crosstab_sparsity(seed, [ctrl], threshold=30)
        assert ("sparse" in caplog.text.lower()) is warned
        assert (ctrl.name in caplog.text) is warned

    def test_no_warning_for_1d_controls(self, caplog):
        """1-D controls are not checked by warn_crosstab_sparsity."""
        seed = pl.DataFrame({"zone_group_id": ["Z1"], "h_size__size_1": [1]})

        with caplog.at_level("WARNING"):
            warn_crosstab_sparsity(seed, [CONTROLS["h_size"]], threshold=30)
        assert "sparse" not in caplog.text.lower()


class TestAggregateControlTotals:
    """Household targets sum household weights; person targets sum person weights."""

    @pytest.fixture
    def totals(self, pums_households, pums_persons) -> ControlTotals:
        """Totals for one household and one person target, each PUMA its own zone."""
        targets = ["h_total", "p_total", "h_size", "p_gender"]
        bundle = build_incidence_table(
            recode_pums_households(pums_households, pums_persons, targets),
            recode_pums_persons(pums_persons, targets),
            targets,
            hh_id_col="SERIALNO",
            extra_cols=["WGTP", "PUMA"],
        )
        persons = bundle.person_pivot.join(
            pums_persons.select("SERIALNO", "SPORDER", "PUMA", "PWGTP"),
            on=["SERIALNO", "SPORDER"],
        )
        return aggregate_control_totals(bundle.incidence, persons, targets, geo_col="PUMA")

    @staticmethod
    def _by_zone(totals: ControlTotals, control: str) -> dict[str, float]:
        rows = totals.totals.filter(pl.col("control_name") == control)
        return dict(rows.group_by("zone_group_id").agg(pl.col("target_total").sum()).iter_rows())

    @pytest.mark.parametrize("control", ["h_total", "h_size"])
    def test_household_targets_sum_household_weights(self, totals, control):
        """PUMA 00100: WGTP 100 + 200; PUMA 00200: WGTP 150."""
        assert self._by_zone(totals, control) == pytest.approx({"00100": 300.0, "00200": 150.0})

    @pytest.mark.parametrize("control", ["p_total", "p_gender"])
    def test_person_targets_sum_person_weights(self, totals, control):
        """PUMA 00100: PWGTP sums to 1040, where WGTP x members would give 1000."""
        assert self._by_zone(totals, control) == pytest.approx({"00100": 1040.0, "00200": 160.0})

    def test_pums_counts(self, totals):
        """Counted once each, whatever the weights."""
        assert (totals.pums_hh_count, totals.pums_person_count) == (3, 7)


class TestParseControlsZoneMerges:
    """ControlRegistryConfig.from_yaml should emit MergeSpecs for zone_merges entries."""

    def test_zone_merges_appended(self):
        """zone_merges produce zone-specific MergeSpecs after the global one."""
        controls = [
            {
                "name": "h_size",
                "merge": {"size_5_plus": ["size_5", "size_6"]},
                "zone_merges": {
                    "Z1": {"size_4_plus": ["size_4", "size_5_plus"]},
                    "Z2": {"size_3_plus": ["size_3", "size_5_plus"]},
                },
            },
        ]
        cfg = ControlRegistryConfig.from_yaml(controls)
        merges = cfg.merges_1d
        assert len(merges) == 3
        assert merges[0].zones is None
        assert "size_5_plus" in merges[0].groups
        # then one per zone key, in config order
        assert [m.zones for m in merges[1:]] == [["Z1"], ["Z2"]]
        assert "size_4_plus" in merges[1].groups
        assert "size_3_plus" in merges[2].groups

    def test_labels_and_members_are_lower_case(self):
        """A label written AAPI is aapi, as every downstream table names its groups."""
        controls = [
            {
                "name": "p_race_ethnicity",
                "merge": {"AAPI": ["Asian", "pacific"]},
                "zone_merges": {"Z1": {"AAPI_Other": ["AAPI", "other"]}},
            },
        ]
        merges = ControlRegistryConfig.from_yaml(controls).merges_1d
        assert merges[0].groups == {"aapi": ["asian", "pacific"]}
        assert merges[1].groups == {"aapi_other": ["aapi", "other"]}

    def test_zone_merges_without_global_merge(self):
        """A control with only zone_merges (no global merge) still works."""
        controls = [
            {
                "name": "h_size",
                "zone_merges": {
                    "Z1": {"size_5_plus": ["size_5", "size_6"]},
                },
            },
        ]
        cfg = ControlRegistryConfig.from_yaml(controls)
        merges = cfg.merges_1d
        assert len(merges) == 1
        assert merges[0].zones == ["Z1"]
        assert merges[0].control == "h_size"


class TestZoneMergeTargets:
    """A zone merge is one target in its own areas and none elsewhere."""

    def test_one_target_row_per_group(self):
        """Merged area A: 0 / 2+; area B keeps 0 / 2 / 3. No duplicates, no extra 2+ in B."""
        pipeline = object.__new__(WeightingPipeline)
        pipeline.controls = ControlRegistryConfig.from_yaml(
            [
                {"name": "h_total"},
                {"name": "p_total"},
                {
                    "name": "h_workers",
                    "zone_merges": {"A": {"w2_plus": ["workers_2", "workers_3"]}},
                },
            ]
        )
        pipeline.pums_incidence = pl.DataFrame(
            {
                "SERIALNO": ["1", "2", "3", "4"],
                "zone_group_id": ["A", "A", "B", "B"],
                "WGTP": [10.0, 20.0, 30.0, 40.0],
                "h_total": [1, 1, 1, 1],
                "h_workers__workers_0": [1, 0, 1, 0],
                "h_workers__workers_2": [0, 1, 0, 0],
                "h_workers__workers_3": [0, 0, 0, 1],
            }
        )
        pipeline.pums_person_incidence = pl.DataFrame(
            {
                "SERIALNO": ["1"],
                "SPORDER": [1],
                "zone_group_id": ["A"],
                "PWGTP": [10.0],
                "p_total": [1],
            }
        )

        pipeline.apply_merges()
        pipeline.aggregate_totals()

        workers = pipeline.control_totals.totals.filter(pl.col("control_name") == "h_workers")
        got = {
            (r["zone_group_id"], r["category"]): r["target_total"]
            for r in workers.iter_rows(named=True)
        }
        assert workers.height == len(got)
        assert {k: v for k, v in got.items() if v} == {
            ("A", "workers_0"): 10.0,
            ("A", "w2_plus"): 20.0,
            ("B", "workers_0"): 30.0,
            ("B", "workers_3"): 40.0,
        }
        assert ("B", "w2_plus") not in got
        assert ("A", "workers_2") not in got

    def test_a_group_of_one_renames_its_category(self):
        """A one-member group, such as a county of one PUMA, is a target under its own name."""
        incidence = pl.DataFrame({"zone_group_id": ["A"], "h_puma__puma_05500": [1.0]})
        spec = MergeSpec(control="h_puma", groups={"napa": ["puma_05500"]}, zones=["A"])
        assert "h_puma__napa" in apply_1d_merges(incidence, [spec]).columns

        totals = ControlTotals(
            totals=pl.DataFrame(
                {
                    "zone_group_id": ["A"],
                    "control_name": ["h_puma"],
                    "category": ["puma_05500"],
                    "target_total": [51167.0],
                }
            ),
            pums_hh_count=1,
            pums_person_count=0,
            zone_group_ids=["A"],
        )
        merged = merge_control_totals(totals, [spec]).totals
        assert merged["category"].to_list() == ["napa"]
        assert merged["target_total"].to_list() == [51167.0]


def _fit(rows: list[tuple[str, str, float, float]]) -> pl.DataFrame:
    """A fit table for h_size from (zone, category, target, weighted) rows."""
    return pl.DataFrame(
        [
            {
                "zone_group_id": z,
                "control_name": "h_size",
                "category": c,
                "target_total": t,
                "weighted_total": w,
                "diff": w - t,
                "diff_pct": (w - t) / t * 100 if t else 0.0,
            }
            for z, c, t, w in rows
        ]
    )


def test_zone_merge_labels_present_in_every_zone():
    """Zone Z1 shows its merged label, Z2 its originals, and every zone has every label."""
    # Z1 arrives merged and Z2 not, as merge_control_totals leaves them upstream
    fit = _fit(
        [
            ("Z1", "size_3", 10.0, 11.0),
            ("Z1", "size_4_plus", 20.0, 21.0),
            ("Z2", "size_3", 10.0, 11.0),
            ("Z2", "size_4", 20.0, 21.0),
            ("Z2", "size_5_plus", 30.0, 31.0),
        ]
    )
    global_merge = MergeSpec(
        control="h_size", groups={"size_5_plus": ["size_5", "size_6"]}, zones=None
    )
    zone_merge = MergeSpec(
        control="h_size", groups={"size_4_plus": ["size_4", "size_5_plus"]}, zones=["Z1"]
    )
    result = apply_fit_merges(fit, [global_merge, zone_merge], ["h_size"])

    def labels(zone: str) -> list[str]:
        rows = result.filter(
            (pl.col("zone_group_id") == zone) & pl.col("target_total").is_not_null()
        )
        return rows["label"].to_list()

    assert {"Size 4 Plus", "Size 3"} <= set(labels("Z1"))
    assert {"Size 4", "Size 5 Plus"} <= set(labels("Z2"))
    z1_merged = result.filter(
        (pl.col("zone_group_id") == "Z1") & (pl.col("label") == "Size 4 Plus")
    )
    assert z1_merged.height == 1
    assert z1_merged["target_total"].item() is not None

    all_labels = result["label"].unique().sort().to_list()
    for z in ["Z1", "Z2"]:
        zone_labels = result.filter(pl.col("zone_group_id") == z)["label"].unique().sort()
        assert zone_labels.to_list() == all_labels, f"Zone {z} missing some labels"


class TestApplyFitMergesEmptyCategories:
    """A category with no target and no weight anywhere gets no row."""

    def test_category_merged_away_in_every_zone_is_dropped(self):
        """Each zone merged size_3 into its own group, so size_3 is 0 everywhere."""
        fit = _fit(
            [
                ("Z1", "size_3", 0.0, 0.0),
                ("Z1", "size_3_plus", 50.0, 51.0),
                ("Z2", "size_3", 0.0, 0.0),
                ("Z2", "size_3_plus", 40.0, 39.0),
            ]
        )
        result = apply_fit_merges(fit, None, ["h_size"])
        assert set(result["category"]) == {"size_3_plus"}

    def test_category_with_a_target_in_one_zone_is_kept_and_padded(self):
        """size_3 is a target in Z2 only, so Z1 keeps a placeholder row for it."""
        fit = _fit(
            [
                ("Z1", "size_3", 0.0, 0.0),
                ("Z1", "size_3_plus", 50.0, 51.0),
                ("Z2", "size_3", 30.0, 31.0),
                ("Z2", "size_3_plus", 40.0, 39.0),
            ]
        )
        result = apply_fit_merges(fit, None, ["h_size"])
        assert result.filter(pl.col("category") == "size_3").height == 2

    def test_weight_with_no_target_is_kept(self):
        """Weight where the target is 0 is a miss, so the row stays."""
        fit = _fit([("Z1", "size_3", 0.0, 5.0), ("Z1", "size_3_plus", 50.0, 45.0)])
        result = apply_fit_merges(fit, None, ["h_size"])
        assert "size_3" in set(result["category"])


@pytest.fixture
def good_seed() -> pl.DataFrame:
    """Seed table where all controls are properly accounted for.

    3 households:
      HH1: 1 person (male, employed_full)
      HH2: 3 persons (1 male + 2 female, 1 employed_full + 1 employed_part + 1 not_employed)
      HH3: 2 persons (1 male + 1 female, 2 not_employed)
    """
    return pl.DataFrame(
        {
            "hh_id": [1, 2, 3],
            "p_total": [1, 3, 2],
            "h_income__low": [1, 0, 0],
            "h_income__mid": [0, 1, 0],
            "h_income__high": [0, 0, 1],
            "p_gender__male": [1, 1, 1],
            "p_gender__female": [0, 2, 1],
            "p_employment__employed_full": [1, 1, 0],
            "p_employment__employed_part": [0, 1, 0],
            "p_employment__not_employed": [0, 1, 2],
        },
    )


class TestCheckIncidenceSums:
    """Tests for incidence-sum validation on the seed table."""

    @pytest.mark.parametrize(
        ("seed_name", "targets"),
        [
            pytest.param("good", ["h_income", "p_gender", "p_employment"], id="sums_all_match"),
            pytest.param("good", ["bogus_control"], id="unknown_target_skipped"),
            pytest.param("no_incidence", ["p_gender"], id="no_incidence_columns_skipped"),
        ],
    )
    def test_passes_without_a_failure(self, good_seed, seed_name, targets, caplog):
        """Matching sums, unknown names and absent columns all pass quietly."""
        seed = good_seed if seed_name == "good" else pl.DataFrame({"hh_id": [1], "p_total": [1]})
        with caplog.at_level(logging.INFO):
            check_incidence_sums(seed, targets, source_label="test")
        assert "all controls pass" in caplog.text
        assert "failure" not in caplog.text

    def test_undercount_raises(self, good_seed):
        """An undercount (below p_total) raises."""
        seed = good_seed.with_columns(
            # HH2 employment sums to 2 instead of 3 (undercount)
            pl.when(pl.col("hh_id") == 2)
            .then(0)
            .otherwise(pl.col("p_employment__employed_part"))
            .alias("p_employment__employed_part")
        )
        with pytest.raises(ValueError, match="p_employment"):
            check_incidence_sums(seed, ["p_employment"], source_label="test")

    def test_fails_on_hh_control_mismatch(self):
        """HH-level control with wrong sum raises ValueError (no h_total needed)."""
        seed = pl.DataFrame(
            {
                "hh_id": [1, 2],
                "h_income__low": [1, 0],
                "h_income__mid": [0, 0],  # HH2 sums to 0 instead of 1
                "h_income__high": [0, 0],
            },
        )
        with pytest.raises(ValueError, match="h_income"):
            check_incidence_sums(seed, ["h_income"], source_label="test")

    def test_tolerance_allows_fractional_drift(self, good_seed):
        """Small floating-point deviation passes with tolerance."""
        seed = good_seed.with_columns(
            (pl.col("p_gender__male").cast(pl.Float64) + 0.005).alias("p_gender__male")
        )
        # Fails with zero tolerance
        with pytest.raises(ValueError, match="p_gender"):
            check_incidence_sums(seed, ["p_gender"], source_label="test")
        # Passes with tolerance
        check_incidence_sums(seed, ["p_gender"], source_label="test", tolerance=0.01)

    def test_error_shows_hh_id_and_counts(self, good_seed):
        """An overcount names the source, the one bad household, and both counts.

        HH3 is double-classified: its gender columns sum to 3 against a
        ``p_total`` of 2. HH1 and HH2 must not appear.
        """
        overcount_seed = good_seed.with_columns(pl.Series("p_gender__male", [1, 1, 2]))
        with pytest.raises(ValueError, match="p_gender") as exc_info:
            check_incidence_sums(overcount_seed, ["p_gender"], source_label="survey")
        msg = str(exc_info.value)

        assert "Incidence sum failures (survey):" in msg
        # one row: hh_id 3, control p_gender, actual 3.00, expected 2.00
        assert re.search(r"^\s+3\s+p_gender\s+3\.00\s+2\.00\s*$", msg, re.MULTILINE)
        assert "1 total failure(s)." in msg
        reported_hh_ids = re.findall(r"^\s+(\d+)\s+p_gender\s", msg, re.MULTILINE)
        assert reported_hh_ids == ["3"]

    def test_warns_when_p_total_missing(self, caplog):
        """Person-control check warns and skips when p_total is absent."""
        seed = pl.DataFrame(
            {
                "hh_id": [1],
                "p_gender__male": [1],
                "p_gender__female": [0],
            },
        )
        with caplog.at_level(logging.WARNING):
            check_incidence_sums(seed, ["p_gender"], source_label="test")
        assert "p_total column missing" in caplog.text


class TestRestoreOriginalAnswers:
    """Weighting can set aside the imputation step's values."""

    # Person 1 answered male, person 2 chose not to say and was imputed male.
    PERSONS = pl.DataFrame(
        {
            "person_id": [1, 2],
            "hh_id": [10, 10],
            "gender": [Gender.MALE.value, Gender.MALE.value],
            "gender_preimputed": [Gender.MALE.value, Gender.PNTA.value],
        }
    )
    HOUSEHOLDS = pl.DataFrame(
        {
            "hh_id": [10, 20],
            "income_bin": [IncomeBroad.INCOME_UNDER25.value, IncomeBroad.INCOME_UNDER25.value],
            "income_bin_preimputed": [IncomeBroad.INCOME_UNDER25.value, None],
        }
    )

    def test_each_table_gets_its_original_values(self):
        """Every stashed column is restored, in both tables."""
        households, persons = restore_original_answers(self.HOUSEHOLDS, self.PERSONS)
        assert households["income_bin"].to_list() == [IncomeBroad.INCOME_UNDER25.value, None]
        assert persons["gender"].to_list() == [Gender.MALE.value, Gender.PNTA.value]

    def test_prefer_not_to_answer_reaches_the_control_as_a_blank(self):
        """A restored sentinel recodes to null, so weighting fills it."""
        _, persons = restore_original_answers(self.HOUSEHOLDS, self.PERSONS)
        recoded = recode_survey_persons(persons, ["p_gender"])
        assert recoded["p_gender"].null_count() == 1
        assert recoded.filter(pl.col("person_id") == 2)["p_gender"].item() is None

    def test_one_stashed_table_is_enough(self):
        """A table with nothing imputed passes through unchanged."""
        households = self.HOUSEHOLDS.drop("income_bin_preimputed")
        restored, _ = restore_original_answers(households, self.PERSONS)
        assert restored.equals(households)

    def test_stops_when_nothing_was_stashed(self):
        """With no original values to read, the setting cannot do what it says."""
        with pytest.raises(ValueError, match="use_imputed_values is false"):
            restore_original_answers(
                self.HOUSEHOLDS.drop("income_bin_preimputed"),
                self.PERSONS.drop("gender_preimputed"),
            )


class TestCheckRecodeNulls:
    """Tests for null-detection on recoded DataFrames (pre-aggregation)."""

    @pytest.mark.parametrize(
        ("df", "targets"),
        [
            pytest.param(
                pl.DataFrame({"person_id": [1, 2], "p_gender": [1, 2]}),
                ["p_gender"],
                id="every_record_classified",
            ),
            pytest.param(
                pl.DataFrame({"person_id": [1], "p_gender": [1]}),
                ["bogus_control"],
                id="unknown_target_skipped",
            ),
            pytest.param(
                pl.DataFrame({"person_id": [1]}),
                ["p_gender"],
                id="absent_column_skipped",
            ),
        ],
    )
    def test_passes_when_all_classified(self, df, targets, caplog):
        """Full classification, unknown names and absent columns all pass quietly."""
        with caplog.at_level(logging.INFO):
            check_recode_nulls(
                df,
                targets,
                level=ControlLevel.PERSON,
                id_col="person_id",
                source_label="test",
            )
        assert "all records classified" in caplog.text
        assert "Null recode values" not in caplog.text

    @pytest.mark.parametrize(
        ("df", "targets", "level", "id_col", "null_id"),
        [
            pytest.param(
                pl.DataFrame({"person_id": [1, 2, 3], "p_gender": [1, None, 2]}),
                ["p_gender"],
                ControlLevel.PERSON,
                "person_id",
                "2",
                id="person_control",
            ),
            pytest.param(
                pl.DataFrame({"hh_id": [10, 20], "h_workers": [1, None]}),
                ["h_workers"],
                ControlLevel.HOUSEHOLD,
                "hh_id",
                "20",
                id="household_control",
            ),
        ],
    )
    def test_warns_on_null_control(self, df, targets, level, id_col, null_id, caplog):
        """A null recode warns and names the record; it never raises."""
        with caplog.at_level(logging.WARNING):
            check_recode_nulls(
                df,
                targets,
                level=level,
                id_col=id_col,
                source_label="survey",
            )
        assert "Null recode values" in caplog.text
        assert re.search(rf"^\s+{null_id}\s+{targets[0]}\s*$", caplog.text, re.MULTILINE)

    def test_filters_by_level(self, caplog):
        """Only controls matching the requested level are checked."""
        df = pl.DataFrame({"hh_id": [1], "h_workers": [None], "p_gender": [None]})
        with caplog.at_level(logging.INFO):
            check_recode_nulls(
                df,
                ["h_workers", "p_gender"],
                level=ControlLevel.HOUSEHOLD,
                id_col="hh_id",
                source_label="test",
            )
        assert "h_workers" in caplog.text
        assert "p_gender" not in caplog.text
