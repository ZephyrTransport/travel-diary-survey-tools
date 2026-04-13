"""Tests for weighting core control_data module (PUMS recode & aggregation).

These tests exercise the recode and aggregation functions using small
synthetic PUMS-like DataFrames (no Census API required).
"""

import polars as pl
import pytest

from data_canon.codebook.households import IncomeBroad
from data_canon.codebook.persons import AgeCategory, Ethnicity
from processing.weighting.controls.enums import (
    CommuteModeCategory,
    GenderCategory,
    HHChildrenCategory,
    HHSizeCategory,
    HHVehiclesCategory,
    HHWorkersCategory,
    StudentCategory,
)
from processing.weighting.data_prep.control_data import (
    apply_zone_groups,
    build_control_totals,
    recode_pums_households,
    recode_pums_persons,
)
from processing.weighting.data_prep.pums_data import (
    PUMSSource,
    load_pums_from_files,
)
from processing.weighting.specs import ControlSpec, ControlTotals


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def pums_households() -> pl.DataFrame:
    """Minimal PUMS household DataFrame."""
    return pl.DataFrame(
        {
            "SERIALNO": ["HH1", "HH2", "HH3"],
            "PUMA": ["00100", "00100", "00200"],
            "ST": ["06", "06", "06"],
            "WGTP": [100.0, 200.0, 150.0],
            "NP": [2, 4, 1],
            "HINCP": [55_000.0, 120_000.0, 15_000.0],
            "VEH": [1, 2, 0],
            "NOC": [0, 2, 0],
            "TYPEHUGQ": [1, 1, 1],
        }
    )


@pytest.fixture
def pums_persons() -> pl.DataFrame:
    """Minimal PUMS person DataFrame matching the household fixture."""
    return pl.DataFrame(
        {
            "SERIALNO": ["HH1", "HH1", "HH2", "HH2", "HH2", "HH2", "HH3"],
            "SPORDER": [1, 2, 1, 2, 3, 4, 1],
            "PUMA": ["00100", "00100", "00100", "00100", "00100", "00100", "00200"],
            "ST": ["06", "06", "06", "06", "06", "06", "06"],
            "PWGTP": [100.0, 100.0, 200.0, 200.0, 200.0, 200.0, 150.0],
            "AGEP": [35, 33, 40, 38, 10, 7, 65],
            "SEX": [1, 2, 1, 2, 1, 2, 2],
            "ESR": [1, 1, 1, 6, 0, 0, 6],  # 0 = under 16
            "WKHP": [40, 35, 40, 0, 0, 0, 0],
            "JWTRNS": [1, 2, 11, None, None, None, None],
            "JWRIP": [1, None, None, None, None, None, None],
            "SCHG": [None, None, None, None, 7, 5, None],
            "SCHL": [21, 22, 24, 16, 5, 3, 17],
            "RAC1P": [1, 6, 2, 1, 1, 1, 9],
            "HISP": [1, 1, 1, 3, 1, 1, 2],
        }
    )


# ---------------------------------------------------------------------------
# PUMSSource / ControlSpec dataclass tests
# ---------------------------------------------------------------------------
class TestDataclasses:
    """Tests for PUMSSource and ControlSpec dataclasses."""

    def test_pums_source_defaults(self):
        """PUMSSource should have correct defaults."""
        src = PUMSSource(state_fips="06", pums_year=2022)
        assert src.state_fips == "06"
        assert src.pums_year == 2022
        assert src.puma_ids is None

    def test_pums_source_with_pumas(self):
        """PUMSSource should accept puma_ids."""
        src = PUMSSource(state_fips="06", pums_year=2022, puma_ids=["00100"])
        assert src.puma_ids == ["00100"]

    def test_control_spec_defaults(self):
        """ControlSpec should have correct defaults."""
        spec = ControlSpec(name="hh_size")
        assert spec.name == "hh_size"


# ---------------------------------------------------------------------------
# recode_pums_households
# ---------------------------------------------------------------------------
class TestRecodePumsHouseholds:
    """Tests for recoding PUMS household controls."""

    def test_creates_ctrl_columns(self, pums_households, pums_persons):
        """Recode should create expected control columns."""
        result = recode_pums_households(pums_households, pums_persons)

        for col in [
            "h_size",
            "h_income",
            "h_vehicles",
            "h_workers",
            "h_children",
        ]:
            assert col in result.columns, f"Missing column: {col}"

    def test_hh_size_recode(self, pums_households, pums_persons):
        """Household size control is derived from person count."""
        result = recode_pums_households(pums_households, pums_persons)
        sizes = result.sort("SERIALNO")["h_size"].to_list()
        # HH1: NP=2 → SIZE_2, HH2: NP=4 → SIZE_4, HH3: NP=1 → SIZE_1
        assert sizes == [
            int(HHSizeCategory.SIZE_2),
            int(HHSizeCategory.SIZE_4),
            int(HHSizeCategory.SIZE_1),
        ]

    def test_hh_income_recode(self, pums_households, pums_persons):
        """Household income control is derived from HINCP."""
        result = recode_pums_households(pums_households, pums_persons)
        incomes = result.sort("SERIALNO")["h_income"].to_list()
        # HH1: 55k → INCOME_50TO75, HH2: 120k → INCOME_100TO200, HH3: 15k → INCOME_UNDER25
        assert incomes == [
            IncomeBroad.INCOME_50TO75.value,
            IncomeBroad.INCOME_100TO200.value,
            IncomeBroad.INCOME_UNDER25.value,
        ]

    def test_hh_vehicles_recode(self, pums_households, pums_persons):
        """Household vehicles control is derived from VEH."""
        result = recode_pums_households(pums_households, pums_persons)
        vehs = result.sort("SERIALNO")["h_vehicles"].to_list()
        # HH1: 1 → VEH_1, HH2: 2 → VEH_2, HH3: 0 → VEH_0
        assert vehs == [
            int(HHVehiclesCategory.VEH_1),
            int(HHVehiclesCategory.VEH_2),
            int(HHVehiclesCategory.VEH_0),
        ]

    def test_hh_workers_derived(self, pums_households, pums_persons):
        """Number of workers control is derived from person employment status."""
        result = recode_pums_households(pums_households, pums_persons)
        workers = result.sort("SERIALNO")["h_workers"].to_list()
        # HH1: persons ESR=[1,1] → 2 workers → WORKERS_2
        # HH2: persons ESR=[1,6,0,0] → 1 worker → WORKERS_1
        # HH3: persons ESR=[6] → 0 workers → WORKERS_0
        assert workers == [
            int(HHWorkersCategory.WORKERS_2),
            int(HHWorkersCategory.WORKERS_1),
            int(HHWorkersCategory.WORKERS_0),
        ]

    def test_hh_children_derived(self, pums_households, pums_persons):
        """Number of children control is derived from person ages."""
        result = recode_pums_households(pums_households, pums_persons)
        children = result.sort("SERIALNO")["h_children"].to_list()
        # HH1: ages [35, 33] → 0 children
        # HH2: ages [40, 38, 10, 7] → 2 children (10 < 18, 7 < 18)
        # HH3: ages [65] → 0 children
        assert children == [
            int(HHChildrenCategory.CHILDREN_0),
            int(HHChildrenCategory.CHILDREN_2),
            int(HHChildrenCategory.CHILDREN_0),
        ]


# ---------------------------------------------------------------------------
# recode_pums_persons
# ---------------------------------------------------------------------------
class TestRecodePumsPersons:
    """Tests for recoding PUMS person controls."""

    def test_creates_ctrl_columns(self, pums_persons):
        """Recode should create expected control columns."""
        result = recode_pums_persons(pums_persons)
        expected_cols = [
            "p_gender",
            "p_employment",
            "p_commute_mode",
            "p_student",
            "p_education",
            "p_race",
            "p_ethnicity",
            "p_age",
        ]
        for col in expected_cols:
            assert col in result.columns, f"Missing column: {col}"

    def test_gender_recode(self, pums_persons):
        """Gender control is derived from PUMS SEX."""
        result = recode_pums_persons(pums_persons)
        genders = result.sort(["SERIALNO", "SPORDER"])["p_gender"].to_list()
        # SEX: [1, 2, 1, 2, 1, 2, 2] → [MALE, FEMALE, MALE, FEMALE, MALE, FEMALE, FEMALE]
        expected = [
            int(GenderCategory.MALE),
            int(GenderCategory.FEMALE),
            int(GenderCategory.MALE),
            int(GenderCategory.FEMALE),
            int(GenderCategory.MALE),
            int(GenderCategory.FEMALE),
            int(GenderCategory.FEMALE),
        ]
        assert genders == expected

    def test_age_recode(self, pums_persons):
        """Age control is derived from PUMS AGEP."""
        result = recode_pums_persons(pums_persons)
        ages = result.sort(["SERIALNO", "SPORDER"])["p_age"].to_list()
        # AGEP: [35, 33, 40, 38, 10, 7, 65]
        expected = [
            AgeCategory.AGE_35_TO_44.value,
            AgeCategory.AGE_25_TO_34.value,
            AgeCategory.AGE_35_TO_44.value,
            AgeCategory.AGE_35_TO_44.value,
            AgeCategory.AGE_5_TO_15.value,
            AgeCategory.AGE_5_TO_15.value,
            AgeCategory.AGE_65_TO_74.value,
        ]
        assert ages == expected

    def test_ethnicity_recode(self, pums_persons):
        """Ethnicity control is derived from PUMS HISP and RAC1P."""
        result = recode_pums_persons(pums_persons)
        eth = result.sort(["SERIALNO", "SPORDER"])["p_ethnicity"].to_list()
        # HISP: [1, 1, 1, 3, 1, 1, 2]
        expected = [
            Ethnicity.NOT_HISPANIC.value,
            Ethnicity.NOT_HISPANIC.value,
            Ethnicity.NOT_HISPANIC.value,
            Ethnicity.PUERTO_RICAN.value,
            Ethnicity.NOT_HISPANIC.value,
            Ethnicity.NOT_HISPANIC.value,
            Ethnicity.MEXICAN.value,
        ]
        assert eth == expected

    def test_commute_mode_with_carpool_detection(self, pums_persons):
        """Commute mode control is derived from JWTRNS and JWRIP."""
        result = recode_pums_persons(pums_persons)
        modes = result.sort(["SERIALNO", "SPORDER"])["p_commute_mode"].to_list()
        # JWTRNS=[1, 2, 11, None, None, None, None], JWRIP=[1, None, None, ...]
        # Person 0: JWTRNS=1, JWRIP=1 → DRIVE_ALONE
        # Person 1: JWTRNS=2 → TRANSIT
        # Person 2: JWTRNS=11 → MOSTLY_REMOTE
        # Persons 3-6: None → NA
        assert modes[0] == int(CommuteModeCategory.DRIVE_ALONE)
        assert modes[1] == int(CommuteModeCategory.TRANSIT)
        assert modes[2] == int(CommuteModeCategory.MOSTLY_REMOTE)

    def test_student_recode(self, pums_persons):
        """Student status control is derived from SCHG and SCHL."""
        result = recode_pums_persons(pums_persons)
        students = result.sort(["SERIALNO", "SPORDER"])["p_student"].to_list()
        # SCHG: [None, None, None, None, 7, 5, None]
        # Persons 0-3,6: None → NOT_STUDENT
        # Person 4: SCHG=7 → STUDENT_K12
        # Person 5: SCHG=5 → STUDENT_K12
        assert students[0] == int(StudentCategory.NOT_STUDENT)
        assert students[4] == int(StudentCategory.STUDENT_K12)
        assert students[5] == int(StudentCategory.STUDENT_K12)
        assert students[6] == int(StudentCategory.NOT_STUDENT)


# ---------------------------------------------------------------------------
# build_control_totals
# ---------------------------------------------------------------------------
class TestBuildControlTotals:
    """Tests for building control totals from recoded PUMS data."""

    def test_basic_aggregation(self, pums_households, pums_persons):
        """Basic aggregation should produce totals DataFrame."""
        hh_recoded = recode_pums_households(pums_households, pums_persons)
        per_recoded = recode_pums_persons(pums_persons)

        controls = [ControlSpec(name="h_size")]
        result = build_control_totals(hh_recoded, per_recoded, controls)

        assert isinstance(result, ControlTotals)
        assert result.pums_hh_count == 3
        assert result.pums_person_count == 7
        assert "target_total" in result.totals.columns
        assert "control_name" in result.totals.columns

    def test_totals_sum_to_weight(self, pums_households, pums_persons):
        """Control totals should sum to total WGTP within each PUMA."""
        hh_recoded = recode_pums_households(pums_households, pums_persons)
        per_recoded = recode_pums_persons(pums_persons)

        controls = [ControlSpec(name="h_size")]
        result = build_control_totals(hh_recoded, per_recoded, controls)

        # Sum of totals per geo should equal sum of WGTP for that geo
        total_by_geo = result.totals.group_by("geo_id").agg(pl.col("target_total").sum())
        # PUMA 00100: WGTP 100 + 200 = 300
        # PUMA 00200: WGTP 150
        puma_100 = total_by_geo.filter(pl.col("geo_id") == "00100")["target_total"][0]
        puma_200 = total_by_geo.filter(pl.col("geo_id") == "00200")["target_total"][0]
        assert puma_100 == pytest.approx(300.0)
        assert puma_200 == pytest.approx(150.0)

    def test_person_level_control(self, pums_households, pums_persons):
        """Person-level controls use PWGTP weights."""
        hh_recoded = recode_pums_households(pums_households, pums_persons)
        per_recoded = recode_pums_persons(pums_persons)

        controls = [ControlSpec(name="p_gender")]
        result = build_control_totals(hh_recoded, per_recoded, controls)

        assert len(result.totals) > 0
        assert all(result.totals["control_name"] == "p_gender")

    def test_multiple_controls(self, pums_households, pums_persons):
        """Multiple controls should all be included in totals."""
        hh_recoded = recode_pums_households(pums_households, pums_persons)
        per_recoded = recode_pums_persons(pums_persons)

        controls = [ControlSpec(name="h_size"), ControlSpec(name="p_gender")]
        result = build_control_totals(hh_recoded, per_recoded, controls)

        control_names = result.totals["control_name"].unique().sort().to_list()
        assert control_names == ["h_size", "p_gender"]

    def test_unknown_control_raises(self, pums_households, pums_persons):
        """Unknown control names should raise ValueError."""
        hh_recoded = recode_pums_households(pums_households, pums_persons)
        per_recoded = recode_pums_persons(pums_persons)

        with pytest.raises(ValueError, match="Unknown control name"):
            build_control_totals(hh_recoded, per_recoded, [ControlSpec(name="bogus")])

    def test_empty_controls_raises(self, pums_households, pums_persons):
        """No controls should raise ValueError."""
        hh_recoded = recode_pums_households(pums_households, pums_persons)
        per_recoded = recode_pums_persons(pums_persons)

        with pytest.raises(ValueError, match="No controls specified"):
            build_control_totals(hh_recoded, per_recoded, [])

    def test_granular_categories_preserved(self, pums_households, pums_persons):
        """Without merges, all granular categories are preserved in totals."""
        hh_recoded = recode_pums_households(pums_households, pums_persons)
        per_recoded = recode_pums_persons(pums_persons)

        controls = [ControlSpec(name="h_size")]
        result = build_control_totals(hh_recoded, per_recoded, controls)

        # Categories should be ints (enum values), not merged string labels
        categories = result.totals["category"].to_list()
        assert all(isinstance(c, int) for c in categories)

    def test_geo_ids_populated(self, pums_households, pums_persons):
        """Geo IDs in totals should match those in PUMS data."""
        hh_recoded = recode_pums_households(pums_households, pums_persons)
        per_recoded = recode_pums_persons(pums_persons)

        controls = [ControlSpec(name="h_size")]
        result = build_control_totals(hh_recoded, per_recoded, controls)

        assert set(result.geo_ids) == {"00100", "00200"}


# ---------------------------------------------------------------------------
# load_pums_from_files
# ---------------------------------------------------------------------------
class TestLoadPumsFromFiles:
    """Tests for loading PUMS data from files."""

    def test_load_csv(self, tmp_path, pums_households, pums_persons):
        """Loading from CSV should return correctly typed DataFrames."""
        hh_path = tmp_path / "hh.csv"
        per_path = tmp_path / "per.csv"
        pums_households.write_csv(hh_path)
        pums_persons.write_csv(per_path)

        hh, per = load_pums_from_files(str(hh_path), str(per_path))
        assert len(hh) == 3
        assert len(per) == 7

    def test_load_parquet(self, tmp_path, pums_households, pums_persons):
        """Loading from Parquet should return correctly typed DataFrames."""
        hh_path = tmp_path / "hh.parquet"
        per_path = tmp_path / "per.parquet"
        pums_households.write_parquet(hh_path)
        pums_persons.write_parquet(per_path)

        hh, per = load_pums_from_files(str(hh_path), str(per_path))
        assert len(hh) == 3
        assert len(per) == 7

    def test_puma_filter(self, tmp_path, pums_households, pums_persons):
        """Filtering by PUMA should return only matching records."""
        hh_path = tmp_path / "hh.csv"
        per_path = tmp_path / "per.csv"
        pums_households.write_csv(hh_path)
        pums_persons.write_csv(per_path)

        hh, _ = load_pums_from_files(str(hh_path), str(per_path), puma_ids=["00100"])
        assert all(hh["PUMA"].cast(pl.Utf8) == "00100")


# ---------------------------------------------------------------------------
# Zone grouping
# ---------------------------------------------------------------------------
class TestApplyZoneGroups:
    """Tests for apply_zone_groups."""

    @pytest.fixture
    def control_totals(self) -> ControlTotals:
        """Minimal ControlTotals fixture with 3 zones and 2 categories each."""
        totals = pl.DataFrame(
            {
                "geo_id": ["A", "A", "B", "B", "C", "C"],
                "control_name": ["ctrl"] * 6,
                "category": [1, 2, 1, 2, 1, 2],
                "target_total": [100.0, 200.0, 30.0, 40.0, 50.0, 60.0],
            }
        )
        return ControlTotals(
            totals=totals, pums_hh_count=10, pums_person_count=20, geo_ids=["A", "B", "C"]
        )

    @pytest.fixture
    def seed(self) -> pl.DataFrame:
        """Minimal seed DataFrame with hh_id and ctrl_geoid matching control_totals."""
        return pl.DataFrame(
            {
                "hh_id": [1, 2, 3],
                "ctrl_geoid": ["A", "B", "C"],
            }
        )

    def test_merges_zones(self, control_totals, seed):
        """Grouped zones should merge in both totals and seed."""
        groups = {"AB": ["A", "B"]}
        new_ct, new_seed = apply_zone_groups(control_totals, seed, groups)

        new_ids = sorted(new_ct.geo_ids)
        assert new_ids == ["AB", "C"]

        # Targets should sum
        ab_cat1 = new_ct.totals.filter((pl.col("geo_id") == "AB") & (pl.col("category") == 1))[
            "target_total"
        ].item()
        assert ab_cat1 == pytest.approx(130.0)  # 100 + 30

        # Seed geo_id remapped
        assert new_seed.filter(pl.col("hh_id") == 1)["ctrl_geoid"].item() == "AB"
        assert new_seed.filter(pl.col("hh_id") == 2)["ctrl_geoid"].item() == "AB"
        assert new_seed.filter(pl.col("hh_id") == 3)["ctrl_geoid"].item() == "C"

        # Original zone IDs preserved
        assert "_orig_ctrl_geoid" in new_seed.columns
        assert new_seed.filter(pl.col("hh_id") == 1)["_orig_ctrl_geoid"].item() == "A"
        assert new_seed.filter(pl.col("hh_id") == 2)["_orig_ctrl_geoid"].item() == "B"
        assert new_seed.filter(pl.col("hh_id") == 3)["_orig_ctrl_geoid"].item() == "C"

        # Zone group labels
        assert "zone_group" in new_seed.columns
        assert new_seed.filter(pl.col("hh_id") == 1)["zone_group"].item() == "AB"
        assert new_seed.filter(pl.col("hh_id") == 2)["zone_group"].item() == "AB"
        assert new_seed.filter(pl.col("hh_id") == 3)["zone_group"].item() is None

    def test_unmapped_zones_unchanged(self, control_totals, seed):
        """Zones not in any group pass through unchanged."""
        groups = {"AB": ["A", "B"]}
        new_ct, _ = apply_zone_groups(control_totals, seed, groups)
        c_total = new_ct.totals.filter((pl.col("geo_id") == "C") & (pl.col("category") == 2))[
            "target_total"
        ].item()
        assert c_total == pytest.approx(60.0)

    def test_preserves_pums_counts(self, control_totals, seed):
        """PUMS counts should be carried forward unchanged."""
        groups = {"AB": ["A", "B"]}
        new_ct, _ = apply_zone_groups(control_totals, seed, groups)
        assert new_ct.pums_hh_count == 10
        assert new_ct.pums_person_count == 20

    def test_duplicate_zone_raises(self, control_totals, seed):
        """A zone in two groups should raise ValueError."""
        groups = {"AB": ["A", "B"], "AC": ["A", "C"]}
        with pytest.raises(ValueError, match="multiple groups"):
            apply_zone_groups(control_totals, seed, groups)
