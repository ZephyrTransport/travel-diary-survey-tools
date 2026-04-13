"""Tests for weighting core seed_data module.

Exercises the survey-side recoding and seed table building functions using
small synthetic survey DataFrames.  Validates target-driven behavior: only
requested controls are produced, and missing required fields raise immediately.
"""

import polars as pl
import pytest

from data_canon.codebook.households import IncomeBroad
from data_canon.codebook.persons import AgeCategory
from processing.weighting.controls.enums import (
    CommuteModeCategory,
    EmploymentCategory,
    GenderCategory,
    HHChildrenCategory,
    HHSizeCategory,
    HHVehiclesCategory,
    HHWorkersCategory,
    StudentCategory,
)
from processing.weighting.data_prep.incidence import build_incidence_table
from processing.weighting.data_prep.seed_data import (
    recode_survey_households,
    recode_survey_persons,
)

# Convenience target lists used across multiple tests
HH_TARGETS = ["h_size", "h_income", "h_workers", "h_children"]
PERSON_TARGETS = [
    "p_gender",
    "p_employment",
    "p_commute_mode",
    "p_student",
    "p_age",
]
ALL_TARGETS = [*HH_TARGETS, *PERSON_TARGETS]


# ---------------------------------------------------------------------------
# Fixtures — synthetic canonical survey data
# ---------------------------------------------------------------------------
@pytest.fixture
def survey_households() -> pl.DataFrame:
    """Minimal canonical households."""
    return pl.DataFrame(
        {
            "hh_id": [1, 2, 3],
            "income_bin": [1, 5, 3],  # Under 25k, $100-200k, $50-75k
            "hh_weight": [1.5, 2.0, 1.0],
            "ctrl_geoid": ["00100", "00100", "00200"],
        }
    )


@pytest.fixture
def survey_persons() -> pl.DataFrame:
    """Minimal canonical persons matching survey_households."""
    return pl.DataFrame(
        {
            "hh_id": [1, 1, 2, 2, 2, 3],
            "person_id": [101, 102, 201, 202, 203, 301],
            "age": [5, 4, 6, 3, 1, 9],  # canonical AgeCategory values
            "gender": [2, 1, 2, 1, 4, 2],  # 2=MALE, 1=FEMALE, 4=NON_BINARY
            "employment": [1, 5, 2, 5, 5, 3],  # 1=FT, 5=not looking, 2=PT, 3=self-emp
            "student": [2, 2, 2, 2, 0, 2],  # 2=non-student, 0=FT in-person
            "school_type": [None, None, None, None, 5, None],  # 5=K-12
            "work_mode": [1, None, 3, None, None, 11],
        }
    )


# ---------------------------------------------------------------------------
# recode_survey_households
# ---------------------------------------------------------------------------
class TestRecodeSurveyHouseholds:
    """Tests for recoding survey households into control categories."""

    def test_creates_ctrl_columns(self, survey_households, survey_persons):
        """Requested household control columns are created."""
        result = recode_survey_households(
            survey_households,
            survey_persons,
            HH_TARGETS,
        )
        for col in [
            "h_size",
            "h_income",
            "h_workers",
            "h_children",
        ]:
            assert col in result.columns, f"Missing column: {col}"

    def test_hh_size_from_person_count(self, survey_households, survey_persons):
        """Household size control is derived from person count, not a household field."""
        result = recode_survey_households(
            survey_households,
            survey_persons,
            ["h_size"],
        )
        sizes = result.sort("hh_id")["h_size"].to_list()
        # HH1: 2 persons → SIZE_2, HH2: 3 persons → SIZE_3, HH3: 1 person → SIZE_1
        assert sizes == [
            int(HHSizeCategory.SIZE_2),
            int(HHSizeCategory.SIZE_3),
            int(HHSizeCategory.SIZE_1),
        ]

    def test_hh_income_from_broad(self, survey_households, survey_persons):
        """Household income control is derived from broad income categories."""
        result = recode_survey_households(
            survey_households,
            survey_persons,
            ["h_income"],
        )
        incomes = result.sort("hh_id")["h_income"].to_list()
        # income_bin: 1=Under 25k, 5=$100-200k, 3=$50-75k
        assert incomes == [
            IncomeBroad.INCOME_UNDER25.value,
            IncomeBroad.INCOME_100TO200.value,
            IncomeBroad.INCOME_50TO75.value,
        ]

    def test_hh_workers_derived(self, survey_households, survey_persons):
        """Number of workers is derived from person-level employment."""
        result = recode_survey_households(
            survey_households,
            survey_persons,
            ["h_workers"],
        )
        workers = result.sort("hh_id")["h_workers"].to_list()
        # HH1: employment=[1,5] → 1 worker (FT)
        # HH2: employment=[2,5,5] → 1 worker (PT)
        # HH3: employment=[3] → 1 worker (self-emp)
        assert workers == [
            int(HHWorkersCategory.WORKERS_1),
            int(HHWorkersCategory.WORKERS_1),
            int(HHWorkersCategory.WORKERS_1),
        ]

    def test_hh_children_derived(self, survey_households, survey_persons):
        """Number of children is derived from person-level age."""
        result = recode_survey_households(
            survey_households,
            survey_persons,
            ["h_children"],
        )
        children = result.sort("hh_id")["h_children"].to_list()
        # Child age categories: 1=Under 5, 2=5-15, 3=16-17 → counted as children
        # HH1: age=[5,4] → AgeCategory 5=25-34, 4=18-24 → 0 children
        # HH2: age=[6,3,1] → AgeCategory 6=35-44, 3=16-17, 1=Under 5 → 2 children
        # HH3: age=[9] → AgeCategory 9=65-74 → 0 children
        assert children == [
            int(HHChildrenCategory.CHILDREN_0),
            int(HHChildrenCategory.CHILDREN_2),
            int(HHChildrenCategory.CHILDREN_0),
        ]

    def test_vehicles_recode(self, survey_households, survey_persons):
        """Number of vehicles control is derived from num_vehicles field."""
        hh = survey_households.with_columns(
            pl.Series("num_vehicles", [0, 2, 1]),
        )
        result = recode_survey_households(
            hh,
            survey_persons,
            ["h_vehicles"],
        )
        vehs = result.sort("hh_id")["h_vehicles"].to_list()
        assert vehs == [
            int(HHVehiclesCategory.VEH_0),
            int(HHVehiclesCategory.VEH_2),
            int(HHVehiclesCategory.VEH_1),
        ]

    def test_vehicles_missing_field_raises(
        self,
        survey_households,
        survey_persons,
    ):
        """Requesting a control whose source field is absent must raise a KeyError immediately."""
        with pytest.raises(KeyError, match="num_vehicles"):
            recode_survey_households(
                survey_households,
                survey_persons,
                ["h_vehicles"],
            )

    def test_missing_income_field_raises(self, survey_persons):
        """Requesting income control when income_bin field is absent must raise."""
        hh_no_income = pl.DataFrame(
            {
                "hh_id": [1],
                "hh_weight": [1.0],
                "ctrl_geoid": ["00100"],
            }
        )
        with pytest.raises(KeyError, match="income_bin"):
            recode_survey_households(hh_no_income, survey_persons, ["h_income"])

    def test_only_requested_columns_created(
        self,
        survey_households,
        survey_persons,
    ):
        """Only requested control columns are created."""
        result = recode_survey_households(
            survey_households,
            survey_persons,
            ["h_size"],
        )
        hh_ctrl_cols = [c for c in result.columns if c.startswith("h_")]
        assert hh_ctrl_cols == ["h_size"]

    def test_person_targets_ignored(self, survey_households, survey_persons):
        """Person-level targets in the list are silently skipped."""
        result = recode_survey_households(
            survey_households,
            survey_persons,
            ["h_size", "p_gender"],
        )
        hh_ctrl_cols = [c for c in result.columns if c.startswith("h_")]
        assert hh_ctrl_cols == ["h_size"]

    def test_unknown_target_raises(self, survey_households, survey_persons):
        """Requesting an unknown target must raise a ValueError."""
        with pytest.raises(ValueError, match="Unknown targets"):
            recode_survey_households(
                survey_households,
                survey_persons,
                ["bogus"],
            )


# ---------------------------------------------------------------------------
# recode_survey_persons
# ---------------------------------------------------------------------------
class TestRecodeSurveyPersons:
    """Tests for recoding survey persons into control categories."""

    def test_creates_ctrl_columns(self, survey_persons):
        """Requested person control columns are created."""
        result = recode_survey_persons(survey_persons, PERSON_TARGETS)
        for col in [
            "p_gender",
            "p_employment",
            "p_commute_mode",
            "p_student",
            "p_age",
        ]:
            assert col in result.columns, f"Missing column: {col}"

    def test_gender_recode(self, survey_persons):
        """Gender."""
        result = recode_survey_persons(survey_persons, ["p_gender"])
        genders = result.sort("person_id")["p_gender"].to_list()
        # gender: [2, 1, 2, 1, 4, 2] → [MALE, FEMALE, MALE, FEMALE, None, MALE]
        expected = [
            int(GenderCategory.MALE),
            int(GenderCategory.FEMALE),
            int(GenderCategory.MALE),
            int(GenderCategory.FEMALE),
            None,  # gender=4 (NON_BINARY) has no PUMS mapping → null
            int(GenderCategory.MALE),
        ]
        assert genders == expected

    def test_employment_recode(self, survey_persons):
        """Employment status."""
        result = recode_survey_persons(survey_persons, ["p_employment"])
        emps = result.sort("person_id")["p_employment"].to_list()
        # employment: [1, 5, 2, 5, 5, 3]
        # 1=FT→EMPLOYED_FULL, 5=not looking→NOT_EMPLOYED, 2=PT→EMPLOYED_PART,
        # 3=self-emp→EMPLOYED_FULL
        expected = [
            int(EmploymentCategory.EMPLOYED_FULL),
            int(EmploymentCategory.NOT_EMPLOYED),
            int(EmploymentCategory.EMPLOYED_PART),
            int(EmploymentCategory.NOT_EMPLOYED),
            int(EmploymentCategory.NOT_EMPLOYED),
            int(EmploymentCategory.EMPLOYED_FULL),
        ]
        assert emps == expected

    def test_student_recode_with_school_type(self, survey_persons):
        """Student status is derived from student, school_type, and age fields."""
        result = recode_survey_persons(survey_persons, ["p_student"])
        students = result.sort("person_id")["p_student"].to_list()
        # student: [2, 2, 2, 2, 0, 2], school_type: [None, None, None, None, 5, None]
        # age:     [5, 4, 6, 3, 1, 9]
        # Persons with student=2 (NONSTUDENT) → NOT_STUDENT (tier 1)
        # Person 203: student=0, school_type=5 (ELEMENTARY) → K12 (tier 2)
        assert students[0] == int(StudentCategory.NOT_STUDENT)
        assert students[4] == int(StudentCategory.STUDENT_K12)  # person_id=203

    def test_student_tier1_explicit_nonstudent(self):
        """Tier 1: explicit non-student overrides everything, even K-12 school_type."""
        persons = pl.DataFrame(
            {
                "hh_id": [1, 1],
                "person_id": [1, 2],
                "student": [2, 2],  # NONSTUDENT
                "school_type": [5, 12],  # ELEMENTARY, COLLEGE_4YEAR
                "age": [2, 5],  # AGE_5_TO_15, AGE_25_TO_34
            }
        )
        result = recode_survey_persons(persons, ["p_student"])
        vals = result.sort("person_id")["p_student"].to_list()
        assert vals == [int(StudentCategory.NOT_STUDENT)] * 2

    def test_student_tier2_k12_school_types(self):
        """Tier 2: K-12 school types → STUDENT_K12 (preschool through high school)."""
        # SchoolType: PRESCHOOL=3, HOME_SCHOOL=4, ELEMENTARY=5, MIDDLE=6, HIGH=7
        persons = pl.DataFrame(
            {
                "hh_id": [1] * 5,
                "person_id": list(range(1, 6)),
                "student": [995] * 5,  # MISSING (children < 16)
                "school_type": [3, 4, 5, 6, 7],
                "age": [1, 2, 2, 2, 3],  # various child ages
            }
        )
        result = recode_survey_persons(persons, ["p_student"])
        vals = result.sort("person_id")["p_student"].to_list()
        assert vals == [int(StudentCategory.STUDENT_K12)] * 5

    def test_student_tier3_college_school_types(self):
        """Tier 3: college school types → STUDENT_COLLEGE."""
        # SchoolType: VOCATIONAL=10, COLLEGE_2YEAR=11, COLLEGE_4YEAR=12, GRADUATE=13
        persons = pl.DataFrame(
            {
                "hh_id": [1] * 4,
                "person_id": list(range(1, 5)),
                "student": [995] * 4,  # MISSING
                "school_type": [10, 11, 12, 13],
                "age": [4, 4, 5, 5],  # adult ages
            }
        )
        result = recode_survey_persons(persons, ["p_student"])
        vals = result.sort("person_id")["p_student"].to_list()
        assert vals == [int(StudentCategory.STUDENT_COLLEGE)] * 4

    def test_student_tier4_childcare_not_school(self):
        """Tier 4: ATHOME/DAYCARE are not school in the Census sense → NOT_STUDENT."""
        # SchoolType: ATHOME=1, DAYCARE=2
        persons = pl.DataFrame(
            {
                "hh_id": [1, 1],
                "person_id": [1, 2],
                "student": [995, 995],  # MISSING
                "school_type": [1, 2],  # ATHOME, DAYCARE
                "age": [1, 1],  # AGE_UNDER_5
            }
        )
        result = recode_survey_persons(persons, ["p_student"])
        vals = result.sort("person_id")["p_student"].to_list()
        assert vals == [int(StudentCategory.NOT_STUDENT)] * 2

    def test_student_tier5_age_fallback_school_age(self):
        """Tier 5: both missing + school-age (5-17) → K12."""
        persons = pl.DataFrame(
            {
                "hh_id": [1, 1],
                "person_id": [1, 2],
                "student": [995, 995],  # MISSING
                "school_type": [None, 995],  # null and MISSING
                "age": [2, 3],  # AGE_5_TO_15, AGE_16_TO_17
            }
        )
        result = recode_survey_persons(persons, ["p_student"])
        vals = result.sort("person_id")["p_student"].to_list()
        assert vals == [int(StudentCategory.STUDENT_K12)] * 2

    def test_student_tier5_age_fallback_non_school_age(self):
        """Tier 5: both missing + non-school-age → NOT_STUDENT."""
        persons = pl.DataFrame(
            {
                "hh_id": [1, 1, 1],
                "person_id": [1, 2, 3],
                "student": [995, 995, 995],  # MISSING
                "school_type": [None, None, 995],  # missing
                "age": [1, 4, 9],  # UNDER_5, 18-24, 65-74
            }
        )
        result = recode_survey_persons(persons, ["p_student"])
        vals = result.sort("person_id")["p_student"].to_list()
        assert vals == [int(StudentCategory.NOT_STUDENT)] * 3

    def test_student_tier6_active_student_missing_school_type(self):
        """Tier 6: active student with missing school_type → null (no dangerous default)."""
        # Student: FULLTIME_INPERSON=0, PARTTIME_ONLINE=3
        persons = pl.DataFrame(
            {
                "hh_id": [1, 1],
                "person_id": [1, 2],
                "student": [0, 3],  # active students
                "school_type": [None, 995],  # missing school_type
                "age": [4, 5],  # adult ages
            }
        )
        result = recode_survey_persons(persons, ["p_student"])
        vals = result.sort("person_id")["p_student"].to_list()
        assert vals == [None, None]

    def test_age_recode(self, survey_persons):
        """Age category recode should map canonical AgeCategory values to control categories."""
        result = recode_survey_persons(survey_persons, ["p_age"])
        ages = result.sort("person_id")["p_age"].to_list()
        # age (AgeCategory values): [5, 4, 6, 3, 1, 9]
        # 5=25-34, 4=18-24, 6=35-44, 3=16-17, 1=Under 5, 9=65-74
        expected = [
            AgeCategory.AGE_25_TO_34.value,
            AgeCategory.AGE_18_TO_24.value,
            AgeCategory.AGE_35_TO_44.value,
            AgeCategory.AGE_16_TO_17.value,
            AgeCategory.AGE_UNDER_5.value,
            AgeCategory.AGE_65_TO_74.value,
        ]
        assert ages == expected

    def test_commute_mode_recode(self, survey_persons):
        """Commute mode recode should map canonical work_mode values to control categories."""
        result = recode_survey_persons(survey_persons, ["p_commute_mode"])
        modes = result.sort("person_id")["p_commute_mode"].to_list()
        # work_mode: [1, None, 3, None, None, 11]
        # Mode.WALK=1 → WALK, None → NA, Mode.BIKE_BORROWED=3 → BIKE,
        # Mode.HOUSEHOLD_VEHICLE_6=11 → DRIVE_ALONE
        expected = [
            int(CommuteModeCategory.WALK),
            int(CommuteModeCategory.NA),
            int(CommuteModeCategory.BIKE),
            int(CommuteModeCategory.NA),
            int(CommuteModeCategory.NA),
            int(CommuteModeCategory.DRIVE_ALONE),
        ]
        assert modes == expected

    def test_mostly_remote_job_type_wfh(self):
        """job_type == 3 (WFH) → MOSTLY_REMOTE regardless of other fields."""
        persons = pl.DataFrame(
            {
                "hh_id": [1, 1],
                "person_id": [1, 2],
                "work_mode": [1, None],  # walk, null
                "job_type": [3, 3],  # WFH for both
                "telework_freq": [None, None],
                "commute_freq": [None, None],
            }
        )
        result = recode_survey_persons(persons, ["p_commute_mode"])
        modes = result.sort("person_id")["p_commute_mode"].to_list()
        assert modes == [
            int(CommuteModeCategory.MOSTLY_REMOTE),
            int(CommuteModeCategory.MOSTLY_REMOTE),
        ]

    def test_mostly_remote_high_telework_rarely_commutes(self):
        """Telework 5+ days/week AND rarely/never commutes → MOSTLY_REMOTE."""
        persons = pl.DataFrame(
            {
                "hh_id": [1, 1, 1],
                "person_id": [1, 2, 3],
                "work_mode": [1, None, 1],
                "job_type": [1, 5, 1],  # not WFH
                "telework_freq": [2, 1, 2],  # 5 days, 6-7 days, 5 days
                "commute_freq": [996, 8, 3],  # never, less than monthly, 4 days/week
            }
        )
        result = recode_survey_persons(persons, ["p_commute_mode"])
        modes = result.sort("person_id")["p_commute_mode"].to_list()
        assert modes[0] == int(CommuteModeCategory.MOSTLY_REMOTE)  # 5 days tw + never commute
        assert modes[1] == int(CommuteModeCategory.MOSTLY_REMOTE)  # 6-7 days tw + less than monthly
        assert modes[2] != int(
            CommuteModeCategory.MOSTLY_REMOTE
        )  # 5 days tw + 4 days commute (close split)

    def test_mostly_remote_telework_ratio_over_60(self):
        """Telework ratio >60%: commute_freq > telework_freq + 1 → MOSTLY_REMOTE."""
        persons = pl.DataFrame(
            {
                "hh_id": [1, 1, 1, 1],
                "person_id": [1, 2, 3, 4],
                "work_mode": [1, 1, 1, 1],
                "job_type": [1, 1, 1, 1],
                "telework_freq": [2, 3, 4, 2],  # 5 days, 4 days, 3 days, 5 days
                "commute_freq": [5, 6, 6, 3],  # 2 days, 1 day, 1 day, 4 days
            }
        )
        result = recode_survey_persons(persons, ["p_commute_mode"])
        modes = result.sort("person_id")["p_commute_mode"].to_list()
        # tw=2, cf=5: diff=3 > 1 → MOSTLY_REMOTE
        assert modes[0] == int(CommuteModeCategory.MOSTLY_REMOTE)
        # tw=3, cf=6: diff=3 > 1 → MOSTLY_REMOTE
        assert modes[1] == int(CommuteModeCategory.MOSTLY_REMOTE)
        # tw=4, cf=6: diff=2 > 1 → MOSTLY_REMOTE
        assert modes[2] == int(CommuteModeCategory.MOSTLY_REMOTE)
        # tw=2, cf=3: diff=1, NOT > 1 → falls through to work_mode
        assert modes[3] != int(CommuteModeCategory.MOSTLY_REMOTE)

    def test_not_mostly_remote_about_half(self):
        """Roughly equal telework/commute (within 1 step) → NOT mostly remote."""
        persons = pl.DataFrame(
            {
                "hh_id": [1, 1],
                "person_id": [1, 2],
                "work_mode": [1, None],
                "job_type": [1, 1],
                "telework_freq": [3, 4],  # 4 days, 3 days
                "commute_freq": [4, 4],  # 3 days, 3 days
            }
        )
        result = recode_survey_persons(persons, ["p_commute_mode"])
        modes = result.sort("person_id")["p_commute_mode"].to_list()
        # diff=1 ≤ 1 → not mostly remote → falls through to work_mode
        assert modes[0] == int(CommuteModeCategory.WALK)  # work_mode=1
        assert modes[1] == int(CommuteModeCategory.NA)  # work_mode=None

    def test_not_mostly_remote_missing_freq(self):
        """Missing telework/commute freq → falls through to work_mode."""
        persons = pl.DataFrame(
            {
                "hh_id": [1, 1],
                "person_id": [1, 2],
                "work_mode": [1, None],
                "job_type": [1, 1],
                "telework_freq": [995, None],
                "commute_freq": [None, 995],
            }
        )
        result = recode_survey_persons(persons, ["p_commute_mode"])
        modes = result.sort("person_id")["p_commute_mode"].to_list()
        assert modes[0] == int(CommuteModeCategory.WALK)
        assert modes[1] == int(CommuteModeCategory.NA)

    def test_missing_field_raises(self):
        """Requesting a target whose source field is absent must raise."""
        persons_minimal = pl.DataFrame(
            {
                "hh_id": [1],
                "person_id": [101],
            }
        )
        with pytest.raises(KeyError, match="gender"):
            recode_survey_persons(persons_minimal, ["p_gender"])

    def test_only_requested_columns_created(self, survey_persons):
        """Only requested control columns are created."""
        result = recode_survey_persons(survey_persons, ["p_age"])
        p_ctrl_cols = [c for c in result.columns if c.startswith("p_") and c != "person_id"]
        assert p_ctrl_cols == ["p_age"]

    def test_hh_targets_ignored(self, survey_persons):
        """Household-level targets in the list are silently skipped."""
        result = recode_survey_persons(survey_persons, ["h_size", "p_age"])
        p_ctrl_cols = [c for c in result.columns if c.startswith("p_") and c != "person_id"]
        assert p_ctrl_cols == ["p_age"]

    def test_unknown_target_raises(self, survey_persons):
        """Requesting an unknown target must raise a ValueError."""
        with pytest.raises(ValueError, match="Unknown targets"):
            recode_survey_persons(survey_persons, ["not_real"])


# ---------------------------------------------------------------------------
# build_incidence_table
# ---------------------------------------------------------------------------
class TestBuildIncidenceTable:
    """Tests for building the incidence table from recoded survey households and persons."""

    def test_basic_seed_table(self, survey_households, survey_persons):
        """Seed table should have one row per household, with hh_id preserved."""
        hh_recoded = recode_survey_households(
            survey_households,
            survey_persons,
            ALL_TARGETS,
        )
        per_recoded = recode_survey_persons(survey_persons, ALL_TARGETS)

        seed = build_incidence_table(
            hh_recoded, per_recoded, ALL_TARGETS, extra_cols=["ctrl_geoid"]
        ).incidence

        assert "hh_id" in seed.columns
        assert "ctrl_geoid" in seed.columns
        assert len(seed) == 3  # one row per household

    def test_hh_ctrl_columns_preserved(self, survey_households, survey_persons):
        """Household-level controls should be pivoted into indicator columns."""
        hh_recoded = recode_survey_households(
            survey_households,
            survey_persons,
            ALL_TARGETS,
        )
        per_recoded = recode_survey_persons(survey_persons, ALL_TARGETS)

        seed = build_incidence_table(hh_recoded, per_recoded, ALL_TARGETS).incidence

        # Non-structural HH controls are pivoted: h_size__one_person, h_income__*, etc.
        hh_indicator_cols = [c for c in seed.columns if c.startswith("h_size__")]
        assert len(hh_indicator_cols) > 0

    def test_person_incidence_columns_created(
        self,
        survey_households,
        survey_persons,
    ):
        """Seed table should have incidence columns for person-level controls."""
        hh_recoded = recode_survey_households(
            survey_households,
            survey_persons,
            ALL_TARGETS,
        )
        per_recoded = recode_survey_persons(survey_persons, ALL_TARGETS)

        seed = build_incidence_table(hh_recoded, per_recoded, ALL_TARGETS).incidence

        # Person incidence columns use p_ prefix, e.g. p_gender__male
        incidence_cols = [c for c in seed.columns if "__" in c]
        assert len(incidence_cols) > 0

    def test_incidence_sums_match_hh_size(
        self,
        survey_households,
        survey_persons,
    ):
        """For gender incidence, sum across categories should equal hh size."""
        hh_recoded = recode_survey_households(
            survey_households,
            survey_persons,
            ALL_TARGETS,
        )
        per_recoded = recode_survey_persons(survey_persons, ALL_TARGETS)

        seed = build_incidence_table(hh_recoded, per_recoded, ALL_TARGETS).incidence

        # Gender incidence columns
        gender_inc_cols = [c for c in seed.columns if c.startswith("p_gender__")]
        assert gender_inc_cols
        # For our fixtures, person with gender=4 (non-binary) maps to null,
        # so they are not counted in any gender incidence column.
        hh_sizes = (
            seed.sort("hh_id")
            .select(pl.sum_horizontal(gender_inc_cols).alias("gender_total"))["gender_total"]
            .to_list()
        )
        # HH1: 2 persons, HH2: 2 of 3 mapped (gender=4 excluded), HH3: 1 person
        assert hh_sizes == [2, 2, 1]

    def test_incidence_values_are_counts(
        self,
        survey_households,
        survey_persons,
    ):
        """Incidence values should be non-negative integers."""
        hh_recoded = recode_survey_households(
            survey_households,
            survey_persons,
            ALL_TARGETS,
        )
        per_recoded = recode_survey_persons(survey_persons, ALL_TARGETS)

        seed = build_incidence_table(hh_recoded, per_recoded, ALL_TARGETS).incidence

        incidence_cols = [c for c in seed.columns if "__" in c]
        for col in incidence_cols:
            vals = seed[col].to_list()
            assert all(v >= 0 for v in vals), f"Negative incidence in {col}"
            assert all(isinstance(v, int) for v in vals), f"Non-integer incidence in {col}"
