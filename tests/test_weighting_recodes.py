"""The survey and PUMS recodes, which must land in the same control categories."""

from types import SimpleNamespace

import polars as pl
import pytest

from data_canon.codebook.households import IncomeBroad
from data_canon.codebook.persons import AgeCategory, Education, Ethnicity, Race
from processing.weighting.controls.enums import (
    CommuteModeCategory,
    EmploymentCategory,
    GenderCategory,
    HHChildrenCategory,
    HHSizeCategory,
    HHVehiclesCategory,
    HHVehiclesPerDriverCategory,
    HHWorkersCategory,
    RaceEthnicityCategory,
    StudentCategory,
)
from processing.weighting.controls.registry import (
    CONTROLS,
    register_puma_control,
    register_target_zone_control,
)
from processing.weighting.core.pipeline import WeightingPipeline
from processing.weighting.core.specs import ControlRegistryConfig
from processing.weighting.data_prep.control_data import (
    recode_allocated_pums,
    recode_pums_households,
    recode_pums_persons,
)
from processing.weighting.data_prep.fractional_impute import fill_null_incidence
from processing.weighting.data_prep.incidence import (
    build_incidence_table,
    pivot_allocated_controls,
)
from processing.weighting.data_prep.seed_data import (
    recode_survey_households,
    recode_survey_persons,
)
from processing.weighting.validation.checksums import check_incidence_sums

HH_TARGETS = ["h_size", "h_income", "h_workers", "h_children"]
PERSON_TARGETS = [
    "p_gender",
    "p_employment",
    "p_commute_mode",
    "p_student",
    "p_age",
]
ALL_TARGETS = [*HH_TARGETS, *PERSON_TARGETS]

# Short names so the parametrized tables read as tables
_NOT_STUDENT = int(StudentCategory.NOT_STUDENT)
_K12 = int(StudentCategory.STUDENT_K12)
_COLLEGE = int(StudentCategory.STUDENT_COLLEGE)
_REMOTE = int(CommuteModeCategory.MOSTLY_REMOTE)
_WALK = int(CommuteModeCategory.WALK)
_NA = int(CommuteModeCategory.NA)


@pytest.fixture
def survey_households() -> pl.DataFrame:
    """Minimal canonical households."""
    return pl.DataFrame(
        {
            "hh_id": [1, 2, 3],
            "income_bin": [1, 5, 3],  # Under 25k, $100-200k, $50-75k
            "hh_weight": [1.5, 2.0, 1.0],
            "zone_group_id": ["00100", "00100", "00200"],
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
            # Workers who rarely telework, so work_mode decides their commute mode
            "job_type": [1, None, 1, None, None, 1],
            "telework_freq": [8, None, 7, None, None, 8],
            "commute_freq": [2, None, 2, None, None, 2],
        }
    )


class TestRecodeSurveyHouseholds:
    """Tests for recoding survey households into control categories."""

    @pytest.mark.parametrize(
        ("column", "expected"),
        [
            pytest.param(
                "h_size",
                # persons per household: 2, 3, 1
                [HHSizeCategory.SIZE_2, HHSizeCategory.SIZE_3, HHSizeCategory.SIZE_1],
                id="h_size_from_person_count",
            ),
            pytest.param(
                "h_income",
                [
                    IncomeBroad.INCOME_UNDER25,
                    IncomeBroad.INCOME_100TO200,
                    IncomeBroad.INCOME_50TO75,
                ],
                id="h_income_from_income_bin",
            ),
            pytest.param(
                "h_children",
                # ages 16-17 and under 5 are the two children, both in household 2
                [
                    HHChildrenCategory.CHILDREN_0,
                    HHChildrenCategory.CHILDREN_2,
                    HHChildrenCategory.CHILDREN_0,
                ],
                id="h_children_from_person_age",
            ),
            pytest.param(
                "h_vehicles",
                # num_vehicles: 0, 2, 1
                [HHVehiclesCategory.VEH_0, HHVehiclesCategory.VEH_2, HHVehiclesCategory.VEH_1],
                id="h_vehicles_from_num_vehicles",
            ),
        ],
    )
    def test_household_recode(self, survey_households, survey_persons, column, expected):
        """Each household control, for all three households, in hh_id order."""
        hh = survey_households.with_columns(pl.Series("num_vehicles", [0, 2, 1]))
        result = recode_survey_households(hh, survey_persons, [column])
        assert result.sort("hh_id")[column].to_list() == [v.value for v in expected]

    def test_hh_workers_derived(self):
        """Workers are counted from person employment; four households, four categories."""
        households = pl.DataFrame({"hh_id": [1, 2, 3, 4]})
        persons = pl.DataFrame(
            {
                # HH1: nobody works.  HH2: one full-timer among three adults.
                # HH3: a part-timer and a self-employed.  HH4: three workers.
                "hh_id": [1, 1, 2, 2, 2, 3, 3, 4, 4, 4],
                "person_id": [101, 102, 201, 202, 203, 301, 302, 401, 402, 403],
                "employment": [5, 5, 1, 5, 5, 2, 3, 1, 1, 2],
            }
        )
        result = recode_survey_households(households, persons, ["h_workers"])
        workers = result.sort("hh_id")["h_workers"].to_list()
        assert workers == [
            int(HHWorkersCategory.WORKERS_0),
            int(HHWorkersCategory.WORKERS_1),
            int(HHWorkersCategory.WORKERS_2),
            int(HHWorkersCategory.WORKERS_3),
        ]

    def test_hh_workers_count_employed_on_leave(self):
        """A person employed but on leave is a worker, as PUMS ESR counts them.

        Unpaid volunteers and interns stay out on both sides.
        """
        households = pl.DataFrame({"hh_id": [1, 2]})
        persons = pl.DataFrame(
            {
                # HH1: one on leave.  HH2: an unpaid intern and a full-timer.
                "hh_id": [1, 1, 2, 2],
                "person_id": [101, 102, 201, 202],
                "employment": [8, 5, 7, 1],
            }
        )
        result = recode_survey_households(households, persons, ["h_workers"])
        workers = result.sort("hh_id")["h_workers"].to_list()
        assert workers == [int(HHWorkersCategory.WORKERS_1), int(HHWorkersCategory.WORKERS_1)]

    @pytest.mark.parametrize(
        ("num_vehicles", "expected"),
        [
            pytest.param(
                [0, 2, 3],
                [
                    HHVehiclesPerDriverCategory.NONE,
                    HHVehiclesPerDriverCategory.ONE_TO_ONE,
                    HHVehiclesPerDriverCategory.MORE_THAN_DRIVERS,
                ],
                id="none_one_to_one_more",
            ),
            pytest.param(
                [1, 1, 1],
                [
                    HHVehiclesPerDriverCategory.LESS_THAN_DRIVERS,
                    HHVehiclesPerDriverCategory.LESS_THAN_DRIVERS,
                    HHVehiclesPerDriverCategory.ONE_TO_ONE,
                ],
                id="less_than_drivers",
            ),
        ],
    )
    def test_vehicles_per_driver_recode(
        self, survey_households, survey_persons, num_vehicles, expected
    ):
        """Drivers are 16+: household 2 has two (a 16-17 counts, an under-5 does not)."""
        hh = survey_households.with_columns(pl.Series("num_vehicles", num_vehicles))
        result = recode_survey_households(hh, survey_persons, ["h_vehicles_per_driver"])
        got = result.sort("hh_id")["h_vehicles_per_driver"].to_list()
        assert got == [int(c) for c in expected]

    @pytest.mark.parametrize(
        ("target", "drop_field", "missing_field"),
        [
            pytest.param("h_vehicles", None, "num_vehicles", id="vehicles"),
            pytest.param("h_income", "income_bin", "income_bin", id="income"),
        ],
    )
    def test_missing_source_field_raises(
        self, survey_households, survey_persons, target, drop_field, missing_field
    ):
        """A control whose source field is absent raises immediately, naming it."""
        hh = survey_households
        if drop_field is not None:
            hh = hh.drop(drop_field)
        with pytest.raises(KeyError, match=missing_field):
            recode_survey_households(hh, survey_persons, [target])

    @pytest.mark.parametrize(
        ("targets", "expected"),
        [
            pytest.param(["h_size"], ["h_size"], id="only_what_was_asked_for"),
            pytest.param(["h_size", "p_gender"], ["h_size"], id="person_targets_ignored"),
        ],
    )
    def test_only_requested_columns_created(
        self, survey_households, survey_persons, targets, expected
    ):
        """Only household controls that were requested are created."""
        result = recode_survey_households(survey_households, survey_persons, targets)
        assert [c for c in result.columns if c.startswith("h_")] == expected


class TestRecodeSurveyPersons:
    """Tests for recoding survey persons into control categories."""

    @pytest.mark.parametrize(
        ("column", "expected"),
        [
            pytest.param(
                "p_gender",
                # non-binary (person 5) has no PUMS category
                [
                    GenderCategory.MALE,
                    GenderCategory.FEMALE,
                    GenderCategory.MALE,
                    GenderCategory.FEMALE,
                    None,
                    GenderCategory.MALE,
                ],
                id="p_gender_from_gender",
            ),
            pytest.param(
                "p_employment",
                # full-time, not looking, part-time, not looking x 2, self-employed
                [
                    EmploymentCategory.EMPLOYED_FULL,
                    EmploymentCategory.NOT_EMPLOYED,
                    EmploymentCategory.EMPLOYED_PART,
                    EmploymentCategory.NOT_EMPLOYED,
                    EmploymentCategory.NOT_EMPLOYED,
                    EmploymentCategory.EMPLOYED_FULL,
                ],
                id="p_employment_from_employment",
            ),
            pytest.param(
                "p_age",
                [
                    AgeCategory.AGE_25_TO_34,
                    AgeCategory.AGE_18_TO_24,
                    AgeCategory.AGE_35_TO_44,
                    AgeCategory.AGE_16_TO_17,
                    AgeCategory.AGE_UNDER_5,
                    AgeCategory.AGE_65_TO_74,
                ],
                id="p_age_from_age",
            ),
            pytest.param(
                "p_commute_mode",
                # work_mode: walk, none, borrowed bike, none, none, household vehicle
                [
                    CommuteModeCategory.WALK,
                    CommuteModeCategory.NA,
                    CommuteModeCategory.BIKE,
                    CommuteModeCategory.NA,
                    CommuteModeCategory.NA,
                    CommuteModeCategory.DRIVE_ALONE,
                ],
                id="p_commute_mode_from_work_mode",
            ),
        ],
    )
    def test_person_recode(self, survey_persons, column, expected):
        """Each person control, for all six persons, in person_id order."""
        result = recode_survey_persons(survey_persons, [column])
        got = result.sort("person_id")[column].to_list()
        assert got == [None if v is None else v.value for v in expected]

    @pytest.mark.parametrize(
        ("student", "school_type", "age", "expected"),
        [
            # Tier 1 — an explicit non-student beats any school_type
            pytest.param(2, 5, 2, _NOT_STUDENT, id="t1_nonstudent_over_elementary"),
            pytest.param(2, 12, 5, _NOT_STUDENT, id="t1_nonstudent_over_college"),
            # Tier 2 — preschool through high school
            pytest.param(995, 3, 1, _K12, id="t2_preschool"),
            pytest.param(995, 4, 2, _K12, id="t2_home_school"),
            pytest.param(995, 5, 2, _K12, id="t2_elementary"),
            pytest.param(995, 6, 2, _K12, id="t2_middle"),
            pytest.param(995, 7, 3, _K12, id="t2_high"),
            pytest.param(0, 5, 1, _K12, id="t2_active_student_with_school_type"),
            # Tier 3 — post-secondary
            pytest.param(995, 10, 4, _COLLEGE, id="t3_vocational"),
            pytest.param(995, 11, 4, _COLLEGE, id="t3_college_2yr"),
            pytest.param(995, 12, 5, _COLLEGE, id="t3_college_4yr"),
            pytest.param(995, 13, 5, _COLLEGE, id="t3_graduate"),
            # Tier 4 — childcare is not school in the Census sense
            pytest.param(995, 1, 1, _NOT_STUDENT, id="t4_at_home"),
            pytest.param(995, 2, 1, _NOT_STUDENT, id="t4_daycare"),
            # Tier 5 — nothing reported, fall back to age
            pytest.param(995, None, 2, _K12, id="t5_age_5_to_15"),
            pytest.param(995, 995, 3, _K12, id="t5_age_16_to_17"),
            pytest.param(995, None, 1, _NOT_STUDENT, id="t5_under_5"),
            pytest.param(995, None, 4, _NOT_STUDENT, id="t5_age_18_to_24"),
            pytest.param(995, 995, 9, _NOT_STUDENT, id="t5_age_65_to_74"),
            # Tier 6 — an active student with no school_type stays unknown
            pytest.param(0, None, 4, None, id="t6_fulltime_inperson_no_school_type"),
            pytest.param(3, 995, 5, None, id="t6_parttime_online_no_school_type"),
        ],
    )
    def test_student_recode(self, student, school_type, age, expected):
        """The student tier rules, one row per (student, school_type, age) case."""
        persons = pl.DataFrame(
            {
                "hh_id": [1],
                "person_id": [1],
                "student": [student],
                "school_type": pl.Series([school_type], dtype=pl.Int64),
                "age": [age],
            }
        )
        result = persons.select(CONTROLS["p_student"].survey_expr())
        assert result.to_series().to_list() == [expected]

    @pytest.mark.parametrize(
        ("education", "age", "expected"),
        [
            pytest.param(995, 1, Education.LESS_HIGH_SCHOOL.value, id="under_5_no_answer"),
            pytest.param(None, 3, Education.LESS_HIGH_SCHOOL.value, id="age_16_to_17_no_answer"),
            pytest.param(995, 4, None, id="adult_no_answer_stays_null"),
            pytest.param(999, 6, None, id="adult_prefer_not_stays_null"),
            pytest.param(3, 3, Education.SOME_COLLEGE.value, id="child_answer_kept"),
            pytest.param(6, 5, Education.BACHELORS.value, id="adult_answer_kept"),
        ],
    )
    def test_education_recode(self, education, age, expected):
        """Children with no answer are less than high school; adults are left to imputation."""
        persons = pl.DataFrame(
            {"education": pl.Series([education], dtype=pl.Int64), "age": [age]},
        )
        result = persons.select(CONTROLS["p_education"].survey_expr())
        assert result.to_series().to_list() == [expected]

    @pytest.mark.parametrize(
        ("ethnicity", "race", "expected"),
        [
            pytest.param(
                Ethnicity.NOT_HISPANIC, Race.WHITE, RaceEthnicityCategory.WHITE, id="not_hispanic"
            ),
            pytest.param(
                Ethnicity.MEXICAN,
                Race.WHITE,
                RaceEthnicityCategory.HISPANIC_ALLRACES,
                id="hispanic_whatever_the_race",
            ),
            pytest.param(
                Ethnicity.OTHER,
                Race.MISSING,
                RaceEthnicityCategory.HISPANIC_ALLRACES,
                id="hispanic_no_race",
            ),
            pytest.param(Ethnicity.NOT_HISPANIC, Race.PNTA, None, id="not_hispanic_no_race"),
            pytest.param(Ethnicity.MISSING, Race.WHITE, None, id="origin_missing"),
            pytest.param(Ethnicity.PNTA, Race.ASIAN, None, id="origin_prefer_not"),
        ],
    )
    def test_race_ethnicity_recode(self, ethnicity, race, expected):
        """Hispanic origin decides first; race only counts for persons not Hispanic."""
        persons = pl.DataFrame({"ethnicity": [ethnicity.value], "race": [race.value]})
        result = persons.select(CONTROLS["p_race_ethnicity"].survey_expr())
        assert result.to_series().to_list() == [expected if expected is None else expected.value]

    @pytest.mark.parametrize(
        ("work_mode", "job_type", "telework_freq", "commute_freq", "expected"),
        [
            # job_type 3 is work-from-home, and settles it on its own
            pytest.param(1, 3, None, None, _REMOTE, id="wfh_over_a_walk_commute"),
            pytest.param(None, 3, None, None, _REMOTE, id="wfh_with_no_work_mode"),
            # teleworks nearly every day and almost never travels in
            pytest.param(1, 1, 2, 996, _REMOTE, id="5_days_remote_never_commutes"),
            pytest.param(None, 5, 1, 8, _REMOTE, id="6_to_7_days_remote_monthly_commute"),
            # telework outweighs commuting by more than one frequency step
            pytest.param(1, 1, 2, 5, _REMOTE, id="5_days_remote_2_days_in"),
            pytest.param(1, 1, 3, 6, _REMOTE, id="4_days_remote_1_day_in"),
            pytest.param(1, 1, 4, 6, _REMOTE, id="3_days_remote_1_day_in"),
            # an even-ish split is not remote, so work_mode decides
            pytest.param(1, 1, 2, 3, _WALK, id="5_days_remote_4_days_in"),
            pytest.param(1, 1, 3, 4, _WALK, id="4_days_remote_3_days_in"),
            pytest.param(None, 1, 4, 4, _NA, id="3_days_remote_3_days_in_no_mode"),
            # nothing reported about frequency, so work_mode decides
            pytest.param(1, 1, 995, None, _WALK, id="no_frequencies_walks"),
            pytest.param(None, 1, None, 995, _NA, id="no_frequencies_no_mode"),
        ],
    )
    def test_commute_mode_remote_rules(
        self, work_mode, job_type, telework_freq, commute_freq, expected
    ):
        """When a worker counts as mostly remote, and what decides it otherwise."""
        persons = pl.DataFrame(
            {
                "hh_id": [1],
                "person_id": [1],
                "work_mode": pl.Series([work_mode], dtype=pl.Int64),
                "job_type": [job_type],
                "telework_freq": pl.Series([telework_freq], dtype=pl.Int64),
                "commute_freq": pl.Series([commute_freq], dtype=pl.Int64),
            }
        )
        result = persons.select(CONTROLS["p_commute_mode"].survey_expr())
        assert result.to_series().to_list() == [expected]

    @pytest.mark.parametrize(
        ("drop", "null", "target", "error"),
        [
            pytest.param("commute_freq", None, "p_commute_mode", KeyError, id="missing"),
            pytest.param(None, "gender", "p_gender", ValueError, id="all_null"),
            pytest.param(None, "employment", "h_workers", ValueError, id="all_null_count_input"),
        ],
    )
    def test_control_inputs_must_have_values(
        self, survey_households, survey_persons, drop, null, target, error
    ):
        """A configured control raises on a field that is absent or null on every row."""
        persons = survey_persons.drop(drop) if drop else survey_persons
        if null:
            persons = persons.with_columns(pl.lit(None, dtype=pl.Int64).alias(null))
        call = (
            (lambda: recode_survey_households(survey_households, persons, [target]))
            if target.startswith("h_")
            else (lambda: recode_survey_persons(persons, [target]))
        )
        with pytest.raises(error, match=drop or null):
            call()

    @pytest.mark.parametrize(
        ("targets", "expected"),
        [
            pytest.param(["p_age"], ["p_age"], id="only_what_was_asked_for"),
            pytest.param(["h_size", "p_age"], ["p_age"], id="household_targets_ignored"),
        ],
    )
    def test_only_requested_columns_created(self, survey_persons, targets, expected):
        """Only person controls that were requested are created."""
        result = recode_survey_persons(survey_persons, targets)
        p_ctrl_cols = [c for c in result.columns if c.startswith("p_") and c != "person_id"]
        assert p_ctrl_cols == expected


@pytest.mark.parametrize(
    "recode",
    [
        pytest.param("households", id="households"),
        pytest.param("persons", id="persons"),
    ],
)
def test_unknown_target_raises(survey_households, survey_persons, recode):
    """Neither recode accepts a target name that is not in the registry."""
    call = (
        (lambda: recode_survey_households(survey_households, survey_persons, ["bogus"]))
        if recode == "households"
        else (lambda: recode_survey_persons(survey_persons, ["bogus"]))
    )
    with pytest.raises(ValueError, match="Unknown targets"):
        call()


@pytest.fixture
def recoded(survey_households, survey_persons) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Both survey tables recoded against every target."""
    return (
        recode_survey_households(survey_households, survey_persons, ALL_TARGETS),
        recode_survey_persons(survey_persons, ALL_TARGETS),
    )


class TestBuildIncidenceTable:
    """Tests for building the incidence table from recoded survey households and persons."""

    def test_basic_seed_table(self, recoded):
        """One row per household, hh_id kept, and extra_cols carried through."""
        hh_recoded, per_recoded = recoded
        seed = build_incidence_table(
            hh_recoded, per_recoded, ALL_TARGETS, extra_cols=["zone_group_id"]
        ).incidence

        assert seed.height == 3
        assert seed.sort("hh_id")["hh_id"].to_list() == [1, 2, 3]
        assert seed.sort("hh_id")["zone_group_id"].to_list() == ["00100", "00100", "00200"]

    def test_incidence_sums_match_hh_size(self, recoded):
        """Gender incidence sums to the household's size, and every value is a count."""
        hh_recoded, per_recoded = recoded
        seed = build_incidence_table(hh_recoded, per_recoded, ALL_TARGETS).incidence

        # non-structural household controls pivot into indicator columns too
        assert [c for c in seed.columns if c.startswith("h_size__")]

        gender_inc_cols = [c for c in seed.columns if c.startswith("p_gender__")]
        assert gender_inc_cols
        hh_sizes = (
            seed.sort("hh_id")
            .select(pl.sum_horizontal(gender_inc_cols).alias("gender_total"))["gender_total"]
            .to_list()
        )
        # household 2's non-binary member is in no gender column
        assert hh_sizes == [2, 2, 1]

        for col in (c for c in seed.columns if "__" in c):
            vals = seed[col].to_list()
            assert all(isinstance(v, int) and v >= 0 for v in vals), f"Bad incidence in {col}"


class TestRecodePumsHouseholds:
    """Tests for recoding PUMS household controls.

    One row per control, so the whole household recode reads as a table.  All
    three households are listed for every control, so a constant fails.
    """

    @pytest.mark.parametrize(
        ("column", "expected"),
        [
            pytest.param(
                "h_size",
                # NP: 2, 4, 1
                [HHSizeCategory.SIZE_2, HHSizeCategory.SIZE_4, HHSizeCategory.SIZE_1],
                id="h_size_from_person_count",
            ),
            pytest.param(
                "h_income",
                # HINCP: 55k, 120k, 15k
                [
                    IncomeBroad.INCOME_50TO75,
                    IncomeBroad.INCOME_100TO200,
                    IncomeBroad.INCOME_UNDER25,
                ],
                id="h_income_from_hincp",
            ),
            pytest.param(
                "h_vehicles",
                # VEH: 1, 2, 0
                [HHVehiclesCategory.VEH_1, HHVehiclesCategory.VEH_2, HHVehiclesCategory.VEH_0],
                id="h_vehicles_from_veh",
            ),
            pytest.param(
                "h_vehicles_per_driver",
                # VEH: 1, 2, 0 against drivers 16+: [35,33], [40,38] (not 10, 7), [65]
                [
                    HHVehiclesPerDriverCategory.LESS_THAN_DRIVERS,
                    HHVehiclesPerDriverCategory.ONE_TO_ONE,
                    HHVehiclesPerDriverCategory.NONE,
                ],
                id="h_vehicles_per_driver_from_veh_and_agep",
            ),
            pytest.param(
                "h_workers",
                # ESR per household: [1,1], [1,6,0,0], [6]
                [
                    HHWorkersCategory.WORKERS_2,
                    HHWorkersCategory.WORKERS_1,
                    HHWorkersCategory.WORKERS_0,
                ],
                id="h_workers_from_person_esr",
            ),
            pytest.param(
                "h_children",
                # AGEP per household: [35,33], [40,38,10,7], [65]
                [
                    HHChildrenCategory.CHILDREN_0,
                    HHChildrenCategory.CHILDREN_2,
                    HHChildrenCategory.CHILDREN_0,
                ],
                id="h_children_from_person_agep",
            ),
        ],
    )
    def test_household_recode(self, pums_households, pums_persons, column, expected):
        """Each household control, for all three households, in SERIALNO order."""
        result = recode_pums_households(pums_households, pums_persons, [column])
        assert result.sort("SERIALNO")[column].to_list() == [v.value for v in expected]

    def test_vehicles_na_is_not_zero_vehicles(self):
        """VEH = -1 (N/A) stays null, so it stops the run instead of becoming 0 vehicles."""
        hh = pl.DataFrame({"VEH": pl.Series([-1, None, 0, 3, 8], dtype=pl.Int32)})
        got = hh.select(CONTROLS["h_vehicles"].pums_expr()).to_series().to_list()
        assert got == [None, None, 0, 3, 6]

    def test_vehicles_per_driver_na_is_not_a_category(self):
        """Not "none" (0 vehicles), and not "more than drivers", where a null would fall."""
        hh = pl.DataFrame(
            {"VEH": pl.Series([-1, None, 0, 2], dtype=pl.Int32), "_n_drivers": [2, 2, 2, 2]}
        )
        got = hh.select(CONTROLS["h_vehicles_per_driver"].pums_expr()).to_series().to_list()
        assert got == [
            None,
            None,
            HHVehiclesPerDriverCategory.NONE,
            HHVehiclesPerDriverCategory.ONE_TO_ONE,
        ]


class TestRecodePumsPersons:
    """Tests for recoding PUMS person controls.

    One row per control, every one of the seven persons asserted, so a
    constant or a partial mapping fails.
    """

    @pytest.mark.parametrize(
        ("column", "expected"),
        [
            pytest.param(
                "p_gender",
                # SEX: 1, 2, 1, 2, 1, 2, 2
                [
                    GenderCategory.MALE,
                    GenderCategory.FEMALE,
                    GenderCategory.MALE,
                    GenderCategory.FEMALE,
                    GenderCategory.MALE,
                    GenderCategory.FEMALE,
                    GenderCategory.FEMALE,
                ],
                id="p_gender_from_sex",
            ),
            pytest.param(
                "p_age",
                # AGEP: 35, 33, 40, 38, 10, 7, 65
                [
                    AgeCategory.AGE_35_TO_44,
                    AgeCategory.AGE_25_TO_34,
                    AgeCategory.AGE_35_TO_44,
                    AgeCategory.AGE_35_TO_44,
                    AgeCategory.AGE_5_TO_15,
                    AgeCategory.AGE_5_TO_15,
                    AgeCategory.AGE_65_TO_74,
                ],
                id="p_age_from_agep",
            ),
            pytest.param(
                "p_race_ethnicity",
                # RAC1P: 1, 6, 2, 1, 1, 1, 9; HISP: 1, 1, 1, 3, 1, 1, 2
                [
                    RaceEthnicityCategory.WHITE,
                    RaceEthnicityCategory.ASIAN,
                    RaceEthnicityCategory.AFAM,
                    RaceEthnicityCategory.HISPANIC_ALLRACES,
                    RaceEthnicityCategory.WHITE,
                    RaceEthnicityCategory.WHITE,
                    RaceEthnicityCategory.HISPANIC_ALLRACES,
                ],
                id="p_race_ethnicity_from_rac1p_and_hisp",
            ),
            pytest.param(
                "p_commute_mode",
                # JWTRNS: 1, 2, 11, then four nulls; JWRIP: 1, then nulls
                [
                    CommuteModeCategory.DRIVE_ALONE,
                    CommuteModeCategory.TRANSIT,
                    CommuteModeCategory.MOSTLY_REMOTE,
                    CommuteModeCategory.NA,
                    CommuteModeCategory.NA,
                    CommuteModeCategory.NA,
                    CommuteModeCategory.NA,
                ],
                id="p_commute_mode_from_jwtrns_and_jwrip",
            ),
            pytest.param(
                "p_student",
                # SCHG: nulls except person 5 (7) and person 6 (5)
                [
                    StudentCategory.NOT_STUDENT,
                    StudentCategory.NOT_STUDENT,
                    StudentCategory.NOT_STUDENT,
                    StudentCategory.NOT_STUDENT,
                    StudentCategory.STUDENT_K12,
                    StudentCategory.STUDENT_K12,
                    StudentCategory.NOT_STUDENT,
                ],
                id="p_student_from_schg",
            ),
        ],
    )
    def test_person_recode(self, pums_persons, column, expected):
        """Each person control, for all seven persons, in (SERIALNO, SPORDER) order."""
        result = recode_pums_persons(pums_persons, [column])
        got = result.sort(["SERIALNO", "SPORDER"])[column].to_list()
        assert got == [v.value for v in expected]

    def test_education_na_schl_is_less_than_high_school(self):
        """SCHL is N/A (0, or blank) under age 3; they count with the other children."""
        persons = pl.DataFrame({"SCHL": pl.Series([0, None, 1, 21], dtype=pl.Int64)})
        result = persons.select(CONTROLS["p_education"].pums_expr())
        assert result.to_series().to_list() == [
            Education.LESS_HIGH_SCHOOL.value,
            Education.LESS_HIGH_SCHOOL.value,
            Education.LESS_HIGH_SCHOOL.value,
            Education.BACHELORS.value,
        ]


class TestPumaControl:
    """The home-PUMA target, registered for this run's PUMAs and never imputed."""

    @pytest.fixture
    def puma_ctrl(self, _clean_registry):
        """``h_puma`` for the two PUMAs the PUMS fixtures live in."""
        register_puma_control(["00100", "00200"])
        return CONTROLS["h_puma"]

    def test_one_category_per_puma_plus_outside(self, puma_ctrl):
        """Numbered from 1 in the order given; 0 is outside the region."""
        assert puma_ctrl.valid_members == [
            (0, "OUTSIDE_REGION"),
            (1, "PUMA_00100"),
            (2, "PUMA_00200"),
        ]

    @pytest.mark.usefixtures("puma_ctrl")
    def test_registering_again_replaces_and_none_removes(self):
        """A second geography never keeps the first one's PUMAs."""
        register_puma_control(["00300"])
        assert [m for _, m in CONTROLS["h_puma"].valid_members] == ["OUTSIDE_REGION", "PUMA_00300"]
        register_puma_control(None)
        assert "h_puma" not in CONTROLS

    def test_survey_recode(self, puma_ctrl):
        """A home in no region PUMA, or in none at all, has no category."""
        hh = pl.DataFrame({"home_puma": ["00200", "00100", "09999", None]})
        assert hh.select(puma_ctrl.survey_expr()).to_series().to_list() == [2, 1, None, None]

    @pytest.mark.usefixtures("puma_ctrl")
    def test_pums_recode(self, pums_households, pums_persons):
        """Each PUMS household lands in its PUMA; one outside the region in OUTSIDE_REGION."""
        outside = pums_households.head(1).with_columns(
            pl.lit("HH4").alias("SERIALNO"), pl.lit("09999").alias("PUMA")
        )
        result = recode_pums_households(
            pl.concat([pums_households, outside]), pums_persons, ["h_puma"]
        )
        assert result.sort("SERIALNO")["h_puma"].to_list() == [1, 1, 2, 0]

    @pytest.mark.usefixtures("puma_ctrl")
    def test_a_household_with_no_puma_is_not_imputed(
        self, survey_households, survey_persons, pums_households, pums_persons
    ):
        """Imputation leaves it empty, so the seed's sum check stops the run."""
        names = ["h_total", "p_total", "h_size", "h_puma"]
        hh = survey_households.with_columns(pl.Series("home_puma", ["00100", "00200", None]))
        survey = build_incidence_table(
            recode_survey_households(hh, survey_persons, names),
            recode_survey_persons(survey_persons, names),
            names,
        )
        pums = build_incidence_table(
            recode_pums_households(pums_households, pums_persons, names),
            recode_pums_persons(pums_persons, names),
            names,
            hh_id_col="SERIALNO",
            extra_cols=["WGTP", "PUMA"],
            person_extra_cols=["PWGTP", "PUMA"],
        )
        one_zone = pl.lit("z1").alias("zone_group_id")
        seed, _ = fill_null_incidence(
            survey,
            pums.incidence.with_columns(one_zone),
            pums.person_pivot.with_columns(one_zone),
            survey.incidence.select("hh_id").with_columns(one_zone),
            names,
        )
        puma_cols = [c for c in seed.columns if c.startswith("h_puma__")]
        assert seed.sort("hh_id").select(pl.sum_horizontal(puma_cols))[:, 0].to_list() == [1, 1, 0]
        with pytest.raises(ValueError, match="h_puma"):
            check_incidence_sums(seed, names, source_label="survey", tolerance=0.01)


class TestTargetZoneControl:
    """The home target-zone target: survey from the home, PUMS from each allocated copy."""

    @pytest.fixture
    def zone_ctrl(self, _clean_registry):
        """``h_target_zone`` for three target zones."""
        register_target_zone_control(["001", "013", "081"])
        return CONTROLS["h_target_zone"]

    def test_one_category_per_target_zone(self, zone_ctrl):
        """Numbered from 1 in the order given, named by the zone ID."""
        assert zone_ctrl.valid_members == [(1, "001"), (2, "013"), (3, "081")]

    def test_survey_recode(self, zone_ctrl):
        """A home's own zone; one in no zone has no category."""
        hh = pl.DataFrame({"target_zone_id": ["013", "001", None]})
        assert hh.select(zone_ctrl.survey_expr()).to_series().to_list() == [2, 1, None]

    @pytest.mark.usefixtures("zone_ctrl")
    def test_pums_recode_leaves_it_for_after_allocation(self, pums_households, pums_persons):
        """Before allocation a PUMS record has no zone, so recode skips the control."""
        result = recode_pums_households(pums_households, pums_persons, ["h_total", "h_target_zone"])
        assert "h_target_zone" not in result.columns

    @pytest.mark.usefixtures("zone_ctrl")
    def test_each_allocated_copy_gets_its_own_zone(self):
        """A PUMA split across two zones: one record, two copies, one column each."""
        allocated = pl.DataFrame(
            {"SERIALNO": ["S1", "S1"], "target_zone_id": ["013", "001"], "WGTP": [90.0, 10.0]}
        )
        out = pivot_allocated_controls(allocated, ["h_total", "h_target_zone"])
        assert out.select(
            "h_target_zone__001", "h_target_zone__013", "h_target_zone__081"
        ).rows() == [
            (0, 1, 0),
            (1, 0, 0),
        ]
        coded = recode_allocated_pums(allocated, ["h_total", "h_target_zone"])
        assert coded["h_target_zone"].to_list() == [2, 1]

    @pytest.mark.usefixtures("zone_ctrl")
    def test_targets_per_zone_and_none_where_a_group_is_one_zone(self):
        """Group A holds zones 001 and 013; group B is zone 081 alone, so it gets none."""
        pipeline = object.__new__(WeightingPipeline)
        pipeline.controls = ControlRegistryConfig.from_yaml(
            [{"name": "h_total"}, {"name": "p_total"}, {"name": "h_target_zone"}]
        )
        pipeline.crosswalk = SimpleNamespace(
            crosswalk_df=pl.DataFrame(
                {"target_zone_id": ["001", "013", "081"], "zone_group_id": ["A", "A", "B"]}
            )
        )
        allocated = pl.DataFrame(
            {
                "SERIALNO": ["S1", "S1", "S2"],
                "target_zone_id": ["013", "001", "081"],
                "zone_group_id": ["A", "A", "B"],
                "WGTP": [90.0, 10.0, 50.0],
                "h_total": [1, 1, 1],
            }
        )
        pipeline.pums_incidence = pivot_allocated_controls(
            allocated, pipeline.controls.target_names
        )
        pipeline.pums_person_incidence = pl.DataFrame(
            {
                "SERIALNO": ["S1"],
                "SPORDER": [1],
                "zone_group_id": ["A"],
                "PWGTP": [10.0],
                "p_total": [1],
            }
        )
        pipeline.aggregate_totals()

        zones = pipeline.control_totals.totals.filter(
            (pl.col("control_name") == "h_target_zone") & (pl.col("target_total") > 0)
        )
        assert {
            (r["zone_group_id"], r["category"]): r["target_total"]
            for r in zones.iter_rows(named=True)
        } == {
            ("A", "001"): 10.0,
            ("A", "013"): 90.0,
        }
        assert zones.filter(pl.col("zone_group_id") == "B").is_empty()
