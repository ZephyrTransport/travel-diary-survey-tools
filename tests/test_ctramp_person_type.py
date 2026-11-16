"""CT-RAMP student category and person type, derived in the order ``format_persons`` uses.

The student and employment categories come first and person type reads them, so
each rule lives in one place. Explicit cases pin priority rules; Hypothesis
properties check invariants over every combination.
"""

import polars as pl
import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

from data_canon.codebook.ctramp import (
    CTRAMPEmploymentCategory,
    CTRAMPPersonType,
    CTRAMPStudentCategory,
)
from data_canon.codebook.persons import AgeCategory, Employment, SchoolType, Student
from processing.formatting.ctramp.format_persons import enrich_persons_with_person_type
from processing.formatting.ctramp.person_mappings import (
    EMPLOYMENT_TO_CTRAMP,
    ctramp_person_type_expression,
)
from processing.formatting.ctramp.student_mappings import (
    ctramp_student_category_expression,
    log_student_category_warnings,
)

GRADE = CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value
COLLEGE = CTRAMPStudentCategory.COLLEGE_OR_HIGHER.value
NOT_STUDENT = CTRAMPStudentCategory.NOT_STUDENT.value

# Column defaults for _people; each row overrides what it needs.
PERSON_DEFAULTS = {
    "employment": Employment.UNEMPLOYED_NOT_LOOKING,
    "student": Student.NONSTUDENT,
    "school_type": SchoolType.MISSING,
    "school_taz": 0,
    "work_taz": 0,
}
ADULTS_UNDER_65 = [
    AgeCategory.AGE_18_TO_24,
    AgeCategory.AGE_25_TO_34,
    AgeCategory.AGE_35_TO_44,
    AgeCategory.AGE_45_TO_54,
    AgeCategory.AGE_55_TO_64,
]
SENIORS = [AgeCategory.AGE_65_TO_74, AgeCategory.AGE_75_TO_84, AgeCategory.AGE_85_AND_UP]


def _people(rows: list[dict]) -> pl.DataFrame:
    """Persons with student_category, employment_category and then person_type derived."""
    filled = [
        {
            "person_id": i,
            **{k: getattr(v, "value", v) for k, v in {**PERSON_DEFAULTS, **row}.items()},
        }
        for i, row in enumerate(rows, start=1)
    ]
    df = pl.DataFrame(
        filled, schema_overrides={"student": pl.Int64, "school_type": pl.Int64}
    ).with_columns(
        ctramp_student_category_expression().alias("student_category"),
        pl.col("employment")
        .replace_strict(EMPLOYMENT_TO_CTRAMP, default=CTRAMPEmploymentCategory.NOT_EMPLOYED.value)
        .alias("employment_category"),
    )
    return df.with_columns(ctramp_person_type_expression().alias("person_type"))


def _student_category(age, student, school_type) -> int:
    return _people([{"age": age, "student": student, "school_type": school_type}])[
        "student_category"
    ][0]


def _person_type(age, employment, student, school_type) -> int:
    row = {"age": age, "employment": employment, "student": student, "school_type": school_type}
    return _people([row])["person_type"][0]


class TestStudentCategoryClassification:
    """Valid status and school type map by level; else children 5-17 fall back on age."""

    @pytest.mark.parametrize(
        ("school_type", "expected_category"),
        [
            (SchoolType.ELEMENTARY, GRADE),
            (SchoolType.MIDDLE_SCHOOL, GRADE),
            (SchoolType.HIGH_SCHOOL, GRADE),
            (SchoolType.HOME_SCHOOL, GRADE),
            (SchoolType.COLLEGE_2YEAR, COLLEGE),
            (SchoolType.COLLEGE_4YEAR, COLLEGE),
            (SchoolType.GRADUATE_SCHOOL, COLLEGE),
            (SchoolType.VOCATIONAL, COLLEGE),
            (SchoolType.DAYCARE, NOT_STUDENT),
            (SchoolType.PRESCHOOL, NOT_STUDENT),
            (SchoolType.ATHOME, NOT_STUDENT),
        ],
    )
    def test_valid_student_and_school_type(self, school_type, expected_category):
        """A full-time student is placed by the level of their school."""
        actual = _student_category(AgeCategory.AGE_18_TO_24, Student.FULLTIME_INPERSON, school_type)
        assert actual == expected_category

    @pytest.mark.parametrize(
        ("age", "expected_category"),
        [
            # Children 5-17 default to GRADE_OR_HIGH_SCHOOL (compulsory education).
            (AgeCategory.AGE_5_TO_15, GRADE),
            (AgeCategory.AGE_16_TO_17, GRADE),
            # No fallback below 5 or over 17.
            (AgeCategory.AGE_UNDER_5, NOT_STUDENT),
            (AgeCategory.AGE_18_TO_24, NOT_STUDENT),
            (AgeCategory.AGE_25_TO_34, NOT_STUDENT),
            (AgeCategory.AGE_35_TO_44, NOT_STUDENT),
            (AgeCategory.AGE_65_TO_74, NOT_STUDENT),
        ],
    )
    def test_missing_data_age_fallback(self, age, expected_category):
        """With both fields missing, age alone decides."""
        assert _student_category(age, Student.MISSING, SchoolType.MISSING) == expected_category

    @pytest.mark.parametrize(
        ("age", "student", "school_type", "expected_category"),
        [
            # A missing or unusable value in either field sends a child to the age
            # fallback, whatever the other field says.
            (AgeCategory.AGE_5_TO_15, Student.MISSING, SchoolType.ELEMENTARY, GRADE),
            (AgeCategory.AGE_5_TO_15, Student.MISSING, SchoolType.MIDDLE_SCHOOL, GRADE),
            (AgeCategory.AGE_5_TO_15, Student.MISSING, SchoolType.HIGH_SCHOOL, GRADE),
            (AgeCategory.AGE_5_TO_15, Student.FULLTIME_INPERSON, SchoolType.MISSING, GRADE),
            (AgeCategory.AGE_16_TO_17, Student.FULLTIME_INPERSON, SchoolType.MISSING, GRADE),
            # PNTA and OTHER are unusable school types, same as MISSING.
            (AgeCategory.AGE_5_TO_15, Student.NONSTUDENT, SchoolType.PNTA, GRADE),
            (AgeCategory.AGE_5_TO_15, Student.NONSTUDENT, SchoolType.OTHER, GRADE),
            (AgeCategory.AGE_5_TO_15, Student.MISSING, SchoolType.PNTA, GRADE),
            (AgeCategory.AGE_5_TO_15, Student.FULLTIME_INPERSON, SchoolType.OTHER, GRADE),
            # A child reported as a non-student still gets the fallback.
            (AgeCategory.AGE_5_TO_15, Student.NONSTUDENT, SchoolType.MISSING, GRADE),
            (AgeCategory.AGE_16_TO_17, Student.NONSTUDENT, SchoolType.MISSING, GRADE),
            (AgeCategory.AGE_5_TO_15, Student.NONSTUDENT, SchoolType.COLLEGE_4YEAR, GRADE),
            # Adults get no fallback: a non-student or missing status is NOT_STUDENT...
            (AgeCategory.AGE_25_TO_34, Student.NONSTUDENT, SchoolType.MISSING, NOT_STUDENT),
            (AgeCategory.AGE_25_TO_34, Student.NONSTUDENT, SchoolType.COLLEGE_4YEAR, NOT_STUDENT),
            (AgeCategory.AGE_25_TO_34, Student.MISSING, SchoolType.PNTA, NOT_STUDENT),
            # ...but a student with an unusable school type is assumed to be in college.
            (AgeCategory.AGE_25_TO_34, Student.FULLTIME_INPERSON, SchoolType.OTHER, COLLEGE),
            # Nulls behave as MISSING throughout.
            (AgeCategory.AGE_5_TO_15, None, SchoolType.ELEMENTARY, GRADE),
            (AgeCategory.AGE_25_TO_34, None, SchoolType.MISSING, NOT_STUDENT),
            (AgeCategory.AGE_16_TO_17, Student.FULLTIME_INPERSON, None, GRADE),
            (AgeCategory.AGE_18_TO_24, Student.FULLTIME_INPERSON, None, COLLEGE),
            (AgeCategory.AGE_5_TO_15, None, None, GRADE),
            (AgeCategory.AGE_16_TO_17, None, None, GRADE),
            (AgeCategory.AGE_25_TO_34, None, None, NOT_STUDENT),
        ],
    )
    def test_missing_or_unusable_combinations(self, age, student, school_type, expected_category):
        """The fallback and catch-all over the real-world data-quality combinations."""
        assert _student_category(age, student, school_type) == expected_category

    def test_valid_data_honored_despite_age_mismatch(self):
        """Valid data wins over age: a 5-15 year old in college is COLLEGE_OR_HIGHER."""
        result = _people(
            [
                {
                    "age": AgeCategory.AGE_5_TO_15,
                    "student": Student.FULLTIME_INPERSON,
                    "school_type": SchoolType.COLLEGE_4YEAR,
                },
                {
                    "age": AgeCategory.AGE_18_TO_24,
                    "student": Student.FULLTIME_INPERSON,
                    "school_type": SchoolType.ELEMENTARY,
                },
            ]
        )

        assert result["student_category"].to_list() == [COLLEGE, GRADE]


class TestStudentCategoryWarnings:
    """``log_student_category_warnings`` counts each data-quality problem it finds."""

    @pytest.mark.parametrize(
        ("rows", "expected_warnings"),
        [
            pytest.param(
                [
                    {"age": AgeCategory.AGE_5_TO_15, "student": Student.MISSING},
                    {"age": AgeCategory.AGE_16_TO_17, "student": Student.MISSING},
                    {"age": AgeCategory.AGE_18_TO_24, "student": Student.MISSING},
                ],
                {"missing_data_used_fallback": 2},
                id="only_children_use_the_age_fallback",
            ),
            pytest.param(
                [
                    {
                        "age": AgeCategory.AGE_UNDER_5,
                        "student": Student.FULLTIME_INPERSON,
                        "school_type": SchoolType.PRESCHOOL,
                    },
                    {
                        "age": AgeCategory.AGE_UNDER_5,
                        "student": Student.PARTTIME_INPERSON,
                        "school_type": SchoolType.DAYCARE,
                    },
                ],
                {"preschool_students": 2},
                id="students_under_five_are_impossible",
            ),
            pytest.param(
                [
                    # Teen in elementary, child in college, adult home-schooled.
                    {
                        "age": AgeCategory.AGE_16_TO_17,
                        "student": Student.FULLTIME_INPERSON,
                        "school_type": SchoolType.ELEMENTARY,
                    },
                    {
                        "age": AgeCategory.AGE_5_TO_15,
                        "student": Student.FULLTIME_INPERSON,
                        "school_type": SchoolType.COLLEGE_4YEAR,
                    },
                    {
                        "age": AgeCategory.AGE_35_TO_44,
                        "student": Student.PARTTIME_INPERSON,
                        "school_type": SchoolType.HOME_SCHOOL,
                    },
                ],
                {"age_inappropriate_school_types": 3},
                id="age_inappropriate_school_types",
            ),
            pytest.param(
                [
                    {
                        "age": AgeCategory.AGE_5_TO_15,
                        "student": Student.FULLTIME_INPERSON,
                        "school_type": SchoolType.ELEMENTARY,
                    },
                    {
                        "age": AgeCategory.AGE_18_TO_24,
                        "student": Student.FULLTIME_INPERSON,
                        "school_type": SchoolType.COLLEGE_4YEAR,
                    },
                    {"age": AgeCategory.AGE_25_TO_34, "student": Student.NONSTUDENT},
                ],
                {},
                id="valid_data_warns_about_nothing",
            ),
            pytest.param(
                [
                    # Persons 1 and 2 are full-time workers (employment beats study)
                    # with a school location but no work location. Person 3 is a
                    # non-student; person 4 has a work location.
                    {
                        "age": AgeCategory.AGE_18_TO_24,
                        "employment": Employment.EMPLOYED_FULLTIME,
                        "student": Student.FULLTIME_INPERSON,
                        "school_type": SchoolType.COLLEGE_4YEAR,
                        "school_taz": 100,
                    },
                    {
                        "age": AgeCategory.AGE_25_TO_34,
                        "employment": Employment.EMPLOYED_FULLTIME,
                        "student": Student.PARTTIME_INPERSON,
                        "school_type": SchoolType.COLLEGE_2YEAR,
                        "school_taz": 150,
                    },
                    {
                        "age": AgeCategory.AGE_18_TO_24,
                        "employment": Employment.EMPLOYED_FULLTIME,
                        "student": Student.NONSTUDENT,
                    },
                    {
                        "age": AgeCategory.AGE_25_TO_34,
                        "employment": Employment.EMPLOYED_FULLTIME,
                        "student": Student.NONSTUDENT,
                        "work_taz": 200,
                    },
                ],
                {"fulltime_workers_no_work_location": 2},
                id="working_students_without_a_work_location",
            ),
        ],
    )
    def test_warning_counts(self, rows, expected_warnings, standard_config):
        """Each row lists the persons and the exact counters they should produce."""
        assert log_student_category_warnings(_people(rows), standard_config) == expected_warnings


class TestPersonTypeClassification:
    """Person type from age, employment and the derived student category."""

    @pytest.mark.parametrize(
        ("age", "employment", "student", "school_type", "expected_type"),
        [
            # Age 16-17 is a driving-age student, worker or not.
            pytest.param(
                AgeCategory.AGE_16_TO_17,
                Employment.UNEMPLOYED_NOT_LOOKING,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.STUDENT_DRIVING_AGE,
                id="16_17_not_student_not_employed",
            ),
            # The student category recodes a school-age non-student with no school
            # location to grade school, and children stay children over a job.
            pytest.param(
                AgeCategory.AGE_16_TO_17,
                Employment.EMPLOYED_FULLTIME,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.STUDENT_DRIVING_AGE,
                id="16_17_full_time_job_recoded_to_student",
            ),
            pytest.param(
                AgeCategory.AGE_18_TO_24,
                Employment.UNEMPLOYED_NOT_LOOKING,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.NON_WORKER,
                id="18_24_not_student_not_worker",
            ),
            pytest.param(
                AgeCategory.AGE_18_TO_24,
                Employment.UNEMPLOYED_NOT_LOOKING,
                Student.FULLTIME_INPERSON,
                SchoolType.HIGH_SCHOOL,
                CTRAMPPersonType.STUDENT_DRIVING_AGE,
                id="18_24_high_school",
            ),
            pytest.param(
                AgeCategory.AGE_18_TO_24,
                Employment.UNEMPLOYED_NOT_LOOKING,
                Student.FULLTIME_INPERSON,
                SchoolType.COLLEGE_4YEAR,
                CTRAMPPersonType.UNIVERSITY_STUDENT,
                id="18_24_college",
            ),
            # Full-time employment beats full- and part-time study.
            pytest.param(
                AgeCategory.AGE_18_TO_24,
                Employment.EMPLOYED_FULLTIME,
                Student.FULLTIME_INPERSON,
                SchoolType.COLLEGE_4YEAR,
                CTRAMPPersonType.FULL_TIME_WORKER,
                id="ft_worker_ft_college_18_24",
            ),
            pytest.param(
                AgeCategory.AGE_35_TO_44,
                Employment.EMPLOYED_FULLTIME,
                Student.FULLTIME_ONLINE,
                SchoolType.COLLEGE_4YEAR,
                CTRAMPPersonType.FULL_TIME_WORKER,
                id="ft_worker_ft_college_35_44",
            ),
            pytest.param(
                AgeCategory.AGE_18_TO_24,
                Employment.EMPLOYED_FULLTIME,
                Student.PARTTIME_INPERSON,
                SchoolType.COLLEGE_4YEAR,
                CTRAMPPersonType.FULL_TIME_WORKER,
                id="ft_worker_pt_college_18_24",
            ),
            pytest.param(
                AgeCategory.AGE_35_TO_44,
                Employment.EMPLOYED_FULLTIME,
                Student.PARTTIME_ONLINE,
                SchoolType.COLLEGE_4YEAR,
                CTRAMPPersonType.FULL_TIME_WORKER,
                id="ft_worker_pt_college_35_44",
            ),
            # From 65, employment still makes a worker; otherwise retired.
            pytest.param(
                AgeCategory.AGE_65_TO_74,
                Employment.EMPLOYED_FULLTIME,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.FULL_TIME_WORKER,
                id="65_plus_full_time",
            ),
            pytest.param(
                AgeCategory.AGE_75_TO_84,
                Employment.EMPLOYED_PARTTIME,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.PART_TIME_WORKER,
                id="75_plus_part_time",
            ),
            pytest.param(
                AgeCategory.AGE_65_TO_74,
                Employment.UNEMPLOYED_NOT_LOOKING,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.RETIRED,
                id="65_plus_not_employed",
            ),
            pytest.param(
                AgeCategory.AGE_UNDER_5,
                Employment.UNEMPLOYED_NOT_LOOKING,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.CHILD_UNDER_5,
                id="under_5",
            ),
            pytest.param(
                AgeCategory.AGE_5_TO_15,
                Employment.UNEMPLOYED_NOT_LOOKING,
                Student.FULLTIME_INPERSON,
                SchoolType.ELEMENTARY,
                CTRAMPPersonType.STUDENT_NON_DRIVING_AGE,
                id="elementary_student",
            ),
            pytest.param(
                AgeCategory.AGE_35_TO_44,
                Employment.EMPLOYED_PARTTIME,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.PART_TIME_WORKER,
                id="part_time_worker",
            ),
            pytest.param(
                AgeCategory.AGE_35_TO_44,
                Employment.UNEMPLOYED_LOOKING,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.NON_WORKER,
                id="unemployed_looking",
            ),
        ],
    )
    def test_critical_person_type_cases(self, age, employment, student, school_type, expected_type):
        """Priority rules between age, employment and study."""
        assert _person_type(age, employment, student, school_type) == expected_type.value

    @given(
        age=st.sampled_from(list(AgeCategory)),
        employment=st.sampled_from([Employment.EMPLOYED_FULLTIME, Employment.EMPLOYED_SELF]),
        student=st.sampled_from(list(Student)),
        school_type=st.sampled_from(list(SchoolType)),
    )
    def test_property_fulltime_employment_precedence(self, age, employment, student, school_type):
        """From 18 up, full-time employment wins over student status.

        Below 18 the age rules win and the explicit table covers them.
        EMPLOYED_UNPAID is part-time under EMPLOYMENT_TO_CTRAMP, so it is left out.
        """
        assume(
            age not in [AgeCategory.AGE_UNDER_5, AgeCategory.AGE_5_TO_15, AgeCategory.AGE_16_TO_17]
        )

        person_type = _person_type(age, employment, student, school_type)

        assert person_type == CTRAMPPersonType.FULL_TIME_WORKER.value

    @given(
        age=st.sampled_from(ADULTS_UNDER_65[:4]),
        employment=st.sampled_from(
            [Employment.UNEMPLOYED_NOT_LOOKING, Employment.UNEMPLOYED_LOOKING]
        ),
        student=st.sampled_from(
            [Student.FULLTIME_INPERSON, Student.FULLTIME_ONLINE, Student.PARTTIME_INPERSON]
        ),
        school_type=st.sampled_from(
            [
                SchoolType.COLLEGE_2YEAR,
                SchoolType.COLLEGE_4YEAR,
                SchoolType.GRADUATE_SCHOOL,
                SchoolType.VOCATIONAL,
            ]
        ),
    )
    def test_property_college_students_are_university_type(
        self, age, employment, student, school_type
    ):
        """College students who are not employed are UNIVERSITY_STUDENT."""
        person_type = _person_type(age, employment, student, school_type)

        assert person_type == CTRAMPPersonType.UNIVERSITY_STUDENT.value

    @given(
        age=st.sampled_from(list(AgeCategory)),
        employment=st.sampled_from(list(Employment)),
        student=st.sampled_from(list(Student)),
        school_type=st.sampled_from(list(SchoolType)),
    )
    def test_property_age_person_type_consistency(self, age, employment, student, school_type):
        """Age rules out person types: children are child types, adults never are."""
        person_type = _person_type(age, employment, student, school_type)

        if age == AgeCategory.AGE_UNDER_5:
            assert person_type == CTRAMPPersonType.CHILD_UNDER_5.value

        if age == AgeCategory.AGE_5_TO_15:
            assert person_type not in [
                CTRAMPPersonType.FULL_TIME_WORKER.value,
                CTRAMPPersonType.PART_TIME_WORKER.value,
                CTRAMPPersonType.UNIVERSITY_STUDENT.value,
                CTRAMPPersonType.RETIRED.value,
                CTRAMPPersonType.CHILD_UNDER_5.value,
            ]

        if age == AgeCategory.AGE_16_TO_17:
            assert person_type not in [
                CTRAMPPersonType.CHILD_UNDER_5.value,
                CTRAMPPersonType.STUDENT_NON_DRIVING_AGE.value,
                CTRAMPPersonType.RETIRED.value,
            ]

        # 18-24 may still be a driving-age student when in high school.
        if age in ADULTS_UNDER_65 and not (
            age == AgeCategory.AGE_18_TO_24
            and person_type == CTRAMPPersonType.STUDENT_DRIVING_AGE.value
        ):
            assert person_type not in [
                CTRAMPPersonType.CHILD_UNDER_5.value,
                CTRAMPPersonType.STUDENT_NON_DRIVING_AGE.value,
                CTRAMPPersonType.RETIRED.value,
            ]
            if age != AgeCategory.AGE_18_TO_24:
                assert person_type != CTRAMPPersonType.STUDENT_DRIVING_AGE.value

        if age in SENIORS:
            assert person_type in [
                CTRAMPPersonType.RETIRED.value,
                CTRAMPPersonType.FULL_TIME_WORKER.value,
                CTRAMPPersonType.PART_TIME_WORKER.value,
                CTRAMPPersonType.UNIVERSITY_STUDENT.value,
            ]

    @given(
        age=st.sampled_from(list(AgeCategory)),
        employment=st.sampled_from(list(Employment)),
        student=st.sampled_from(list(Student)),
        school_type=st.sampled_from(list(SchoolType)),
    )
    def test_property_employment_person_type_consistency(
        self, age, employment, student, school_type
    ):
        """Worker types match their employment; child types appear only for children."""
        person_type = _person_type(age, employment, student, school_type)

        if person_type == CTRAMPPersonType.FULL_TIME_WORKER.value:
            assert employment in [Employment.EMPLOYED_FULLTIME, Employment.EMPLOYED_SELF]

        if person_type == CTRAMPPersonType.PART_TIME_WORKER.value:
            assert employment in [Employment.EMPLOYED_PARTTIME, Employment.EMPLOYED_UNPAID]

        if person_type in [
            CTRAMPPersonType.CHILD_UNDER_5.value,
            CTRAMPPersonType.STUDENT_NON_DRIVING_AGE.value,
        ]:
            assert age in [AgeCategory.AGE_UNDER_5, AgeCategory.AGE_5_TO_15]

    @given(
        age=st.sampled_from(ADULTS_UNDER_65),
        employment=st.sampled_from(list(Employment)),
        student=st.sampled_from(
            [
                Student.FULLTIME_INPERSON,
                Student.FULLTIME_ONLINE,
                Student.PARTTIME_INPERSON,
                Student.PARTTIME_ONLINE,
            ]
        ),
    )
    def test_property_student_missing_school_type_age_18_plus(self, age, employment, student):
        """Students 18-64 with no school type are UNIVERSITY_STUDENT unless working full-time."""
        person_type = _person_type(age, employment, student, SchoolType.MISSING)

        if employment not in [Employment.EMPLOYED_FULLTIME, Employment.EMPLOYED_SELF]:
            assert person_type == CTRAMPPersonType.UNIVERSITY_STUDENT.value


@pytest.mark.parametrize(
    "present",
    [
        pytest.param([], id="neither"),
        pytest.param(["employment_category"], id="student_category_missing"),
        pytest.param(["student_category"], id="employment_category_missing"),
    ],
)
def test_person_type_refuses_a_frame_without_the_categories(present):
    """Person type reads the derived categories only; a missing one raises with how to fix it."""
    df = pl.DataFrame(
        [
            {
                "person_id": 1,
                "age": AgeCategory.AGE_25_TO_34.value,
                "employment": Employment.EMPLOYED_FULLTIME.value,
                "student": Student.NONSTUDENT.value,
                "school_type": SchoolType.MISSING.value,
            }
        ]
    ).with_columns([pl.lit(0).alias(col) for col in present])

    with pytest.raises(ValueError, match="which person type is derived from"):
        enrich_persons_with_person_type(df)
