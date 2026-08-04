"""Unit tests for CT-RAMP person type classification.

Comprehensive tests using a hybrid approach:
- Critical explicit test cases for known bugs and priority rules
- Property-based tests with Hypothesis for invariant checking
"""

import polars as pl
import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

from data_canon.codebook.ctramp import CTRAMPPersonType
from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    SchoolType,
    Student,
)
from processing.formatting.ctramp.person_mappings import ctramp_person_type_expression


class TestPersonTypeClassification:
    """Comprehensive tests for CTRAMP person type classification.

    Uses a hybrid approach:
    - Critical explicit test cases for known bugs and priority rules
    - Property-based tests with Hypothesis for invariant checking
    """

    @pytest.mark.parametrize(
        ("age", "employment", "student", "school_type", "expected_type", "description"),
        [
            # === CRITICAL BUG CASES - Age 16-17 ===
            # These test the bug where 16-17 were classified as NON_WORKER/RETIRED
            (
                AgeCategory.AGE_16_TO_17,
                Employment.UNEMPLOYED_NOT_LOOKING,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.STUDENT_DRIVING_AGE,
                "BUG FIX: Age 16-17, not student/not employed → STUDENT_DRIVING_AGE",
            ),
            (
                AgeCategory.AGE_16_TO_17,
                Employment.EMPLOYED_FULLTIME,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.FULL_TIME_WORKER,
                "Age 16-17, full-time worker → employment wins",
            ),
            # === CRITICAL BUG CASES - Age 18-24 ===
            (
                AgeCategory.AGE_18_TO_24,
                Employment.UNEMPLOYED_NOT_LOOKING,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.NON_WORKER,
                "Age 18-24, not student/not worker → NON_WORKER",
            ),
            (
                AgeCategory.AGE_18_TO_24,
                Employment.UNEMPLOYED_NOT_LOOKING,
                Student.FULLTIME_INPERSON,
                SchoolType.HIGH_SCHOOL,
                CTRAMPPersonType.STUDENT_DRIVING_AGE,
                "Age 18-24, high school → STUDENT_DRIVING_AGE",
            ),
            (
                AgeCategory.AGE_18_TO_24,
                Employment.UNEMPLOYED_NOT_LOOKING,
                Student.FULLTIME_INPERSON,
                SchoolType.COLLEGE_4YEAR,
                CTRAMPPersonType.UNIVERSITY_STUDENT,
                "Age 18-24, college → UNIVERSITY_STUDENT",
            ),
            # === PRIORITY RULES - Employment vs Student ===
            # Full-time employment beats full-time student status
            (
                AgeCategory.AGE_18_TO_24,
                Employment.EMPLOYED_FULLTIME,
                Student.FULLTIME_INPERSON,
                SchoolType.COLLEGE_4YEAR,
                CTRAMPPersonType.FULL_TIME_WORKER,
                "FT worker + FT college student → employment wins",
            ),
            (
                AgeCategory.AGE_35_TO_44,
                Employment.EMPLOYED_FULLTIME,
                Student.FULLTIME_ONLINE,
                SchoolType.COLLEGE_4YEAR,
                CTRAMPPersonType.FULL_TIME_WORKER,
                "Working adult + FT college → employment wins",
            ),
            # Full-time employment beats part-time student
            (
                AgeCategory.AGE_18_TO_24,
                Employment.EMPLOYED_FULLTIME,
                Student.PARTTIME_INPERSON,
                SchoolType.COLLEGE_4YEAR,
                CTRAMPPersonType.FULL_TIME_WORKER,
                "FT worker + PT college student → employment wins",
            ),
            (
                AgeCategory.AGE_35_TO_44,
                Employment.EMPLOYED_FULLTIME,
                Student.PARTTIME_ONLINE,
                SchoolType.COLLEGE_4YEAR,
                CTRAMPPersonType.FULL_TIME_WORKER,
                "FT worker + PT college student → employment wins",
            ),
            # === PRIORITY RULES - Age 65+ employed → worker types ===
            (
                AgeCategory.AGE_65_TO_74,
                Employment.EMPLOYED_FULLTIME,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.FULL_TIME_WORKER,
                "Age 65+, working FT → FULL_TIME_WORKER (employment wins)",
            ),
            (
                AgeCategory.AGE_75_TO_84,
                Employment.EMPLOYED_PARTTIME,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.PART_TIME_WORKER,
                "Age 75+, working PT → PART_TIME_WORKER (employment wins)",
            ),
            # 65+ non-employed → RETIRED
            (
                AgeCategory.AGE_65_TO_74,
                Employment.UNEMPLOYED_NOT_LOOKING,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.RETIRED,
                "Age 65+, not employed → RETIRED",
            ),
            # === REPRESENTATIVE CASES - Each person type ===
            (
                AgeCategory.AGE_UNDER_5,
                Employment.UNEMPLOYED_NOT_LOOKING,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.CHILD_UNDER_5,
                "Under 5",
            ),
            (
                AgeCategory.AGE_5_TO_15,
                Employment.UNEMPLOYED_NOT_LOOKING,
                Student.FULLTIME_INPERSON,
                SchoolType.ELEMENTARY,
                CTRAMPPersonType.STUDENT_NON_DRIVING_AGE,
                "Elementary student",
            ),
            (
                AgeCategory.AGE_35_TO_44,
                Employment.EMPLOYED_PARTTIME,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.PART_TIME_WORKER,
                "Part-time worker",
            ),
            (
                AgeCategory.AGE_35_TO_44,
                Employment.UNEMPLOYED_LOOKING,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.NON_WORKER,
                "Unemployed looking",
            ),
        ],
    )
    def test_critical_person_type_cases(
        self, age, employment, student, school_type, expected_type, description
    ):
        """Test critical edge cases and known bugs with explicit test cases."""
        df = pl.DataFrame(
            [
                {
                    "age": age.value,
                    "employment": employment.value,
                    "student": student.value,
                    "school_type": school_type.value,
                }
            ]
        )

        result = df.with_columns(ctramp_person_type_expression())
        person_type = result["literal"][0]

        assert person_type == expected_type.value, (
            f"Failed: {description}\n"
            f"  Age: {age.name} ({age.value})\n"
            f"  Employment: {employment.name}\n"
            f"  Student: {student.name}\n"
            f"  School Type: {school_type.name}\n"
            f"  Expected: {expected_type.name} ({expected_type.value})\n"
            f"  Got: {person_type}"
        )

    # === PARAMETRIZED TESTS FOR WORKING COLLEGE STUDENTS ===

    @pytest.mark.parametrize("age", [18, 22, 24, 30, 45])
    @pytest.mark.parametrize(
        "school_type",
        [
            SchoolType.COLLEGE_2YEAR,
            SchoolType.COLLEGE_4YEAR,
            SchoolType.VOCATIONAL,
            SchoolType.GRADUATE_SCHOOL,
        ],
    )
    def test_parttime_worker_college_student_is_university(self, age, school_type):
        """Part-time workers who are college students should be classified as UNIVERSITY_STUDENT.

        This test validates that student status takes precedence over part-time employment
        when the student is in college. Tests across multiple ages (18-45) and all college
        school types to ensure comprehensive coverage.
        """
        # Map age to AgeCategory
        if age <= 17:
            age_cat = AgeCategory.AGE_16_TO_17
        elif age <= 24:
            age_cat = AgeCategory.AGE_18_TO_24
        elif age <= 34:
            age_cat = AgeCategory.AGE_25_TO_34
        elif age <= 44:
            age_cat = AgeCategory.AGE_35_TO_44
        elif age <= 54:
            age_cat = AgeCategory.AGE_45_TO_54
        else:
            age_cat = AgeCategory.AGE_55_TO_64

        df = pl.DataFrame(
            [
                {
                    "age": age_cat.value,
                    "employment": Employment.EMPLOYED_PARTTIME.value,
                    "student": Student.FULLTIME_INPERSON.value,
                    "school_type": school_type.value,
                }
            ]
        )

        result = df.with_columns(ctramp_person_type_expression())
        person_type = result["literal"][0]

        assert person_type == CTRAMPPersonType.UNIVERSITY_STUDENT.value, (
            f"Part-time worker (age {age}) who is a college student "
            f"({school_type.name}) should be UNIVERSITY_STUDENT, got {person_type}"
        )

    @pytest.mark.parametrize("age", [18, 20, 22, 24])
    def test_student_18_24_missing_school_type_is_university(self, age):
        """Students age 18-24 with MISSING school_type should be classified as UNIVERSITY_STUDENT.

        When a young adult is marked as a student but school_type is MISSING,
        we assume they are university students rather than letting them fall through
        to STUDENT_DRIVING_AGE catch-all.
        """
        age_cat = AgeCategory.AGE_18_TO_24

        df = pl.DataFrame(
            [
                {
                    "age": age_cat.value,
                    "employment": Employment.UNEMPLOYED_NOT_LOOKING.value,
                    "student": Student.FULLTIME_INPERSON.value,
                    "school_type": SchoolType.MISSING.value,
                }
            ]
        )

        result = df.with_columns(ctramp_person_type_expression())
        person_type = result["literal"][0]

        assert person_type == CTRAMPPersonType.UNIVERSITY_STUDENT.value, (
            f"Student age {age} with MISSING school_type "
            f"should be UNIVERSITY_STUDENT, got {person_type}"
        )

    @pytest.mark.parametrize("age", [30, 35, 45, 55])
    def test_student_working_age_missing_school_type_is_university(self, age):
        """Students of working age with MISSING school_type should be classified as UNIVERSITY_STUDENT.

        When a working-age adult is marked as a student but school_type is MISSING,
        we assume they are university students rather than letting them fall through
        to NON_WORKER catch-all.
        """  # noqa: E501
        # Map age to AgeCategory
        if age <= 34:
            age_cat = AgeCategory.AGE_25_TO_34
        elif age <= 44:
            age_cat = AgeCategory.AGE_35_TO_44
        elif age <= 54:
            age_cat = AgeCategory.AGE_45_TO_54
        else:
            age_cat = AgeCategory.AGE_55_TO_64

        df = pl.DataFrame(
            [
                {
                    "age": age_cat.value,
                    "employment": Employment.UNEMPLOYED_NOT_LOOKING.value,
                    "student": Student.PARTTIME_INPERSON.value,
                    "school_type": SchoolType.MISSING.value,
                }
            ]
        )

        result = df.with_columns(ctramp_person_type_expression())
        person_type = result["literal"][0]

        assert person_type == CTRAMPPersonType.UNIVERSITY_STUDENT.value, (
            f"Student age {age} with MISSING school_type "
            f"should be UNIVERSITY_STUDENT, got {person_type}"
        )

    # === PROPERTY-BASED TESTS WITH HYPOTHESIS ===

    @given(
        age=st.just(AgeCategory.AGE_UNDER_5),
        employment=st.sampled_from(list(Employment)),
        student=st.sampled_from(list(Student)),
        school_type=st.sampled_from(list(SchoolType)),
    )
    def test_property_age_under_5_always_child_under_5(self, age, employment, student, school_type):
        """Property: Anyone under 5 must always be classified as CHILD_UNDER_5."""
        df = pl.DataFrame(
            [
                {
                    "age": age.value,
                    "employment": employment.value,
                    "student": student.value,
                    "school_type": school_type.value,
                }
            ]
        )

        result = df.with_columns(ctramp_person_type_expression())
        person_type = result["literal"][0]

        assert person_type == CTRAMPPersonType.CHILD_UNDER_5.value, (
            f"Age under 5 must be CHILD_UNDER_5, got {person_type}"
        )

    @given(
        age=st.sampled_from(
            [AgeCategory.AGE_65_TO_74, AgeCategory.AGE_75_TO_84, AgeCategory.AGE_85_AND_UP]
        ),
        employment=st.sampled_from(list(Employment)),
        student=st.sampled_from(list(Student)),
        school_type=st.sampled_from(list(SchoolType)),
    )
    def test_property_age_65_plus_classification(self, age, employment, student, school_type):
        """Property: 65+ defaults to RETIRED, but employment overrides to worker types."""
        df = pl.DataFrame(
            [
                {
                    "age": age.value,
                    "employment": employment.value,
                    "student": student.value,
                    "school_type": school_type.value,
                }
            ]
        )

        result = df.with_columns(ctramp_person_type_expression())
        person_type = result["literal"][0]

        is_ft_employed = employment in [
            Employment.EMPLOYED_FULLTIME,
            Employment.EMPLOYED_SELF,
        ]
        is_pt_employed = employment in [
            Employment.EMPLOYED_PARTTIME,
            Employment.EMPLOYED_UNPAID,
        ]
        is_college = school_type in [
            SchoolType.COLLEGE_2YEAR,
            SchoolType.COLLEGE_4YEAR,
            SchoolType.GRADUATE_SCHOOL,
            SchoolType.VOCATIONAL,
        ]
        is_student = student in [
            Student.FULLTIME_INPERSON,
            Student.FULLTIME_ONLINE,
            Student.PARTTIME_INPERSON,
            Student.PARTTIME_ONLINE,
        ]

        if is_ft_employed:
            assert person_type == CTRAMPPersonType.FULL_TIME_WORKER.value, (
                f"Age 65+ with FT employment must be FULL_TIME_WORKER, got {person_type}"
            )
        elif is_pt_employed and is_student and is_college:
            assert person_type == CTRAMPPersonType.UNIVERSITY_STUDENT.value, (
                f"Age 65+ PT employed college student must be UNIVERSITY_STUDENT, got {person_type}"
            )
        elif is_student and is_college:
            assert person_type == CTRAMPPersonType.UNIVERSITY_STUDENT.value, (
                f"Age 65+ college student must be UNIVERSITY_STUDENT, got {person_type}"
            )
        elif is_pt_employed:
            assert person_type == CTRAMPPersonType.PART_TIME_WORKER.value, (
                f"Age 65+ with PT employment must be PART_TIME_WORKER, got {person_type}"
            )
        else:
            assert person_type == CTRAMPPersonType.RETIRED.value, (
                f"Age 65+ without employment must be RETIRED, got {person_type}"
            )

    @given(
        age=st.sampled_from([AgeCategory.AGE_16_TO_17, AgeCategory.AGE_18_TO_24]),
        employment=st.sampled_from(
            [Employment.UNEMPLOYED_NOT_LOOKING, Employment.UNEMPLOYED_LOOKING]
        ),
        student=st.just(Student.NONSTUDENT),
        school_type=st.just(SchoolType.MISSING),
    )
    def test_property_youth_non_employed_non_student_default(
        self, age, employment, student, school_type
    ):
        """Property: Non-employed non-students default by age.

        - 16-17 → STUDENT_DRIVING_AGE
        - 18-24 → NON_WORKER
        """
        df = pl.DataFrame(
            [
                {
                    "age": age.value,
                    "employment": employment.value,
                    "student": student.value,
                    "school_type": school_type.value,
                }
            ]
        )

        result = df.with_columns(ctramp_person_type_expression())
        person_type = result["literal"][0]

        if age == AgeCategory.AGE_16_TO_17:
            assert person_type == CTRAMPPersonType.STUDENT_DRIVING_AGE.value, (
                f"Age 16-17, unemployed, non-student must be STUDENT_DRIVING_AGE, got {person_type}"
            )
        else:
            assert person_type == CTRAMPPersonType.NON_WORKER.value, (
                f"Age 18-24, unemployed, non-student must be NON_WORKER, got {person_type}"
            )

    @given(
        age=st.sampled_from(list(AgeCategory)),
        employment=st.sampled_from(
            [Employment.EMPLOYED_FULLTIME, Employment.EMPLOYED_SELF, Employment.EMPLOYED_UNPAID]
        ),
        student=st.sampled_from(list(Student)),
        school_type=st.sampled_from(list(SchoolType)),
    )
    def test_property_fulltime_employment_precedence(self, age, employment, student, school_type):
        """Property: Full-time employment always wins over student status.

        Exceptions:
        - Age < 5 → CHILD_UNDER_5
        - Age 5-15 → STUDENT_NON_DRIVING_AGE
        - Age 16-17 grade/high school students → STUDENT_DRIVING_AGE
        - EMPLOYED_UNPAID is treated as part-time
        """
        # Skip if age forces different classification
        assume(
            age
            not in [
                AgeCategory.AGE_UNDER_5,
                AgeCategory.AGE_5_TO_15,
            ]
        )
        # EMPLOYED_UNPAID is treated as part-time, not full-time
        assume(employment != Employment.EMPLOYED_UNPAID)

        df = pl.DataFrame(
            [
                {
                    "age": age.value,
                    "employment": employment.value,
                    "student": student.value,
                    "school_type": school_type.value,
                }
            ]
        )

        result = df.with_columns(ctramp_person_type_expression())
        person_type = result["literal"][0]

        # Exception: 16-17 grade/high school students are STUDENT_DRIVING_AGE
        is_high_school_type = school_type in [
            SchoolType.HIGH_SCHOOL,
            SchoolType.HOME_SCHOOL,
        ]
        is_student_status = student in [
            Student.FULLTIME_INPERSON,
            Student.FULLTIME_ONLINE,
            Student.PARTTIME_INPERSON,
            Student.PARTTIME_ONLINE,
        ]

        if age == AgeCategory.AGE_16_TO_17 and is_high_school_type and is_student_status:
            assert person_type == CTRAMPPersonType.STUDENT_DRIVING_AGE.value, (
                f"16-17 grade/HS student must be STUDENT_DRIVING_AGE "
                f"regardless of employment, got {person_type}"
            )
        else:
            assert person_type == CTRAMPPersonType.FULL_TIME_WORKER.value, (
                f"Full-time employed person (age {age.name}) "
                f"should be FULL_TIME_WORKER, got {person_type}"
            )

    @given(
        age=st.sampled_from(list(AgeCategory)),
        employment=st.sampled_from(list(Employment)),
        student=st.sampled_from(list(Student)),
        school_type=st.sampled_from(list(SchoolType)),
    )
    def test_property_all_persons_classified(self, age, employment, student, school_type):
        """Property: Every valid combination must be classified into one of the 8 person types."""
        df = pl.DataFrame(
            [
                {
                    "age": age.value,
                    "employment": employment.value,
                    "student": student.value,
                    "school_type": school_type.value,
                }
            ]
        )

        result = df.with_columns(ctramp_person_type_expression())
        person_type = result["literal"][0]

        valid_types = {t.value for t in CTRAMPPersonType}
        assert person_type in valid_types, (
            f"Person type {person_type} is not a valid CTRAMPPersonType"
        )

    @given(
        age=st.sampled_from(
            [
                AgeCategory.AGE_18_TO_24,
                AgeCategory.AGE_25_TO_34,
                AgeCategory.AGE_35_TO_44,
                AgeCategory.AGE_45_TO_54,
            ]
        ),
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
        """Property: College students (not employed full-time) should be UNIVERSITY_STUDENT."""
        df = pl.DataFrame(
            [
                {
                    "age": age.value,
                    "employment": employment.value,
                    "student": student.value,
                    "school_type": school_type.value,
                }
            ]
        )

        result = df.with_columns(ctramp_person_type_expression())
        person_type = result["literal"][0]

        assert person_type == CTRAMPPersonType.UNIVERSITY_STUDENT.value, (
            f"College student (age {age.name}, {school_type.name}) "
            f"should be UNIVERSITY_STUDENT, got {person_type}"
        )

    @given(
        age=st.sampled_from(list(AgeCategory)),
        employment=st.sampled_from(list(Employment)),
        student=st.sampled_from(list(Student)),
        school_type=st.sampled_from(list(SchoolType)),
    )
    def test_property_age_person_type_consistency(self, age, employment, student, school_type):
        """Property: Age determines valid person types - certain age/type combinations are impossible.

        Age-based constraints:
        - Age < 5 must be CHILD_UNDER_5
        - Age 5-15 cannot be FULL_TIME_WORKER, PART_TIME_WORKER, UNIVERSITY_STUDENT, or RETIRED
        - Age 16-17 cannot be CHILD_UNDER_5, CHILD_NON_DRIVING_AGE, or RETIRED
        - Age 18-64 cannot be CHILD_UNDER_5, CHILD_NON_DRIVING_AGE, or RETIRED
        - Age 65+ must be RETIRED, FT/PT_WORKER, or UNIVERSITY_STUDENT
        """  # noqa: E501
        df = pl.DataFrame(
            [
                {
                    "age": age.value,
                    "employment": employment.value,
                    "student": student.value,
                    "school_type": school_type.value,
                }
            ]
        )

        result = df.with_columns(ctramp_person_type_expression())
        person_type = result["literal"][0]

        # Age < 5: Must be CHILD_UNDER_5
        if age == AgeCategory.AGE_UNDER_5:
            assert person_type == CTRAMPPersonType.CHILD_UNDER_5.value, (
                f"Age < 5 must be CHILD_UNDER_5, got {person_type}"
            )

        # Age 5-15: Cannot be adult worker types, university, or retired
        if age == AgeCategory.AGE_5_TO_15:
            assert person_type not in [
                CTRAMPPersonType.FULL_TIME_WORKER.value,
                CTRAMPPersonType.PART_TIME_WORKER.value,
                CTRAMPPersonType.UNIVERSITY_STUDENT.value,
                CTRAMPPersonType.RETIRED.value,
                CTRAMPPersonType.CHILD_UNDER_5.value,
            ], f"Age 5-15 cannot be FT/PT worker, university student, or retired, got {person_type}"

        # Age 16-17: Cannot be young children or retired
        if age == AgeCategory.AGE_16_TO_17:
            assert person_type not in [
                CTRAMPPersonType.CHILD_UNDER_5.value,
                CTRAMPPersonType.STUDENT_NON_DRIVING_AGE.value,
                CTRAMPPersonType.RETIRED.value,
            ], f"Age 16-17 cannot be young children or retired, got {person_type}"

        # Age 18-64: Cannot be any child type or retired
        if age in [
            AgeCategory.AGE_18_TO_24,
            AgeCategory.AGE_25_TO_34,
            AgeCategory.AGE_35_TO_44,
            AgeCategory.AGE_45_TO_54,
            AgeCategory.AGE_55_TO_64,
        ]:
            # Exception: 18-24 can be STU_DRIVING_AGE if they're in high school
            if (
                age == AgeCategory.AGE_18_TO_24
                and person_type == CTRAMPPersonType.STUDENT_DRIVING_AGE.value
            ):
                # This is allowed for high school students
                pass
            else:
                assert person_type not in [
                    CTRAMPPersonType.CHILD_UNDER_5.value,
                    CTRAMPPersonType.STUDENT_NON_DRIVING_AGE.value,
                    CTRAMPPersonType.RETIRED.value,
                ], f"Age {age.name} cannot be young children or retired, got {person_type}"

                if age != AgeCategory.AGE_18_TO_24:
                    assert person_type != CTRAMPPersonType.STUDENT_DRIVING_AGE.value, (
                        f"Age {age.name} cannot be STUDENT_DRIVING_AGE, got {person_type}"
                    )

        # Age 65+: RETIRED unless employed (then worker types)
        if age in [AgeCategory.AGE_65_TO_74, AgeCategory.AGE_75_TO_84, AgeCategory.AGE_85_AND_UP]:
            valid_senior_types = [
                CTRAMPPersonType.RETIRED.value,
                CTRAMPPersonType.FULL_TIME_WORKER.value,
                CTRAMPPersonType.PART_TIME_WORKER.value,
                CTRAMPPersonType.UNIVERSITY_STUDENT.value,
            ]
            assert person_type in valid_senior_types, (
                f"Age 65+ must be RETIRED, FT/PT_WORKER, or UNIVERSITY_STUDENT, got {person_type}"
            )

    @given(
        age=st.sampled_from(list(AgeCategory)),
        employment=st.sampled_from(list(Employment)),
        student=st.sampled_from(list(Student)),
        school_type=st.sampled_from(list(SchoolType)),
    )
    def test_property_employment_person_type_consistency(
        self, age, employment, student, school_type
    ):
        """Property: Employment status and person type must be consistent.

        Employment-based constraints:
        - FULL_TIME_WORKER must have full-time employment (unless age overrides)
        - PART_TIME_WORKER must have part-time employment (unless age overrides)
        - Child types generally shouldn't have full-time employment
        """
        df = pl.DataFrame(
            [
                {
                    "age": age.value,
                    "employment": employment.value,
                    "student": student.value,
                    "school_type": school_type.value,
                }
            ]
        )

        result = df.with_columns(ctramp_person_type_expression())
        person_type = result["literal"][0]

        # If classified as FULL_TIME_WORKER, must have full-time employment
        # (EMPLOYED_UNPAID is now treated as part-time)
        if person_type == CTRAMPPersonType.FULL_TIME_WORKER.value:
            assert employment in [
                Employment.EMPLOYED_FULLTIME,
                Employment.EMPLOYED_SELF,
            ], f"FULL_TIME_WORKER must have full-time employment, got {employment.name}"

        # If classified as PART_TIME_WORKER, must have part-time employment
        # (EMPLOYED_UNPAID is treated as part-time)
        if person_type == CTRAMPPersonType.PART_TIME_WORKER.value:
            assert employment in [
                Employment.EMPLOYED_PARTTIME,
                Employment.EMPLOYED_UNPAID,
            ], f"PART_TIME_WORKER must have part-time employment, got {employment.name}"

        # Young children shouldn't have full-time employment
        # (if they do, age-based rules should override)
        if person_type in [
            CTRAMPPersonType.CHILD_UNDER_5.value,
            CTRAMPPersonType.STUDENT_NON_DRIVING_AGE.value,
        ]:
            # These types should never have been workers - age overrides employment
            assert age in [AgeCategory.AGE_UNDER_5, AgeCategory.AGE_5_TO_15], (
                f"Child types should only appear for young ages, got age {age.name}"
            )

    @given(
        age=st.sampled_from(list(AgeCategory)),
        employment=st.sampled_from(list(Employment)),
        student=st.sampled_from(list(Student)),
        school_type=st.sampled_from(list(SchoolType)),
    )
    def test_property_student_person_type_consistency(self, age, employment, student, school_type):
        """Property: Student status and person type must be consistent.

        Student-based constraints:
        - UNIVERSITY_STUDENT must be a college student
        - College students cannot be CHILD_UNDER_5 or CHILD_NON_DRIVING_AGE
        - High school students should be appropriate child types
        """
        df = pl.DataFrame(
            [
                {
                    "age": age.value,
                    "employment": employment.value,
                    "student": student.value,
                    "school_type": school_type.value,
                }
            ]
        )

        result = df.with_columns(ctramp_person_type_expression())
        person_type = result["literal"][0]

        # If classified as UNIVERSITY_STUDENT, must be a college student with valid school_type
        if person_type == CTRAMPPersonType.UNIVERSITY_STUDENT.value:
            # Must be a student
            assert student in [
                Student.FULLTIME_INPERSON,
                Student.FULLTIME_ONLINE,
                Student.PARTTIME_INPERSON,
                Student.PARTTIME_ONLINE,
            ], f"UNIVERSITY_STUDENT must be a student, got {student.name}"

            # Must be in college OR have MISSING school_type (if age 18+)
            if school_type not in [
                SchoolType.COLLEGE_2YEAR,
                SchoolType.COLLEGE_4YEAR,
                SchoolType.GRADUATE_SCHOOL,
                SchoolType.VOCATIONAL,
                SchoolType.MISSING,  # Now allowed for 18+
            ]:
                msg = (
                    f"UNIVERSITY_STUDENT must be in college/vocational or "
                    f"have MISSING school_type, got {school_type.name}"
                )
                raise AssertionError(msg)

        # Part-time workers who are college students should be UNIVERSITY_STUDENT
        # (unless age 65+ which overrides to RETIRED, or young children)
        if (
            employment == Employment.EMPLOYED_PARTTIME
            and student
            in [
                Student.FULLTIME_INPERSON,
                Student.FULLTIME_ONLINE,
                Student.PARTTIME_INPERSON,
                Student.PARTTIME_ONLINE,
            ]
            and school_type
            in [
                SchoolType.COLLEGE_2YEAR,
                SchoolType.COLLEGE_4YEAR,
                SchoolType.GRADUATE_SCHOOL,
                SchoolType.VOCATIONAL,
            ]
            and age
            not in [
                AgeCategory.AGE_UNDER_5,
                AgeCategory.AGE_5_TO_15,
                AgeCategory.AGE_16_TO_17,
                AgeCategory.AGE_65_TO_74,
                AgeCategory.AGE_75_TO_84,
                AgeCategory.AGE_85_AND_UP,
            ]
        ):
            assert person_type == CTRAMPPersonType.UNIVERSITY_STUDENT.value, (
                f"Part-time worker who is a college student (age {age.name}) "
                f"should be UNIVERSITY_STUDENT, got person_type={person_type}"
            )

        # College students cannot be very young child types (age overrides)
        if school_type in [
            SchoolType.COLLEGE_2YEAR,
            SchoolType.COLLEGE_4YEAR,
        ] and person_type in [
            CTRAMPPersonType.CHILD_UNDER_5.value,
            CTRAMPPersonType.STUDENT_NON_DRIVING_AGE.value,
        ]:
            # This means age overrode the impossible school type
            assert age in [AgeCategory.AGE_UNDER_5, AgeCategory.AGE_5_TO_15], (
                f"College student classified as young child must be young age, got {age.name}"
            )

    @given(
        age=st.sampled_from(
            [
                AgeCategory.AGE_18_TO_24,
                AgeCategory.AGE_25_TO_34,
                AgeCategory.AGE_35_TO_44,
                AgeCategory.AGE_45_TO_54,
                AgeCategory.AGE_55_TO_64,
            ]
        ),
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
        """Property: Students age 18+ with MISSING school_type should be UNIVERSITY_STUDENT.

        This validates that students with ambiguous/missing school type data
        are classified as university students rather than falling through to
        age-based catch-alls (STUDENT_DRIVING_AGE for 18-24, NON_WORKER for 25+).
        """
        df = pl.DataFrame(
            [
                {
                    "age": age.value,
                    "employment": employment.value,
                    "student": student.value,
                    "school_type": SchoolType.MISSING.value,
                }
            ]
        )

        result = df.with_columns(ctramp_person_type_expression())
        person_type = result["literal"][0]

        # Exception: Full-time employment overrides student status
        # (EMPLOYED_UNPAID is now part-time, so it doesn't override)
        if employment not in [
            Employment.EMPLOYED_FULLTIME,
            Employment.EMPLOYED_SELF,
        ]:
            assert person_type == CTRAMPPersonType.UNIVERSITY_STUDENT.value, (
                f"Student age {age.name} with MISSING school_type "
                f"should be UNIVERSITY_STUDENT (got {person_type}), "
                f"unless full-time employed"
            )

    def test_batch_classification_consistency(self):
        """Test that batch classification produces same results as individual classification."""
        test_cases = [
            {
                "age": AgeCategory.AGE_16_TO_17.value,
                "employment": Employment.UNEMPLOYED_NOT_LOOKING.value,
                "student": Student.FULLTIME_INPERSON.value,
                "school_type": SchoolType.HIGH_SCHOOL.value,
                "expected": CTRAMPPersonType.STUDENT_DRIVING_AGE.value,
            },
            {
                "age": AgeCategory.AGE_35_TO_44.value,
                "employment": Employment.EMPLOYED_FULLTIME.value,
                "student": Student.NONSTUDENT.value,
                "school_type": SchoolType.MISSING.value,
                "expected": CTRAMPPersonType.FULL_TIME_WORKER.value,
            },
            {
                "age": AgeCategory.AGE_5_TO_15.value,
                "employment": Employment.UNEMPLOYED_NOT_LOOKING.value,
                "student": Student.FULLTIME_INPERSON.value,
                "school_type": SchoolType.ELEMENTARY.value,
                "expected": CTRAMPPersonType.STUDENT_NON_DRIVING_AGE.value,
            },
            {
                "age": AgeCategory.AGE_65_TO_74.value,
                "employment": Employment.UNEMPLOYED_NOT_LOOKING.value,
                "student": Student.NONSTUDENT.value,
                "school_type": SchoolType.MISSING.value,
                "expected": CTRAMPPersonType.RETIRED.value,
            },
        ]

        df = pl.DataFrame(test_cases)
        result = df.with_columns(ctramp_person_type_expression())

        for i, row in enumerate(result.iter_rows(named=True)):
            assert row["literal"] == row["expected"], (
                f"Row {i} failed batch classification:  "
                f"expected {row['expected']}, got {row['literal']}"
            )
