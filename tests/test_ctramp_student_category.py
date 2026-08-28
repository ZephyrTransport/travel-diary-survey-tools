"""Unit tests for CT-RAMP student category classification.

Tests the three-tier logic for deriving student_category:
1. Valid student status + valid school type → map by school level
2. Missing data → age-based fallback for children 5-17
3. Catch-all → NOT_STUDENT

Note: These tests use AgeCategory enum values (e.g., AGE_5_TO_15=2), not continuous
ages, since format_persons.py computes student_category BEFORE converting age to continuous.
"""

import polars as pl
import pytest

from data_canon.codebook.ctramp import CTRAMPStudentCategory
from data_canon.codebook.persons import AgeCategory, Employment, SchoolType, Student
from processing.formatting.ctramp.ctramp_config import CTRAMPConfig
from processing.formatting.ctramp.person_mappings import ctramp_person_type_expression
from processing.formatting.ctramp.student_mappings import (
    ctramp_student_category_expression,
    log_student_category_warnings,
)


@pytest.fixture
def standard_config():
    """Standard test configuration."""
    return CTRAMPConfig(
        usability_flag_col="usable",
        income_low_threshold=30000,
        income_med_threshold=60000,
        income_high_threshold=100000,
        income_survey_year_to_ctramp_year=0.5319148936,
    )


class TestStudentCategoryClassification:
    """Tests for CTRAMP student category derivation with age-based fallbacks."""

    @pytest.mark.parametrize(
        ("school_type", "expected_category"),
        [
            (SchoolType.ELEMENTARY, CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL),
            (SchoolType.MIDDLE_SCHOOL, CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL),
            (SchoolType.HIGH_SCHOOL, CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL),
            (SchoolType.HOME_SCHOOL, CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL),
            (SchoolType.COLLEGE_2YEAR, CTRAMPStudentCategory.COLLEGE_OR_HIGHER),
            (SchoolType.COLLEGE_4YEAR, CTRAMPStudentCategory.COLLEGE_OR_HIGHER),
            (SchoolType.GRADUATE_SCHOOL, CTRAMPStudentCategory.COLLEGE_OR_HIGHER),
            (SchoolType.VOCATIONAL, CTRAMPStudentCategory.COLLEGE_OR_HIGHER),
            (SchoolType.DAYCARE, CTRAMPStudentCategory.NOT_STUDENT),
            (SchoolType.PRESCHOOL, CTRAMPStudentCategory.NOT_STUDENT),
            (SchoolType.ATHOME, CTRAMPStudentCategory.NOT_STUDENT),
        ],
    )
    def test_valid_student_and_school_type(
        self, school_type: SchoolType, expected_category: CTRAMPStudentCategory
    ) -> None:
        """Test Tier 1: Valid student status + valid school type."""
        df = pl.DataFrame(
            {
                "person_id": [1],
                "age": [AgeCategory.AGE_18_TO_24.value],
                "student": [Student.FULLTIME_INPERSON.value],
                "school_type": [school_type.value],
                "school_taz": [0],
            }
        )

        result = df.with_columns(ctramp_student_category_expression().alias("student_category"))

        assert result["student_category"][0] == expected_category.value

    @pytest.mark.parametrize(
        ("age", "expected_category", "description"),
        [
            # BUG FIX: Children 5-17 should default to GRADE_OR_HIGH_SCHOOL
            (
                AgeCategory.AGE_5_TO_15,
                CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL,
                "Age 5-15 missing data → GRADE_OR_HIGH_SCHOOL",
            ),
            (
                AgeCategory.AGE_16_TO_17,
                CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL,
                "Age 16-17 missing data → GRADE_OR_HIGH_SCHOOL",
            ),
            # Adults and young children default to NOT_STUDENT
            (AgeCategory.AGE_UNDER_5, CTRAMPStudentCategory.NOT_STUDENT, "Under 5 → NOT_STUDENT"),
            (
                AgeCategory.AGE_18_TO_24,
                CTRAMPStudentCategory.NOT_STUDENT,
                "Age 18+ missing data → NOT_STUDENT",
            ),
            (
                AgeCategory.AGE_65_TO_74,
                CTRAMPStudentCategory.NOT_STUDENT,
                "Age 65+ missing data → NOT_STUDENT",
            ),
        ],
    )
    def test_missing_data_age_fallback(
        self, age: AgeCategory, expected_category: CTRAMPStudentCategory, description: str
    ) -> None:
        """Test Tier 2: Age-based fallback when student/school_type is missing."""
        df = pl.DataFrame(
            {
                "person_id": [1],
                "age": [age.value],
                "student": [Student.MISSING.value],
                "school_type": [SchoolType.MISSING.value],
                "school_taz": [0],
            }
        )

        result = df.with_columns(ctramp_student_category_expression().alias("student_category"))

        assert result["student_category"][0] == expected_category.value, description

    def test_missing_triggers_fallback_for_either_field(self) -> None:
        """Test that missing student OR school_type triggers age fallback."""
        # Child with missing student, valid school_type
        df1 = pl.DataFrame(
            {
                "age": [AgeCategory.AGE_5_TO_15.value],
                "student": [Student.MISSING.value],
                "school_type": [SchoolType.ELEMENTARY.value],
                "school_taz": [0],
            }
        )
        # Child with valid student, missing school_type
        df2 = pl.DataFrame(
            {
                "age": [AgeCategory.AGE_16_TO_17.value],
                "student": [Student.FULLTIME_INPERSON.value],
                "school_type": [SchoolType.MISSING.value],
                "school_taz": [0],
            }
        )

        result1 = df1.with_columns(ctramp_student_category_expression().alias("student_category"))
        result2 = df2.with_columns(ctramp_student_category_expression().alias("student_category"))

        assert result1["student_category"][0] == CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value
        assert result2["student_category"][0] == CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value

    def test_regression_child_with_missing_student_status(self) -> None:
        """REGRESSION TEST: Child age 10 with student=MISSING should not be NOT_STUDENT.

        This was the original bug report: children with missing student status were
        incorrectly classified as NOT_STUDENT even when they had valid school_type.
        """
        # Exact scenario from bug report:
        # age 10, student=MISSING (995),
        # school_type=MIDDLE_SCHOOL (6)
        df = pl.DataFrame(
            {
                "person_id": [2300104902],
                "age": [AgeCategory.AGE_5_TO_15.value],  # Age bin for 5-15 (includes age 10)
                "student": [Student.MISSING.value],  # 995
                "school_type": [SchoolType.MIDDLE_SCHOOL.value],  # 6
                "school_taz": [0],
            }
        )

        result = df.with_columns(ctramp_student_category_expression().alias("student_category"))

        # BUG FIX: Should be GRADE_OR_HIGH_SCHOOL, not NOT_STUDENT
        assert result["student_category"][0] == CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value
        assert result["student_category"][0] != CTRAMPStudentCategory.NOT_STUDENT.value

    def test_regression_child_marked_nonstudent_with_missing_school(self) -> None:
        """Child marked as NONSTUDENT with missing school_type should use age fallback.

        Children age 5-17 marked as NONSTUDENT but with missing school_type data
        should default to GRADE_OR_HIGH_SCHOOL based on age (compulsory education).
        """
        df = pl.DataFrame(
            {
                "person_id": [1, 2],
                "age": [AgeCategory.AGE_5_TO_15.value, AgeCategory.AGE_16_TO_17.value],
                "student": [Student.NONSTUDENT.value, Student.NONSTUDENT.value],
                "school_type": [SchoolType.MISSING.value, SchoolType.MISSING.value],
                "school_taz": [0, 0],
            }
        )

        result = df.with_columns(ctramp_student_category_expression().alias("student_category"))

        # Both should get age-based fallback to GRADE_OR_HIGH_SCHOOL
        assert result["student_category"][0] == CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value
        assert result["student_category"][1] == CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value

    def test_various_missing_combinations(self) -> None:
        """Various combinations of missing data for school-age children.

        Tests multiple real-world scenarios where data quality issues could occur.
        All should default to GRADE_OR_HIGH_SCHOOL for ages 5-17.
        """
        test_cases = [
            # (age, student, school_type, description)
            (
                AgeCategory.AGE_5_TO_15,
                Student.MISSING,
                SchoolType.ELEMENTARY,
                "Missing student, has school",
            ),
            (
                AgeCategory.AGE_5_TO_15,
                Student.MISSING,
                SchoolType.MIDDLE_SCHOOL,
                "Missing student, middle school",
            ),
            (
                AgeCategory.AGE_5_TO_15,
                Student.MISSING,
                SchoolType.HIGH_SCHOOL,
                "Missing student, high school",
            ),
            (
                AgeCategory.AGE_5_TO_15,
                Student.FULLTIME_INPERSON,
                SchoolType.MISSING,
                "Has student, missing school",
            ),
            (
                AgeCategory.AGE_5_TO_15,
                Student.NONSTUDENT,
                SchoolType.PNTA,
                "Non-student, PNTA school",
            ),
            (
                AgeCategory.AGE_5_TO_15,
                Student.NONSTUDENT,
                SchoolType.OTHER,
                "Non-student, OTHER school",
            ),
            (AgeCategory.AGE_16_TO_17, Student.MISSING, SchoolType.MISSING, "Teen, both missing"),
            (AgeCategory.AGE_16_TO_17, Student.NONSTUDENT, SchoolType.MISSING, "Teen, non-student"),
        ]

        for age, student, school_type, description in test_cases:
            df = pl.DataFrame(
                {
                    "age": [age.value],
                    "student": [student.value],
                    "school_type": [school_type.value],
                    "school_taz": [0],
                }
            )

            result = df.with_columns(ctramp_student_category_expression().alias("student_category"))

            assert (
                result["student_category"][0] == CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value
            ), f"Failed for: {description}"

    def test_adults_do_not_get_age_fallback(self) -> None:
        """Test that adults with missing data correctly get NOT_STUDENT, not age fallback.

        Ensures the age fallback only applies to children 5-17, not to adults.
        """
        adult_ages = [
            AgeCategory.AGE_18_TO_24,
            AgeCategory.AGE_25_TO_34,
            AgeCategory.AGE_35_TO_44,
            AgeCategory.AGE_65_TO_74,
        ]

        for age in adult_ages:
            df = pl.DataFrame(
                {
                    "age": [age.value],
                    "student": [Student.MISSING.value],
                    "school_type": [SchoolType.MISSING.value],
                    "school_taz": [0],
                }
            )

            result = df.with_columns(ctramp_student_category_expression().alias("student_category"))

            assert result["student_category"][0] == CTRAMPStudentCategory.NOT_STUDENT.value, (
                f"Adult age {age.name} incorrectly got age fallback"
            )

    def test_young_children_do_not_get_age_fallback(self) -> None:
        """Test that children under 5 with missing data get NOT_STUDENT, not age fallback.

        Ensures the age fallback only applies to school-age children (5-17), not preschool.
        """
        df = pl.DataFrame(
            {
                "age": [AgeCategory.AGE_UNDER_5.value],
                "student": [Student.MISSING.value],
                "school_type": [SchoolType.MISSING.value],
                "school_taz": [0],
            }
        )

        result = df.with_columns(ctramp_student_category_expression().alias("student_category"))

        assert result["student_category"][0] == CTRAMPStudentCategory.NOT_STUDENT.value

    @pytest.mark.parametrize(
        ("student", "school_type"),
        [
            (Student.NONSTUDENT, SchoolType.MISSING),
            (Student.NONSTUDENT, SchoolType.COLLEGE_4YEAR),
            (Student.MISSING, SchoolType.PNTA),
            (Student.FULLTIME_INPERSON, SchoolType.OTHER),
        ],
    )
    def test_edge_cases_and_catch_all(self, student: Student, school_type: SchoolType) -> None:
        """Test NONSTUDENT with school_type, PNTA, OTHER all trigger appropriate logic."""
        # Test with child age (should trigger fallback for missing/PNTA/OTHER)
        df_child = pl.DataFrame(
            {
                "age": [AgeCategory.AGE_5_TO_15.value],
                "student": [student.value],
                "school_type": [school_type.value],
                "school_taz": [0],
            }
        )
        # Test with adult age
        df_adult = pl.DataFrame(
            {
                "age": [AgeCategory.AGE_25_TO_34.value],
                "student": [student.value],
                "school_type": [school_type.value],
                "school_taz": [0],
            }
        )

        result_child = df_child.with_columns(
            ctramp_student_category_expression().alias("student_category")
        )
        result_adult = df_adult.with_columns(
            ctramp_student_category_expression().alias("student_category")
        )

        # Children should get age fallback when data is missing/invalid
        if student == Student.NONSTUDENT or school_type in [
            SchoolType.MISSING,
            SchoolType.PNTA,
            SchoolType.OTHER,
        ]:
            assert (
                result_child["student_category"][0]
                == CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value
            )
        # Adults: non-students → NOT_STUDENT; active students with ambiguous school_type → COLLEGE
        if student in (Student.NONSTUDENT, Student.MISSING):
            assert result_adult["student_category"][0] == CTRAMPStudentCategory.NOT_STUDENT.value
        elif school_type in [SchoolType.MISSING, SchoolType.PNTA, SchoolType.OTHER]:
            # Active adult students with missing/other school_type → assumed college
            assert (
                result_adult["student_category"][0] == CTRAMPStudentCategory.COLLEGE_OR_HIGHER.value
            )

    def test_valid_data_honored_despite_age_mismatch(self) -> None:
        """Test that valid data is honored even if age-inappropriate."""
        # Child in college - should still get COLLEGE_OR_HIGHER
        df = pl.DataFrame(
            {
                "person_id": [1, 2],
                "age": [AgeCategory.AGE_5_TO_15.value, AgeCategory.AGE_18_TO_24.value],
                "student": [Student.FULLTIME_INPERSON.value, Student.FULLTIME_INPERSON.value],
                "school_type": [SchoolType.COLLEGE_4YEAR.value, SchoolType.ELEMENTARY.value],
                "school_taz": [0, 0],
            }
        )

        result = df.with_columns(ctramp_student_category_expression().alias("student_category"))

        # Valid data should be honored (will trigger warnings but classification is correct)
        assert result["student_category"][0] == CTRAMPStudentCategory.COLLEGE_OR_HIGHER.value
        assert result["student_category"][1] == CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value


class TestStudentCategoryWarnings:
    """Tests for student category warning detection."""

    def test_missing_data_fallback_warning(self, standard_config) -> None:
        """Test warning for children 5-17 using age-based fallback."""
        df = pl.DataFrame(
            {
                "person_id": [1, 2, 3],
                "age": [
                    AgeCategory.AGE_5_TO_15.value,
                    AgeCategory.AGE_16_TO_17.value,
                    AgeCategory.AGE_18_TO_24.value,
                ],
                "student": [Student.MISSING.value, Student.MISSING.value, Student.MISSING.value],
                "school_type": [
                    SchoolType.MISSING.value,
                    SchoolType.MISSING.value,
                    SchoolType.MISSING.value,
                ],
                "school_taz": [0, 0, 0],
            }
        )

        df = df.with_columns(ctramp_student_category_expression().alias("student_category"))
        warnings = log_student_category_warnings(df, standard_config)

        # Two children (age 5-15 and 16-17) should trigger fallback warning
        assert warnings.get("missing_data_used_fallback", 0) == 2

    def test_preschool_students_warning(self, standard_config) -> None:
        """Test warning for children under 5 with student status."""
        df = pl.DataFrame(
            {
                "person_id": [1, 2],
                "age": [AgeCategory.AGE_UNDER_5.value, AgeCategory.AGE_UNDER_5.value],
                "student": [Student.FULLTIME_INPERSON.value, Student.PARTTIME_INPERSON.value],
                "school_type": [SchoolType.PRESCHOOL.value, SchoolType.DAYCARE.value],
                "school_taz": [0, 0],
            }
        )

        df = df.with_columns(ctramp_student_category_expression().alias("student_category"))
        warnings = log_student_category_warnings(df, standard_config)

        assert warnings.get("preschool_students", 0) == 2

    def test_age_inappropriate_school_types_warning(self, standard_config) -> None:
        """Test warning for age-inappropriate school type combinations."""
        df = pl.DataFrame(
            {
                "person_id": [1, 2, 3],
                "age": [
                    AgeCategory.AGE_16_TO_17.value,  # Teen in elementary
                    AgeCategory.AGE_5_TO_15.value,  # Child in college
                    AgeCategory.AGE_35_TO_44.value,  # Adult in HOME_SCHOOL
                ],
                "student": [
                    Student.FULLTIME_INPERSON.value,
                    Student.FULLTIME_INPERSON.value,
                    Student.PARTTIME_INPERSON.value,
                ],
                "school_type": [
                    SchoolType.ELEMENTARY.value,
                    SchoolType.COLLEGE_4YEAR.value,
                    SchoolType.HOME_SCHOOL.value,
                ],
                "school_taz": [0, 0, 0],
            }
        )

        df = df.with_columns(ctramp_student_category_expression().alias("student_category"))
        warnings = log_student_category_warnings(df, standard_config)

        assert warnings.get("age_inappropriate_school_types", 0) == 3

    def test_no_warnings_for_valid_data(self, standard_config) -> None:
        """Test that valid data produces no warnings."""
        df = pl.DataFrame(
            {
                "person_id": [1, 2, 3],
                "age": [
                    AgeCategory.AGE_5_TO_15.value,
                    AgeCategory.AGE_18_TO_24.value,
                    AgeCategory.AGE_25_TO_34.value,
                ],
                "student": [
                    Student.FULLTIME_INPERSON.value,
                    Student.FULLTIME_INPERSON.value,
                    Student.NONSTUDENT.value,
                ],
                "school_type": [
                    SchoolType.ELEMENTARY.value,
                    SchoolType.COLLEGE_4YEAR.value,
                    SchoolType.MISSING.value,
                ],
                "school_taz": [0, 0, 0],
            }
        )

        df = df.with_columns(ctramp_student_category_expression().alias("student_category"))
        warnings = log_student_category_warnings(df, standard_config)

        assert len(warnings) == 0

    def test_fulltime_workers_no_work_location_warning(self, standard_config) -> None:
        """Test warning for full-time workers (dual status) without work location."""
        df = pl.DataFrame(
            {
                "person_id": [1, 2, 3, 4],
                "age": [
                    AgeCategory.AGE_18_TO_24.value,
                    AgeCategory.AGE_25_TO_34.value,
                    AgeCategory.AGE_18_TO_24.value,
                    AgeCategory.AGE_25_TO_34.value,
                ],
                "employment": [
                    Employment.EMPLOYED_FULLTIME.value,
                    Employment.EMPLOYED_FULLTIME.value,
                    Employment.EMPLOYED_FULLTIME.value,
                    Employment.EMPLOYED_FULLTIME.value,
                ],
                "student": [
                    Student.FULLTIME_INPERSON.value,
                    Student.PARTTIME_INPERSON.value,  # Part-time student
                    Student.NONSTUDENT.value,
                    Student.NONSTUDENT.value,
                ],
                "school_type": [
                    SchoolType.COLLEGE_4YEAR.value,
                    SchoolType.COLLEGE_2YEAR.value,  # Part-time at college
                    SchoolType.MISSING.value,
                    SchoolType.MISSING.value,
                ],
                "school_taz": [100, 150, 0, 0],  # First two have school locations
                "work_taz": [0, 0, 0, 200],  # Only fourth has work location
            }
        )

        # Derive person_type and student_category
        df = df.with_columns(
            [
                ctramp_person_type_expression().alias("person_type"),
                ctramp_student_category_expression().alias("student_category"),
            ]
        )

        warnings = log_student_category_warnings(df, standard_config)

        # Should find 2 full-time workers without work location:
        # Person 1 is now FULL_TIME_WORKER (FT employment beats FT student) without work location
        # Person 2 is FULL_TIME_WORKER + part-time student without work location
        # Person 3 is FULL_TIME_WORKER + non-student without work location (doesn't trigger - no student status)  # noqa: E501
        # Person 4 has work_taz=200, so no warning
        assert warnings.get("fulltime_workers_no_work_location", 0) == 2


class TestStudentCategoryNullHandling:
    """Tests for null value handling in student category classification."""

    def test_null_student_status(self) -> None:
        """Test handling of null student status."""
        df = pl.DataFrame(
            {
                "person_id": [1, 2],
                "age": [AgeCategory.AGE_5_TO_15.value, AgeCategory.AGE_25_TO_34.value],
                "student": [None, None],
                "school_type": [SchoolType.ELEMENTARY.value, SchoolType.MISSING.value],
                "school_taz": [0, 0],
            }
        )

        result = df.with_columns(ctramp_student_category_expression().alias("student_category"))

        # Child should get age fallback, adult should get NOT_STUDENT
        assert result["student_category"][0] == CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value
        assert result["student_category"][1] == CTRAMPStudentCategory.NOT_STUDENT.value

    def test_null_school_type(self) -> None:
        """Test handling of null school type."""
        df = pl.DataFrame(
            {
                "person_id": [1, 2],
                "age": [AgeCategory.AGE_16_TO_17.value, AgeCategory.AGE_18_TO_24.value],
                "student": [Student.FULLTIME_INPERSON.value, Student.FULLTIME_INPERSON.value],
                "school_type": [None, None],
                "school_taz": [0, 0],
            }
        )

        result = df.with_columns(ctramp_student_category_expression().alias("student_category"))

        # Teen gets age fallback; adult student with null school_type → assumed college
        assert result["student_category"][0] == CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value
        assert result["student_category"][1] == CTRAMPStudentCategory.COLLEGE_OR_HIGHER.value

    def test_both_null(self) -> None:
        """Test handling when both student and school_type are null."""
        df = pl.DataFrame(
            {
                "person_id": [1, 2, 3],
                "age": [
                    AgeCategory.AGE_5_TO_15.value,
                    AgeCategory.AGE_16_TO_17.value,
                    AgeCategory.AGE_25_TO_34.value,
                ],
                "student": [None, None, None],
                "school_type": [None, None, None],
                "school_taz": [0, 0, 0],
            }
        )

        result = df.with_columns(ctramp_student_category_expression().alias("student_category"))

        # Children get age fallback, adults get NOT_STUDENT
        assert result["student_category"][0] == CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value
        assert result["student_category"][1] == CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value
        assert result["student_category"][2] == CTRAMPStudentCategory.NOT_STUDENT.value
