# ruff: noqa: D102, RUF012
"""Person-level weighting controls.

Each class maps raw survey / PUMS values into coarser category ints for
person-level weighting targets.

All ``survey_expr`` / ``pums_expr`` overrides implement the interface
documented in [`ControlTarget`][processing.weighting.controls.base.ControlTarget]
— individual method docstrings are omitted for brevity (ruff noqa: D102) because the base class
fully documents the expected behavior and error handling.
"""

import polars as pl

from data_canon.codebook.persons import (
    AgeCategory,
    CommuteFreq,
    Education,
    Employment,
    Ethnicity,
    Gender,
    JobType,
    Race,
    SchoolType,
    Student,
)
from data_canon.codebook.pums import (
    PumsEsr,
    PumsHisp,
    PumsJwtrns,
    PumsRac1p,
    PumsSchg,
    PumsSchl,
    PumsSex,
    PumsThresholds,
)
from data_canon.codebook.trips import ModeType
from processing.weighting.controls.base import (
    ControlLevel,
    ControlTarget,
    breakpoint_expr,
    identity_expr,
)
from processing.weighting.controls.enums import (
    CommuteModeCategory,
    EmploymentCategory,
    GenderCategory,
    StudentCategory,
    TotalCategory,
)


class GenderControl(ControlTarget):
    """Gender (male / female)."""

    name = "p_gender"
    level = ControlLevel.PERSON
    description = "Gender"
    categories = GenderCategory
    survey_fields = ("gender",)
    pums_fields = ("SEX",)

    _survey_map: dict[int, int] = {
        Gender.FEMALE.value: GenderCategory.FEMALE,
        Gender.MALE.value: GenderCategory.MALE,
    }

    _pums_map: dict[int, int] = {
        PumsSex.MALE.value: GenderCategory.MALE,
        PumsSex.FEMALE.value: GenderCategory.FEMALE,
    }

    def survey_expr(self) -> pl.Expr:
        return pl.col("gender").replace_strict(
            self._survey_map,
            default=None,
            return_dtype=pl.Int16,
        )

    def pums_expr(self) -> pl.Expr:
        return pl.col("SEX").replace_strict(
            self._pums_map,
            default=None,
            return_dtype=pl.Int16,
        )


class EmploymentControl(ControlTarget):
    """Employment status (full-time / part-time / not employed)."""

    name = "p_employment"
    level = ControlLevel.PERSON
    description = "Employment status"
    categories = EmploymentCategory
    survey_fields = ("employment",)
    pums_fields = ("ESR", "WKHP")

    _survey_map: dict[int, int] = {
        Employment.EMPLOYED_FULLTIME.value: EmploymentCategory.EMPLOYED_FULL,
        Employment.EMPLOYED_PARTTIME.value: EmploymentCategory.EMPLOYED_PART,
        Employment.EMPLOYED_SELF.value: EmploymentCategory.EMPLOYED_FULL,
        Employment.UNEMPLOYED_NOT_LOOKING.value: EmploymentCategory.NOT_EMPLOYED,
        Employment.UNEMPLOYED_LOOKING.value: EmploymentCategory.NOT_EMPLOYED,
        Employment.EMPLOYED_UNPAID.value: EmploymentCategory.NOT_EMPLOYED,
        Employment.EMPLOYED_FURLOUGHED.value: EmploymentCategory.EMPLOYED_PART,
        Employment.MISSING.value: EmploymentCategory.NOT_EMPLOYED,
    }

    _pums_employed_esr: list[int] = PumsEsr.EMPLOYED
    _pums_not_employed_esr: list[int] = PumsEsr.NOT_EMPLOYED

    def survey_expr(self) -> pl.Expr:
        emp = pl.col("employment")
        return (
            pl.when(emp.is_null())
            .then(EmploymentCategory.NOT_EMPLOYED)
            .otherwise(
                emp.replace_strict(
                    self._survey_map,
                    default=None,
                    return_dtype=pl.Int16,
                )
            )
            .cast(pl.Int16)
        )

    def pums_expr(self) -> pl.Expr:
        """ESR + WKHP → employment category (WKHP < 35 = part-time).

        ESR is null for persons under 16 — they are coded as not employed.
        """
        esr = pl.col("ESR")
        wkhp = pl.col("WKHP")
        return (
            pl.when(
                esr.is_in(self._pums_employed_esr)
                & wkhp.is_not_null()
                & (wkhp > 0)
                & (wkhp < PumsThresholds.PART_TIME_HOURS)
            )
            .then(EmploymentCategory.EMPLOYED_PART)
            .when(esr.is_in(self._pums_employed_esr))
            .then(EmploymentCategory.EMPLOYED_FULL)
            .otherwise(EmploymentCategory.NOT_EMPLOYED)
            .cast(pl.Int16)
        )


class CommuteModeControl(ControlTarget):
    """Commute mode (drive, carpool, transit, bike, walk, mostly_remote, other, N/A).

    Survey side: Uses a combination of ``job_type``, ``telework_freq``, and
    ``commute_freq`` to identify mostly-remote workers — those whose telework
    frequency exceeds their commute frequency.  For all other workers the
    observed ``work_mode`` determines the category.

    PUMS side: ``JWTRNS=11`` ("Worked at home") maps to ``MOSTLY_REMOTE``.
    This is the closest analog — the PUMS question asks about the *usual*
    mode to work, so respondents who mostly remote-work select this.
    """

    name = "p_commute_mode"
    level = ControlLevel.PERSON
    description = "Commute mode"
    categories = CommuteModeCategory
    survey_fields = ("work_mode", "job_type", "telework_freq", "commute_freq")
    pums_fields = ("JWTRNS", "JWRIP")

    # -- Survey helpers ----------------------------------------------------

    # Telework-freq codes that count as "weekly or more" (eligible for ratio)
    _WEEKLY_PLUS: list[int] = [
        CommuteFreq.DAYS_6_7.value,  # 1
        CommuteFreq.DAYS_5.value,  # 2
        CommuteFreq.DAYS_4.value,  # 3
        CommuteFreq.DAYS_3.value,  # 4
        CommuteFreq.DAYS_2.value,  # 5
        CommuteFreq.DAY_1.value,  # 6
    ]

    # Commute-freq codes meaning "rarely or never commutes"
    _RARELY_COMMUTES: list[int] = [
        CommuteFreq.LESS_THAN_MONTHLY.value,  # 8
        CommuteFreq.NEVER.value,  # 996
    ]

    # ModeType → CommuteModeCategory (intermediate for building _survey_map)
    _mode_type_to_commute: dict[ModeType, int] = {
        ModeType.WALK: CommuteModeCategory.WALK,
        ModeType.BIKE: CommuteModeCategory.BIKE,
        ModeType.BIKESHARE: CommuteModeCategory.BIKE,
        ModeType.SCOOTERSHARE: CommuteModeCategory.OTHER,
        ModeType.CAR: CommuteModeCategory.DRIVE_ALONE,
        ModeType.CARSHARE: CommuteModeCategory.DRIVE_ALONE,
        ModeType.TRANSIT: CommuteModeCategory.TRANSIT,
        ModeType.FERRY: CommuteModeCategory.TRANSIT,
        ModeType.TNC: CommuteModeCategory.OTHER,
        ModeType.TAXI: CommuteModeCategory.OTHER,
        ModeType.SHUTTLE: CommuteModeCategory.OTHER,
        ModeType.SCHOOL_BUS: CommuteModeCategory.OTHER,
        ModeType.LONG_DISTANCE: CommuteModeCategory.OTHER,
        ModeType.OTHER: CommuteModeCategory.OTHER,
        ModeType.MISSING: CommuteModeCategory.NA,
    }

    # Mode.value → CommuteModeCategory  (outer-iterable trick for class scope)
    _survey_map: dict[int, int] = {
        mode.value: mtc.get(mtype, CommuteModeCategory.OTHER)
        for mtc in [_mode_type_to_commute]
        for mode, mtype in ModeType.from_mode().items()
    }

    # -- PUMS map ----------------------------------------------------------

    _pums_map: dict[int, int] = {
        PumsJwtrns.CAR_TRUCK_VAN.value: CommuteModeCategory.DRIVE_ALONE,
        PumsJwtrns.BUS.value: CommuteModeCategory.TRANSIT,
        PumsJwtrns.STREETCAR.value: CommuteModeCategory.TRANSIT,
        PumsJwtrns.SUBWAY.value: CommuteModeCategory.TRANSIT,
        PumsJwtrns.RAILROAD.value: CommuteModeCategory.TRANSIT,
        PumsJwtrns.FERRYBOAT.value: CommuteModeCategory.TRANSIT,
        PumsJwtrns.TAXICAB.value: CommuteModeCategory.OTHER,
        PumsJwtrns.MOTORCYCLE.value: CommuteModeCategory.OTHER,
        PumsJwtrns.BICYCLE.value: CommuteModeCategory.BIKE,
        PumsJwtrns.WALKED.value: CommuteModeCategory.WALK,
        PumsJwtrns.WORKED_AT_HOME.value: CommuteModeCategory.MOSTLY_REMOTE,
        PumsJwtrns.OTHER.value: CommuteModeCategory.OTHER,
        PumsJwtrns.NA.value: CommuteModeCategory.NA,
    }

    def survey_expr(self) -> pl.Expr:
        """Classify survey persons into commute-mode categories.

        Mostly-remote detection (checked first):
          1. ``job_type == 3`` (WFH always)
          2. telework 5+ days/week AND rarely/never commutes
          3. Telework ratio >60%: ``commute_freq > telework_freq + 1``
             for weekly-or-more frequencies on both sides

        Everyone else falls through to the ``work_mode`` mapping.
        """
        jt = pl.col("job_type")
        tw = pl.col("telework_freq")
        cf = pl.col("commute_freq")
        wm = pl.col("work_mode")

        # Codes are inverse-ordered: 1 = most frequent, 8 = least frequent
        # Higher code → lower frequency.  So commute_freq > telework_freq + 1
        # means "commutes much less often than teleworks" → mostly remote.
        is_mostly_remote = (
            # (a) job_type == WFH always
            (jt == JobType.WFH.value)
            # (b) telework 5+ days/week AND rarely/never commutes
            | (
                tw.is_in([CommuteFreq.DAYS_6_7.value, CommuteFreq.DAYS_5.value])
                & cf.is_in(self._RARELY_COMMUTES)
            )
            # (c) >60% telework ratio among weekly+ frequencies
            | (
                (cf > tw + 1)
                & tw.is_in(self._WEEKLY_PLUS)
                & cf.is_in(self._WEEKLY_PLUS)
                & (jt != JobType.WFH.value)
                & (tw != CommuteFreq.MISSING.value)
                & (cf != CommuteFreq.MISSING.value)
            )
        )

        mode_based = (
            pl.when(wm.is_null())
            .then(CommuteModeCategory.NA)
            .otherwise(
                wm.replace_strict(
                    self._survey_map,
                    return_dtype=pl.Int16,
                ),
            )
        )

        return (
            pl.when(is_mostly_remote)
            .then(CommuteModeCategory.MOSTLY_REMOTE)
            .otherwise(mode_based)
            .cast(pl.Int16)
        )

    def pums_expr(self) -> pl.Expr:
        """JWRIP >= 2 refines drive-alone to carpool."""
        jwtrns = pl.col("JWTRNS")
        base = jwtrns.replace_strict(
            self._pums_map,
            # default=CommuteModeCategory.OTHER, # defaults are dangerous...
            return_dtype=pl.Int16,
        )
        return (
            pl.when(jwtrns.is_null() | (jwtrns == 0))
            .then(CommuteModeCategory.NA)
            .when(
                (jwtrns == PumsJwtrns.CAR_TRUCK_VAN.value)
                & pl.col("JWRIP").is_not_null()
                & (pl.col("JWRIP") >= PumsThresholds.CARPOOL_MIN_OCCUPANCY),
            )
            .then(CommuteModeCategory.CARPOOL)
            .otherwise(base)
            .cast(pl.Int16)
        )


class StudentControl(ControlTarget):
    """Student status (K-12 / college / not a student).

    Classification priority:
        1. Explicit non-student (``student == NONSTUDENT``) → NOT_STUDENT
        2. Known K-12 school type (preschool thru high school) → K12
        3. Known college school type → COLLEGE
        4. Childcare / at-home (not school in the Census sense) → NOT_STUDENT
        5. Age-based fallback when both student & school_type are missing:
           school-age children (5-17) → K12, everyone else → NOT_STUDENT
        6. Active student with missing school_type → COLLEGE (adult default)

    The ``student`` field is only collected for persons age 16+ in the
    survey instrument; younger children have ``student = 995`` (MISSING)
    but typically have a valid ``school_type``, so school_type is checked
    before discarding missing students.
    """

    name = "p_student"
    level = ControlLevel.PERSON
    description = "Student status"
    categories = StudentCategory
    survey_fields = ("student", "school_type", "age")
    pums_fields = ("SCHG",)

    # Aligns with Census SCHG: nursery/preschool (SCHG=1) through grade 12.
    # DAYCARE and ATHOME are excluded — the Census does not count childcare
    # as school enrollment.
    _k12_school_types: frozenset[int] = frozenset(
        {
            SchoolType.PRESCHOOL.value,
            SchoolType.HOME_SCHOOL.value,
            SchoolType.ELEMENTARY.value,
            SchoolType.MIDDLE_SCHOOL.value,
            SchoolType.HIGH_SCHOOL.value,
        }
    )

    _college_school_types: frozenset[int] = frozenset(
        {
            SchoolType.VOCATIONAL.value,
            SchoolType.COLLEGE_2YEAR.value,
            SchoolType.COLLEGE_4YEAR.value,
            SchoolType.GRADUATE_SCHOOL.value,
        }
    )

    _childcare_school_types: frozenset[int] = frozenset(
        {
            SchoolType.ATHOME.value,
            SchoolType.DAYCARE.value,
        }
    )

    # AgeCategory values corresponding to school-age children (5-17).
    _school_age: frozenset[int] = frozenset(
        {
            AgeCategory.AGE_5_TO_15.value,
            AgeCategory.AGE_16_TO_17.value,
        }
    )

    _pums_map: dict[int, int] = {
        PumsSchg.NOT_ATTENDING.value: StudentCategory.NOT_STUDENT,
        **dict.fromkeys(PumsSchg.K12, StudentCategory.STUDENT_K12),
        **dict.fromkeys(PumsSchg.COLLEGE, StudentCategory.STUDENT_COLLEGE),
    }

    def survey_expr(self) -> pl.Expr:
        stu = pl.col("student")
        stype = pl.col("school_type")
        age = pl.col("age")

        is_missing_student = (stu == Student.MISSING.value) | stu.is_null()
        is_missing_school_type = (
            stype.is_in([SchoolType.MISSING.value, SchoolType.PNTA.value]) | stype.is_null()
        )

        return (
            # 1. Explicit non-student
            pl.when(stu == Student.NONSTUDENT.value)
            .then(StudentCategory.NOT_STUDENT)
            # 2. Known K-12 school type (preschool thru high school)
            .when(stype.is_in(list(self._k12_school_types)))
            .then(StudentCategory.STUDENT_K12)
            # 3. Known college school type
            .when(stype.is_in(list(self._college_school_types)))
            .then(StudentCategory.STUDENT_COLLEGE)
            # 4. Childcare / at-home → not school in the Census sense
            .when(stype.is_in(list(self._childcare_school_types)))
            .then(StudentCategory.NOT_STUDENT)
            # 5. Both fields missing → age-based fallback
            .when(is_missing_student & is_missing_school_type & age.is_in(list(self._school_age)))
            .then(StudentCategory.STUDENT_K12)
            .when(is_missing_student & is_missing_school_type)
            .then(StudentCategory.NOT_STUDENT)
            # 6. No rule matched → null so downstream warnings surface
            .otherwise(None)
            .cast(pl.Int16)
        )

    def pums_expr(self) -> pl.Expr:
        return (
            pl.when(pl.col("SCHG").is_null())
            .then(StudentCategory.NOT_STUDENT)
            .otherwise(
                pl.col("SCHG").replace_strict(
                    self._pums_map,
                    default=StudentCategory.NOT_STUDENT,
                    return_dtype=pl.Int16,
                ),
            )
            .cast(pl.Int16)
        )


class EducationControl(ControlTarget):
    """Education attainment (canonical Education enum)."""

    name = "p_education"
    level = ControlLevel.PERSON
    description = "Education attainment"
    categories = Education
    survey_fields = ("education",)
    pums_fields = ("SCHL",)

    _schl_map: dict[int, int] = {  # type: ignore[dict-item]
        **dict.fromkeys(PumsSchl.LESS_THAN_HS, Education.LESS_HIGH_SCHOOL.value),
        **dict.fromkeys(PumsSchl.HIGH_SCHOOL, Education.HIGHSCHOOL.value),
        **dict.fromkeys(PumsSchl.SOME_COLLEGE, Education.SOME_COLLEGE.value),
        PumsSchl.ASSOCIATE.value: Education.ASSOCIATE.value,
        PumsSchl.BACHELORS.value: Education.BACHELORS.value,
        **dict.fromkeys(PumsSchl.GRADUATE, Education.GRAD.value),
    }

    def survey_expr(self) -> pl.Expr:
        return identity_expr("education", Education)

    def pums_expr(self) -> pl.Expr:
        return pl.col("SCHL").replace_strict(
            self._schl_map,
            default=None,
            return_dtype=pl.Int16,
        )


class RaceControl(ControlTarget):
    """Race (canonical Race enum)."""

    name = "p_race"
    level = ControlLevel.PERSON
    description = "Race"
    categories = Race
    survey_fields = ("race",)
    pums_fields = ("RAC1P",)

    _pums_map: dict[int, int] = {  # type: ignore[dict-item]
        PumsRac1p.WHITE.value: Race.WHITE.value,
        PumsRac1p.BLACK.value: Race.AFAM.value,
        PumsRac1p.AIAN.value: Race.NATIVE.value,
        PumsRac1p.ALASKA_NATIVE.value: Race.NATIVE.value,
        PumsRac1p.AIAN_BOTH.value: Race.NATIVE.value,
        PumsRac1p.ASIAN.value: Race.ASIAN.value,
        PumsRac1p.NHPI.value: Race.PACIFIC.value,
        PumsRac1p.OTHER.value: Race.OTHER.value,
        PumsRac1p.TWO_OR_MORE.value: Race.MULTI.value,
    }

    def survey_expr(self) -> pl.Expr:
        return identity_expr("race", Race)

    def pums_expr(self) -> pl.Expr:
        return pl.col("RAC1P").replace_strict(
            self._pums_map,
            default=Race.OTHER.value,
            return_dtype=pl.Int16,
        )


class EthnicityControl(ControlTarget):
    """Hispanic/Latino ethnicity (canonical Ethnicity enum)."""

    name = "p_ethnicity"
    level = ControlLevel.PERSON
    description = "Hispanic/Latino ethnicity"
    categories = Ethnicity
    survey_fields = ("ethnicity",)
    pums_fields = ("HISP",)

    _pums_map: dict[int, int] = {  # type: ignore[dict-item]
        PumsHisp.NOT_HISPANIC.value: Ethnicity.NOT_HISPANIC.value,
        PumsHisp.MEXICAN.value: Ethnicity.MEXICAN.value,
        PumsHisp.PUERTO_RICAN.value: Ethnicity.PUERTO_RICAN.value,
        PumsHisp.CUBAN.value: Ethnicity.CUBAN.value,
    }

    def survey_expr(self) -> pl.Expr:
        return identity_expr("ethnicity", Ethnicity)

    def pums_expr(self) -> pl.Expr:
        return pl.col("HISP").replace_strict(
            self._pums_map,
            default=Ethnicity.OTHER.value,
            return_dtype=pl.Int16,
        )


class AgeControl(ControlTarget):
    """Age (canonical AgeCategory breakpoints)."""

    name = "p_age"
    level = ControlLevel.PERSON
    description = "Age"
    categories = AgeCategory
    survey_fields = ("age",)
    pums_fields = ("AGEP",)

    def survey_expr(self) -> pl.Expr:
        return identity_expr("age", AgeCategory)

    def pums_expr(self) -> pl.Expr:
        return breakpoint_expr("AGEP", AgeCategory)


class PersonTotalControl(ControlTarget):
    """Structural control: total persons (incidence = 1 per person).

    When aggregated to the seed table (one row per household), the incidence
    column becomes the count of persons in the household — effectively the
    non-top-coded household size.
    """

    name = "p_total"
    level = ControlLevel.PERSON
    description = "Total persons"
    categories = TotalCategory
    survey_fields = ()
    pums_fields = ()
    structural = True

    def survey_expr(self) -> pl.Expr:
        return pl.lit(1).cast(pl.Int16)

    def pums_expr(self) -> pl.Expr:
        return pl.lit(1).cast(pl.Int16)
