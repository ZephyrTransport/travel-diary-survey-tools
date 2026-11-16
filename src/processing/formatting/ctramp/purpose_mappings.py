"""CT-RAMP tour and trip purpose mappings."""

from typing import Literal

import polars as pl

from data_canon.codebook.ctramp import (
    CTRAMPStudentCategory,
    CTRAMPTourPurpose,
    CTRAMPTripPurpose,
)
from data_canon.codebook.persons import EMPLOYED, SchoolType
from data_canon.codebook.trips import WORK_PURPOSE_CATEGORIES, PurposeCategory

from .ctramp_config import CTRAMPConfig

# A joint record is no one person, so it has no employment of its own: a
# work-related purpose on it is never work.
NO_EMPLOYMENT = pl.lit(None, dtype=pl.Int64)

# Joint tour frequency group of each tour purpose: S(hopping), M(aintenance),
# E(ating out), V(isiting/social) or D(iscretionary).
PURPOSECATEGORY_TO_JTF_GROUP = {
    # Shopping
    PurposeCategory.SHOP.value: "S",
    # Maintenance/errands
    PurposeCategory.ERRAND.value: "M",
    # Eating out
    PurposeCategory.MEAL.value: "E",
    # Visiting/social/recreational
    PurposeCategory.SOCIALREC.value: "V",
    # Discretionary - Work/School (typically not joint, but possible)
    PurposeCategory.WORK.value: "D",
    PurposeCategory.WORK_RELATED.value: "D",
    PurposeCategory.SCHOOL.value: "D",
    PurposeCategory.SCHOOL_RELATED.value: "D",
    # Discretionary - Escort
    PurposeCategory.ESCORT.value: "D",
    # Discretionary - Other activities
    PurposeCategory.OTHER.value: "D",
    # Discretionary - Home/overnight (not typical joint tour destinations)
    PurposeCategory.HOME.value: "D",
    PurposeCategory.OVERNIGHT.value: "D",
    # Discretionary - Mode change (transfer point, not a tour purpose)
    PurposeCategory.CHANGE_MODE.value: "D",
    # Discretionary - Data quality issues
    PurposeCategory.MISSING.value: "D",
    PurposeCategory.PNTA.value: "D",
    PurposeCategory.NOT_IMPUTABLE.value: "D",
}

# Every PurposeCategory must have a JTF group, and nothing else may.
_missing_categories = {pc.value for pc in PurposeCategory} - set(PURPOSECATEGORY_TO_JTF_GROUP)
if _missing_categories:
    msg = f"Missing PurposeCategory mappings in PURPOSECATEGORY_TO_JTF_GROUP: {_missing_categories}"
    raise ValueError(msg)
if len(PURPOSECATEGORY_TO_JTF_GROUP) != len(PurposeCategory):
    msg = "PURPOSECATEGORY_TO_JTF_GROUP has keys that are not PurposeCategory values"
    raise ValueError(msg)


def is_employed(employment: pl.Expr) -> pl.Expr:
    """Whether the person has a job (see ``EMPLOYED``)."""
    return employment.is_in([e.value for e in EMPLOYED]).fill_null(value=False)


def is_work_purpose(purpose_category: pl.Expr, employment: pl.Expr) -> pl.Expr:
    """Whether CT-RAMP takes a tour of this purpose, by this person, as work.

    An employed person's work-related tour is work: the survey separates the
    workplace from a meeting, jobsite or volunteering, and the model does not.
    Employment decides, not person type, so a student with a job counts too.
    Anyone else's work-related tour stays non-mandatory -- a retiree's is
    volunteering, and a child's is a parent's purpose copied onto a shared trip.
    A reported workplace visit is work for everyone, as it always was.
    """
    return (purpose_category == PurposeCategory.WORK.value) | (
        purpose_category.is_in([p.value for p in WORK_PURPOSE_CATEGORIES]) & is_employed(employment)
    )


def ctramp_purpose_category_expression(
    purpose_category: pl.Expr,
    income: pl.Expr,
    school_type: pl.Expr,
    employment: pl.Expr,
    config: CTRAMPConfig,
    parent_tour_purpose: pl.Expr | None = None,
    is_subtour: pl.Expr | None = None,
    purpose_kind: Literal["tour", "trip"] = "tour",
) -> pl.Expr:
    """Map canonical PurposeCategory to CTRAMP purpose string.

    CTRAMP requires detailed purpose strings that distinguish work income
    levels (low/med/high/very high) and school types (grade/high/university).

    Args:
        purpose_category: Polars expression for canonical purpose category
            (from trips.PurposeCategory enum)
        income: Polars expression for household income (absolute dollars)
        school_type: Polars expression for school type
            (from persons.SchoolType enum)
        employment: Polars expression for the canonical employment code. Whether
            the person has a job decides whether a work-related purpose is work
            (see :func:`is_work_purpose`).
        config: CT-RAMP settings; its income thresholds split the work purposes.
        parent_tour_purpose: Polars expression for parent tour purpose (if available)
        is_subtour: Polars expression for identifying subtours
        purpose_kind: Emit detailed tour purposes (CTRAMPTourPurpose) or the
            simplified trip purposes (CTRAMPTripPurpose), which collapse work
            income levels, school types, and at-work/escort sub-purposes.

    Returns:
        Polars expression resolving to CTRAMP purpose string
    """
    # The detailed purpose expression below is always built from CTRAMPTourPurpose;
    # for trips it is collapsed to the simplified CTRAMPTripPurpose values at the end.
    purpose_enum = CTRAMPTourPurpose

    # Compute student category from school_type enum
    student_category = (
        pl.when(
            school_type.is_in(
                [
                    SchoolType.COLLEGE_2YEAR.value,
                    SchoolType.COLLEGE_4YEAR.value,
                    SchoolType.GRADUATE_SCHOOL.value,
                    SchoolType.VOCATIONAL.value,
                ]
            )
        )
        .then(pl.lit(CTRAMPStudentCategory.COLLEGE_OR_HIGHER.value))
        .when(
            school_type.is_in(
                [
                    SchoolType.ELEMENTARY.value,
                    SchoolType.MIDDLE_SCHOOL.value,
                    SchoolType.HIGH_SCHOOL.value,
                ]
            )
        )
        .then(pl.lit(CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value))
        .otherwise(pl.lit(CTRAMPStudentCategory.NOT_STUDENT.value))
    )

    # Home purpose
    home_expr = pl.when(purpose_category == PurposeCategory.HOME.value).then(
        pl.lit(purpose_enum.HOME.value)
    )

    # At-work sub-tour purposes only apply when the row is a subtour of a work tour.
    if parent_tour_purpose is not None and is_subtour is not None:
        is_at_work_subtour = is_subtour & is_work_purpose(parent_tour_purpose, employment)
    else:
        is_at_work_subtour = pl.lit(False)  # noqa: FBT003

    # At-work subtours with WORK/WORK_RELATED purposes
    # ATWORK_BUSINESS (check FIRST, before primary work)
    atwork_business_expr = home_expr.when(
        is_at_work_subtour
        & is_employed(employment)
        & (
            (purpose_category == PurposeCategory.WORK_RELATED.value)
            | (purpose_category == PurposeCategory.WORK.value)
        )
    ).then(pl.lit(purpose_enum.ATWORK_BUSINESS.value))

    # Work purposes - segmented by income (only for primary work tours, not at-work subtours)
    work_income_segmentation = (
        pl.when(income < config.income_low_threshold)
        .then(pl.lit(purpose_enum.WORK_LOW.value))
        .when(income < config.income_med_threshold)
        .then(pl.lit(purpose_enum.WORK_MED.value))
        .when(income < config.income_high_threshold)
        .then(pl.lit(purpose_enum.WORK_HIGH.value))
        .otherwise(pl.lit(purpose_enum.WORK_VERY_HIGH.value))
    )
    work_expr = atwork_business_expr.when(is_work_purpose(purpose_category, employment)).then(
        work_income_segmentation
    )

    # School purposes - segmented by student type
    school_segmentation_expr = (
        pl.when(student_category == CTRAMPStudentCategory.COLLEGE_OR_HIGHER.value)
        .then(pl.lit(purpose_enum.UNIVERSITY.value))
        .when(
            (student_category == CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value)
            & (school_type == SchoolType.HIGH_SCHOOL.value)
        )
        .then(pl.lit(purpose_enum.SCHOOL_HIGH.value))
        .otherwise(pl.lit(purpose_enum.SCHOOL_GRADE.value))
    )
    school_expr = work_expr.when(purpose_category.is_in([PurposeCategory.SCHOOL.value])).then(
        school_segmentation_expr
    )

    # Eating out vs at-work eating
    eatout_expr = (
        school_expr.when(is_at_work_subtour & (purpose_category == PurposeCategory.MEAL.value))
        .then(pl.lit(purpose_enum.ATWORK_EAT.value))
        .when(purpose_category == PurposeCategory.MEAL.value)
        .then(pl.lit(purpose_enum.EATOUT.value))
    )

    # Escort
    # Escort segmentation follows the current CT-RAMP student-category convention.
    escort_segmentation_expr = (
        pl.when(
            student_category.is_in(
                [
                    CTRAMPStudentCategory.COLLEGE_OR_HIGHER.value,
                    CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value,
                ]
            )
        )
        .then(pl.lit(purpose_enum.ESCORT_KIDS.value))
        .otherwise(pl.lit(purpose_enum.ESCORT_NO_KIDS.value))
    )
    escort_expr = (
        eatout_expr.when(is_at_work_subtour & (purpose_category == PurposeCategory.ESCORT.value))
        .then(pl.lit(purpose_enum.ATWORK_MAINT.value))
        .when(purpose_category == PurposeCategory.ESCORT.value)
        .then(escort_segmentation_expr)
    )

    # Shopping
    shopping_expr = (
        escort_expr.when(is_at_work_subtour & (purpose_category == PurposeCategory.SHOP.value))
        .then(pl.lit(purpose_enum.ATWORK_MAINT.value))
        .when(purpose_category == PurposeCategory.SHOP.value)
        .then(pl.lit(purpose_enum.SHOPPING.value))
    )

    # Social/recreation
    social_expr = shopping_expr.when(purpose_category == PurposeCategory.SOCIALREC.value).then(
        pl.lit(purpose_enum.SOCIAL.value)
    )

    # Maintenance/errands
    maintenance_expr = (
        social_expr.when(is_at_work_subtour & (purpose_category == PurposeCategory.ERRAND.value))
        .then(pl.lit(purpose_enum.ATWORK_MAINT.value))
        .when(purpose_category == PurposeCategory.ERRAND.value)
        .then(pl.lit(purpose_enum.OTHMAINT.value))
    )

    # Discretionary - all others
    # this includes school-related trips and work-related trips for nonworkers
    tour_purpose_expr = maintenance_expr.otherwise(pl.lit(purpose_enum.OTHDISCR.value))

    if purpose_kind == "tour":
        return tour_purpose_expr

    # Collapse detailed tour purposes to the simplified trip purposes. Pass-through
    # values (Home, university, eatout, shopping, social, othmaint, othdiscr) are
    # identical in both enums.
    return (
        pl.when(
            tour_purpose_expr.is_in(
                [
                    CTRAMPTourPurpose.WORK_LOW.value,
                    CTRAMPTourPurpose.WORK_MED.value,
                    CTRAMPTourPurpose.WORK_HIGH.value,
                    CTRAMPTourPurpose.WORK_VERY_HIGH.value,
                ]
            )
        )
        .then(pl.lit(CTRAMPTripPurpose.WORK.value))
        .when(
            tour_purpose_expr.is_in(
                [
                    CTRAMPTourPurpose.SCHOOL_HIGH.value,
                    CTRAMPTourPurpose.SCHOOL_GRADE.value,
                ]
            )
        )
        .then(pl.lit(CTRAMPTripPurpose.SCHOOL.value))
        .when(
            tour_purpose_expr.is_in(
                [
                    CTRAMPTourPurpose.ATWORK_BUSINESS.value,
                    CTRAMPTourPurpose.ATWORK_EAT.value,
                    CTRAMPTourPurpose.ATWORK_MAINT.value,
                ]
            )
        )
        .then(pl.lit(CTRAMPTripPurpose.ATWORK.value))
        .when(
            tour_purpose_expr.is_in(
                [
                    CTRAMPTourPurpose.ESCORT_KIDS.value,
                    CTRAMPTourPurpose.ESCORT_NO_KIDS.value,
                ]
            )
        )
        .then(pl.lit(CTRAMPTripPurpose.ESCORT.value))
        .otherwise(tour_purpose_expr)
    )
