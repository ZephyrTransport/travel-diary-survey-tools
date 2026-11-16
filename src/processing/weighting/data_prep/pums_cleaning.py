"""PUMS cleaning: which records count, in which dollars, on which weights.

Applied once, after the PUMS is loaded and checked and before anything is
recoded. Each rule is one function, run in the order ``clean_pums`` lists them,
and each logs what it changed, so the run log shows every record removed and why.
"""

import logging

import polars as pl

from processing.weighting.core.specs import HouseholdIncome, PumsWeights

logger = logging.getLogger(__name__)

#: ``TYPEHUGQ`` for a housing unit; 2 is institutional and 3 noninstitutional group quarters.
_HOUSING_UNIT = 1

#: ``ADJINC`` as a factor (2019-2023: 1.010 to 1.042), as ``read_pums`` returns it.
#: Outside this range it is not a factor, such as the official file's integer form (#184).
_ADJINC_RANGE = (0.5, 2.0)

#: ``RELSHIPP`` for roommate/housemate (34) and other nonrelative (36). Unmarried
#: partners (22, 24) and foster children (35) count as related, as the survey does.
_NONRELATIVES = (34, 36)

_HH_REQUIRED = ("SERIALNO", "NP", "TYPEHUGQ", "WGTP", "HINCP", "ADJINC")
_PERSON_REQUIRED = ("SERIALNO", "PWGTP")
_NONRELATIVE_INCOME_REQUIRED = ("SERIALNO", "PINCP", "RELSHIPP")


def clean_pums(
    hh_df: pl.DataFrame,
    person_df: pl.DataFrame,
    *,
    pums_weights: PumsWeights,
    household_income: HouseholdIncome,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Apply the PUMS rules, in order.

    1. `drop_vacant_homes`
    2. `drop_group_quarters`
    3. `adjust_income`
    4. `exclude_nonrelative_income`, when *household_income* is ``related_members``
    5. `apply_pums_weights`

    Raises:
        KeyError: If a column a rule reads is missing.
    """
    _require(hh_df, _HH_REQUIRED, "household")
    _require(person_df, _PERSON_REQUIRED, "person")
    hh_df, person_df = drop_vacant_homes(hh_df, person_df)
    hh_df, person_df = drop_group_quarters(hh_df, person_df)
    hh_df = adjust_income(hh_df)
    if household_income == HouseholdIncome.RELATED_MEMBERS:
        hh_df = exclude_nonrelative_income(hh_df, person_df)
    return apply_pums_weights(hh_df, person_df, pums_weights)


def drop_vacant_homes(
    hh_df: pl.DataFrame, person_df: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Drop vacant housing units (``NP = 0``).

    A vacant home has no residents, so it is not a household. Kept, each one
    counted as a one-person household in the lowest income bin (``HINCP`` holds
    the N/A code -60000) with no vehicles (``VEH`` holds -1).
    """
    kept = hh_df.filter(pl.col("NP") > 0)
    _log_dropped("vacant homes", hh_df, kept, person_df, person_df)
    return kept, person_df


def drop_group_quarters(
    hh_df: pl.DataFrame, person_df: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Drop group quarters, institutional and noninstitutional, and their residents.

    The population is household residents. Dorm residents travel and belong in
    it, as one-person households, but are left out until that is built.
    """
    kept = hh_df.filter(pl.col("TYPEHUGQ") == _HOUSING_UNIT)
    kept_persons = person_df.join(kept.select("SERIALNO"), on="SERIALNO", how="semi")
    _log_dropped("group quarters", hh_df, kept, person_df, kept_persons)
    return kept, kept_persons


def adjust_income(hh_df: pl.DataFrame) -> pl.DataFrame:
    """Put household income in the PUMS year's dollars: ``HINCP x ADJINC``.

    Each record's income covers the 12 months before its interview, so records
    from early and late in the year are in different dollars until adjusted.

    Raises:
        ValueError: If ``ADJINC`` is blank, or not a factor near 1.
    """
    factor = hh_df["ADJINC"]
    low, high = _ADJINC_RANGE
    if factor.null_count() or not low <= factor.min() <= factor.max() <= high:
        msg = (
            f"ADJINC is not an income factor: {factor.null_count():,} blank, range "
            f"{factor.min()} to {factor.max()}. It should be the factor itself (about 1.02); "
            "the official file's value is 1,000,000 times that and is divided when read."
        )
        raise ValueError(msg)
    logger.info("PUMS rule income: HINCP x ADJINC, factor %.6f to %.6f", factor.min(), factor.max())
    return hh_df.with_columns(pl.col("HINCP") * pl.col("ADJINC"))


def exclude_nonrelative_income(hh_df: pl.DataFrame, person_df: pl.DataFrame) -> pl.DataFrame:
    """Take roommates' and other nonrelatives' income out of household income.

    For a survey that asked for related members' income only. Each nonrelative's
    ``PINCP``, in the PUMS year's dollars and with a loss or "under 15" counted as
    nothing, comes off the household's adjusted ``HINCP``. A household this takes
    below zero is set to zero; one already below zero stays where it was.
    """
    _require(person_df, _NONRELATIVE_INCOME_REQUIRED, "person")
    nonrelative_income = (
        person_df.filter(pl.col("RELSHIPP").is_in(_NONRELATIVES))
        .join(hh_df.select("SERIALNO", "ADJINC"), on="SERIALNO", how="inner")
        .group_by("SERIALNO")
        .agg((pl.col("PINCP").clip(lower_bound=0) * pl.col("ADJINC")).sum().alias("_nonrel"))
    )
    hh_df = hh_df.join(nonrelative_income, on="SERIALNO", how="left").with_columns(
        pl.col("_nonrel").fill_null(0.0)
    )
    removed = hh_df.filter(pl.col("_nonrel") > 0)
    logger.info(
        "PUMS rule income: nonrelatives' income left out of %s of %s households, $%s in all",
        f"{removed.height:,}",
        f"{hh_df.height:,}",
        f"{removed['_nonrel'].sum():,.0f}",
    )
    return hh_df.with_columns(
        pl.max_horizontal(
            pl.col("HINCP") - pl.col("_nonrel"), pl.min_horizontal(pl.col("HINCP"), 0.0)
        ).alias("HINCP")
    ).drop("_nonrel")


def apply_pums_weights(
    hh_df: pl.DataFrame,
    person_df: pl.DataFrame,
    pums_weights: PumsWeights,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Set the weights the targets sum, per ``pums_weights``; replicates follow the weight.

    Household targets sum ``WGTP`` and person targets sum ``PWGTP`` in every
    case. ``household`` copies each household's weights onto its members;
    ``person`` sets each household's to the mean of its members'; ``independent``
    changes nothing.
    """
    hh_cols, person_cols = _weight_columns(hh_df, person_df)
    person_to_hh = dict(zip(person_cols, hh_cols, strict=True))
    before_hh, before_per = hh_df["WGTP"].sum(), person_df["PWGTP"].sum()

    if pums_weights is PumsWeights.HOUSEHOLD:
        household_weights = hh_df.select(
            "SERIALNO", *(pl.col(h).alias(p) for p, h in person_to_hh.items())
        )
        person_df = person_df.drop(person_cols).join(household_weights, on="SERIALNO", how="left")
    elif pums_weights is PumsWeights.PERSON:
        mean_weights = person_df.group_by("SERIALNO").agg(
            pl.col(p).cast(pl.Float64).mean().alias(h) for p, h in person_to_hh.items()
        )
        hh_df = hh_df.drop(hh_cols).join(mean_weights, on="SERIALNO", how="left")

    if hh_df["WGTP"].null_count() or person_df["PWGTP"].null_count():
        msg = "A household with no persons, or a person with no household, after the PUMS rules"
        raise ValueError(msg)
    logger.info(
        "PUMS rule weights (%s): households %.0f -> %.0f weighted, persons %.0f -> %.0f",
        pums_weights.value,
        before_hh,
        hh_df["WGTP"].sum(),
        before_per,
        person_df["PWGTP"].sum(),
    )
    return hh_df, person_df


def _weight_columns(hh_df: pl.DataFrame, person_df: pl.DataFrame) -> tuple[list[str], list[str]]:
    """``WGTP`` and ``PWGTP`` plus the replicates both tables carry, paired in order."""
    hh_reps = {c[4:] for c in hh_df.columns if c.startswith("WGTP") and c[4:].isdigit()}
    person_reps = {c[5:] for c in person_df.columns if c.startswith("PWGTP") and c[5:].isdigit()}
    if hh_reps != person_reps:
        msg = (
            f"Household and person replicate weights differ: {len(hh_reps)} WGTP and "
            f"{len(person_reps)} PWGTP replicates"
        )
        raise ValueError(msg)
    reps = sorted(hh_reps, key=int)
    return ["WGTP", *(f"WGTP{r}" for r in reps)], ["PWGTP", *(f"PWGTP{r}" for r in reps)]


def _log_dropped(
    rule: str,
    hh_before: pl.DataFrame,
    hh_after: pl.DataFrame,
    person_before: pl.DataFrame,
    person_after: pl.DataFrame,
) -> None:
    logger.info(
        "PUMS rule %s: dropped %d households (%.0f weighted), %d persons (%.0f weighted)",
        rule,
        hh_before.height - hh_after.height,
        hh_before["WGTP"].sum() - hh_after["WGTP"].sum(),
        person_before.height - person_after.height,
        person_before["PWGTP"].sum() - person_after["PWGTP"].sum(),
    )


def _require(df: pl.DataFrame, columns: tuple[str, ...], table: str) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        msg = f"PUMS {table} records lack {missing}, which the PUMS rules read"
        raise KeyError(msg)
