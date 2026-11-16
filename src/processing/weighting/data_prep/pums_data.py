"""PUMS microdata I/O.

Reads the Census Bureau's official ACS PUMS 1-year files, kept in the census
folder as ``pums_<year>/csv_h<st>.zip`` and ``pums_<year>/csv_p<st>.zip``,
translates their coding to the Census API's, checks them, and casts them to
numeric dtypes. A file missing from the folder is downloaded there first, so a
fresh clone runs without any manual step; after that every run reads it as it is.

Records come back as the Census published them: which ones count is decided by
``pums_cleaning``, recoding lives in ``control_data`` and aggregation in ``incidence``.
"""

import io
import logging
import zipfile
from pathlib import Path

import polars as pl
import pygris
import requests

from processing.weighting.controls.base import ControlLevel
from processing.weighting.controls.registry import pums_variables

logger = logging.getLogger(__name__)


# Infrastructure vars that don't come from any ControlTarget.
# Names use the **2020+** (canonical) convention; for older vintages the
# aliases in ``_PRE2020_ALIASES`` are requested instead and then renamed.
_HH_INFRA = {"SERIALNO", "PUMA", "STATE", "WGTP", "TYPEHUGQ", "ADJINC"}


# PINCP and RELSHIPP: nonrelatives' income, for HouseholdIncome.RELATED_MEMBERS.
_PERSON_INFRA = {"SERIALNO", "SPORDER", "PUMA", "STATE", "PWGTP", "PINCP", "RELSHIPP"}


# Variable renames between pre-2020 and 2020+ PUMS vintages.
# Keys = canonical (2020+) name, values = pre-2020 name.
_PRE2020_ALIASES: dict[str, str] = {
    "STATE": "ST",
    "TYPEHUGQ": "TYPE",
}


# Replicate weight columns for variance estimation (80 per table)
_HH_REPLICATE_WEIGHTS = {f"WGTP{i}" for i in range(1, 81)}


_PERSON_REPLICATE_WEIGHTS = {f"PWGTP{i}" for i in range(1, 81)}


#: TYPEHUGQ: 1 housing unit, 2 institutional GQ, 3 noninstitutional GQ.
_TYPEHUGQ_DOMAIN = (1, 2, 3)


# Derived dynamically from the registry + infrastructure
_HH_VARS = _HH_INFRA | pums_variables(ControlLevel.HOUSEHOLD)


_PERSON_VARS = _PERSON_INFRA | pums_variables(ControlLevel.PERSON)


#: Where the Census Bureau publishes each year's official 1-year files.
_OFFICIAL_URL = "https://www2.census.gov/programs-surveys/acs/data/pums/{year}/1-Year/{name}"

#: The official files leave "not applicable" blank where the API writes a code.
#: Checked record by record against the API for California 2023.
_NOT_APPLICABLE = {
    "HINCP": -60000,  # vacant homes and group quarters
    "PINCP": -19999,  # under 15
    "VEH": -1,  # vacant homes and group quarters
    "ESR": 0,  # under 16
    "WKHP": 0,  # did not work
    "JWTRNS": 0,  # not a worker
    "JWRIP": 0,  # not a worker, or did not drive
    "SCHG": 0,  # not in school
    "SCHL": 0,  # under 3
}


#: Lowest correlation a replicate weight may have with its full weight.
_MIN_REPLICATE_CORRELATION = 0.5

#: Widest spread allowed among the replicates' correlations with their full weight.
_MAX_REPLICATE_CORRELATION_SPREAD = 0.1


def _replicate_weight_problems(
    df: pl.DataFrame, weight_col: str, replicates: set[str]
) -> list[str]:
    """Replicate weights that do not track their own full weight.

    Unlike the household-size check there is no cross-table fact to test these
    against, and a shuffled weight is still a plausible weight -- which is how
    65% of them stayed wrong across months of runs without anything noticing.
    Correlation with the full weight is the invariant that does hold. Two bounds
    test it:

    * ``_MIN_REPLICATE_CORRELATION``: a replicate is a perturbation of the full
      weight, so the two track each other. On clean 2023 California data every
      replicate correlates 0.77 with WGTP; misattributed ones sat at 0.20. The
      bound is loose because the sound value is a property of the sample --
      another state or vintage need not give 0.77 -- so it only separates
      "tracks its weight" from "does not".
    * ``_MAX_REPLICATE_CORRELATION_SPREAD``: replicates of one weight are built
      alike, so they correlate with it to the same degree, whatever that is.
      Sound data agrees to within 0.009 across all 80; a partly misattributed
      file split into two groups 0.605 apart. Being relative, this needs no
      assumption about the sound value, which makes it the stronger check. It
      cannot stand alone: if *every* replicate were misattributed they would
      agree on being wrong, which the first bound catches.
    """
    present = sorted(replicates & set(df.columns))
    if weight_col not in df.columns or not present:
        return []
    occupied = df.filter(pl.col(weight_col) > 0)
    if occupied.height < 2:  # noqa: PLR2004 - a correlation needs two points
        return []

    measured = {}
    for col in present:
        corr = occupied.select(pl.corr(weight_col, col)).item()
        if corr is not None:
            measured[col] = corr
    if not measured:
        return []

    problems = []
    adrift = sorted(
        ((c, v) for c, v in measured.items() if v < _MIN_REPLICATE_CORRELATION),
        key=lambda pair: pair[1],
    )
    if adrift:
        worst = ", ".join(f"{c} ({v:.2f})" for c, v in adrift[:5])
        problems.append(
            f"{len(adrift)} of {len(present)} {weight_col} replicate weight(s) do not track "
            f"{weight_col} (correlation below {_MIN_REPLICATE_CORRELATION}): {worst}"
        )

    spread = max(measured.values()) - min(measured.values())
    if spread > _MAX_REPLICATE_CORRELATION_SPREAD:
        low = min(measured, key=lambda c: measured[c])
        high = max(measured, key=lambda c: measured[c])
        problems.append(
            f"{weight_col} replicate weights disagree with each other: correlation with "
            f"{weight_col} ranges {measured[low]:.2f} ({low}) to {measured[high]:.2f} "
            f"({high}), a spread of {spread:.2f} where sound data agrees to within "
            f"{_MAX_REPLICATE_CORRELATION_SPREAD}. Replicates are built alike, so a split "
            "like this means some are on the wrong records"
        )
    return problems


def check_pums_complete(hh_df: pl.DataFrame, person_df: pl.DataFrame) -> None:
    """Raise if any variable is blank on any record, after the coding translation.

    The official files leave "not applicable" blank, and ``_to_api_coding``
    writes it as the API's code (0, -1, -60000). A blank left after that is a
    variable the translation does not cover, never an answer (#184).

    Raises:
        ValueError: Naming each variable with blanks and how many.
    """
    blanks = [
        f"{table} {col}: {n:,} of {df.height:,}"
        for table, df in (("household", hh_df), ("person", person_df))
        for col in df.columns
        if (n := df[col].null_count())
    ]
    if blanks:
        msg = "PUMS has blank values the coding translation does not cover:\n  - " + (
            "\n  - ".join(blanks)
        )
        raise ValueError(msg)


def check_pums_integrity(hh_df: pl.DataFrame, person_df: pl.DataFrame) -> None:
    """Verify PUMS records are internally consistent, or raise.

    A record carrying another record's values still looks well formed -- right
    keys, plausible numbers, correct row count. These checks look for the
    disagreements that misattribution produces and that nothing downstream can
    see:

    * ``NP`` is the household's own count of its members, so it must equal the
      number of person records sharing that ``SERIALNO``. This is the sharpest
      check available: the two facts come from different tables, so they only
      agree when both are attached to the right household. It found 965
      disagreements in a corrupted fetch and none in a sound one.
    * ``TYPEHUGQ`` distinguishes housing units from the two kinds of group
      quarters and admits nothing else. A foreign value there is proof of
      misattribution -- it is how this class of corruption was first noticed.
    * Keys identify rows, or the tables cannot be joined at all.

    Args:
        hh_df: PUMS household records, before ``clean_pums``.
        person_df: PUMS person records.

    Raises:
        ValueError: If any check fails, naming the count and what it means.
    """
    problems: list[str] = []

    if hh_df["SERIALNO"].n_unique() != hh_df.height:
        problems.append(
            f"SERIALNO repeats in households: {hh_df['SERIALNO'].n_unique():,} distinct "
            f"of {hh_df.height:,} rows"
        )
    if {"SERIALNO", "SPORDER"} <= set(person_df.columns):
        n_unique = person_df.select(["SERIALNO", "SPORDER"]).n_unique()
        if n_unique != person_df.height:
            problems.append(
                f"(SERIALNO, SPORDER) repeats in persons: {n_unique:,} distinct "
                f"of {person_df.height:,} rows"
            )

    if "TYPEHUGQ" in hh_df.columns:
        foreign = hh_df.filter(
            pl.col("TYPEHUGQ").is_not_null() & ~pl.col("TYPEHUGQ").is_in(_TYPEHUGQ_DOMAIN)
        )
        if foreign.height:
            seen = sorted(foreign["TYPEHUGQ"].unique().to_list())[:8]
            problems.append(
                f"{foreign.height:,} household(s) have a TYPEHUGQ outside "
                f"{sorted(_TYPEHUGQ_DOMAIN)}: saw {seen}"
            )

    if "NP" in hh_df.columns and "SERIALNO" in person_df.columns:
        counted = person_df.group_by("SERIALNO").agg(pl.len().alias("_n_persons"))
        joined = (
            hh_df.select("SERIALNO", "NP")
            .join(counted, on="SERIALNO", how="left")
            .with_columns(pl.col("_n_persons").fill_null(0))
        )
        # NP == 0 marks a vacant unit, which correctly has no person records.
        disagree = joined.filter((pl.col("NP") > 0) & (pl.col("NP") != pl.col("_n_persons")))
        if disagree.height:
            problems.append(
                f"{disagree.height:,} household(s) report an NP that disagrees with their "
                "own number of person records"
            )

    problems += _replicate_weight_problems(hh_df, "WGTP", _HH_REPLICATE_WEIGHTS)
    problems += _replicate_weight_problems(person_df, "PWGTP", _PERSON_REPLICATE_WEIGHTS)

    if problems:
        msg = (
            "PUMS records are not internally consistent, so values are attached to "
            "the wrong records:\n  - "
            + "\n  - ".join(problems)
            + "\n\nThis is a damaged file, not a data-quality question: these facts are "
            "true of the Census extract by construction. Delete the file to have it "
            "downloaded again."
        )
        raise ValueError(msg)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def pums_files(census_dir: str | Path, state_fips: str, pums_year: int) -> tuple[Path, Path]:
    """Where the official household and person files live in the census folder.

    Named as the Census names them, such as ``csv_hca.zip`` and ``csv_pca.zip``
    for California.
    """
    states = pygris.fips_codes()
    (abbr,) = states.loc[states["state_code"] == state_fips, "state"].unique()
    folder = Path(census_dir) / f"pums_{pums_year}"
    return folder / f"csv_h{abbr.lower()}.zip", folder / f"csv_p{abbr.lower()}.zip"


def _ensure_file(path: Path, pums_year: int) -> None:
    """Download the official file to *path* when it is not there yet."""
    if path.exists():
        return
    url = _OFFICIAL_URL.format(year=pums_year, name=path.name)
    logger.info("Downloading %s to %s", url, path)
    resp = requests.get(url, timeout=600)
    resp.raise_for_status()
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(path.name + ".part")
    partial.write_bytes(resp.content)
    partial.replace(path)


def _read_official(path: Path, wanted: set[str]) -> pl.DataFrame:
    """Read the *wanted* columns present in an official PUMS zip, as text."""
    with zipfile.ZipFile(path) as zf:
        (inner,) = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        source = io.BytesIO(zf.read(inner))
    header = pl.read_csv(source, n_rows=0).columns
    source.seek(0)
    renamed = {old: new for new, old in _PRE2020_ALIASES.items() if old in header}
    columns = [c for c in header if c in wanted or renamed.get(c) in wanted]
    df = pl.read_csv(source, columns=columns, infer_schema_length=0)
    return df.rename(renamed)


def _to_api_coding(df: pl.DataFrame) -> pl.DataFrame:
    """Write the official file's values as the API does, so cleaning reads one coding."""
    fills = [
        pl.col(c).str.strip_chars().replace("", None).fill_null(str(code)).alias(c)
        for c, code in _NOT_APPLICABLE.items()
        if c in df.columns
    ]
    if "ADJINC" in df.columns:  # six implied decimals: 1019518 is a factor of 1.019518
        fills.append((pl.col("ADJINC").cast(pl.Float64) / 1_000_000).round(6).cast(pl.Utf8))
    return df.with_columns(fills) if fills else df


def read_pums(
    census_dir: str | Path,
    state_fips: str,
    pums_year: int,
    *,
    load_replicate_weights: bool = False,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Read the official PUMS household and person files, downloading any that are missing.

    Args:
        census_dir: The census folder; the files are in its ``pums_<year>``.
        state_fips: Two-digit state FIPS code.
        pums_year: ACS 1-year vintage.
        load_replicate_weights: Also keep ``WGTP1``-``WGTP80`` and
            ``PWGTP1``-``PWGTP80``, for MOE-based importance.

    Returns:
        ``(households, persons)`` with every record, typed; vacant homes and
        group quarters are dropped later by ``clean_pums``.

    Raises:
        ValueError: If a file lacks a variable the controls need, or fails the
            completeness or integrity checks.
    """
    hh_wanted = _HH_VARS | (_HH_REPLICATE_WEIGHTS if load_replicate_weights else set())
    per_wanted = _PERSON_VARS | (_PERSON_REPLICATE_WEIGHTS if load_replicate_weights else set())
    households, persons = pums_files(census_dir, state_fips, pums_year)
    frames = []
    tables = (("households", households, hh_wanted), ("persons", persons, per_wanted))
    for label, path, wanted in tables:
        _ensure_file(path, pums_year)
        df = _to_api_coding(_read_official(path, wanted))
        missing = sorted(wanted - set(df.columns))
        if missing:
            msg = f"PUMS {label} file {path} lacks {missing}"
            raise ValueError(msg)
        frames.append(_cast_pums_types(df, wanted))
    hh_df, person_df = frames
    check_pums_complete(hh_df, person_df)
    check_pums_integrity(hh_df, person_df)
    logger.info(
        "Read %d household and %d person records from %s and %s",
        hh_df.height,
        person_df.height,
        households,
        persons,
    )
    return hh_df, person_df


def _cast_pums_types(df: pl.DataFrame, expected_vars: set[str]) -> pl.DataFrame:
    """Cast PUMS columns from string to numeric types.

    Strict: a value that does not read as a number raises, naming the variable.
    Turning it into null instead is how ``ADJINC``, which the API sends as a
    decimal, once came back blank on every record while the fetch reported success.

    Raises:
        ValueError: If any value of an expected variable is not a number.
    """
    # String ID columns that should stay as strings
    string_cols = {"SERIALNO", "PUMA"}
    # Written with a fractional part; every other variable read is a whole number
    decimal_vars = {"HINCP", "PINCP", "PWGTP", "WGTP", "WKHP", "ADJINC"}

    casts = []
    for col_name in df.columns:
        if col_name in string_cols:
            casts.append(pl.col(col_name).cast(pl.Utf8))
        elif col_name in expected_vars:
            dtype = pl.Float64 if col_name in decimal_vars else pl.Int32
            text = pl.col(col_name).cast(pl.Utf8).str.strip_chars()
            unreadable = df.filter(text.is_not_null() & text.cast(dtype, strict=False).is_null())
            if unreadable.height:
                sample = unreadable[col_name].cast(pl.Utf8).unique().head(3).to_list()
                msg = (
                    f"PUMS variable {col_name}: {unreadable.height:,} value(s) do not read as "
                    f"{'numbers' if dtype == pl.Float64 else 'whole numbers'}, e.g. {sample}"
                )
                raise ValueError(msg)
            casts.append(text.cast(dtype))

    if casts:
        df = df.with_columns(casts)
    return df
