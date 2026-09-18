"""PUMS microdata I/O.

Downloads ACS PUMS 1-year microdata directly from the Census Bureau API or
loads from local CSV / Parquet files.  Handles type-casting of Census API
string responses to proper numeric dtypes.

# API behaviour

* All PUMAs batched in a single API request.
* Column chunking when >48 columns (API limit ~50), parallel via
  ``ThreadPoolExecutor``.
* JSON → Polars directly (no pandas intermediate).
* Streaming download with ``tqdm`` progress bars.
* Parquet caching at ``<cache_dir>/pums/{state}_{year}_{hh|person}.parquet``.

Transformation (recoding, aggregation) lives in ``control_data``.
"""

import difflib
import hashlib
import json
import logging
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import polars as pl
import requests
from tqdm import tqdm

from processing.weighting.controls.base import ControlLevel
from processing.weighting.controls.registry import pums_variables
from processing.weighting.core.specs import PUMSSource
from processing.weighting.data_prep.census_geo import _KEY_SIGNUP_URL, _census_api_key

logger = logging.getLogger(__name__)

# Infrastructure vars that don't come from any ControlTarget.
# Names use the **2020+** (canonical) convention; for older vintages the
# aliases in ``_PRE2020_ALIASES`` are requested instead and then renamed.
_HH_INFRA = {"SERIALNO", "PUMA", "STATE", "WGTP", "TYPEHUGQ"}
_PERSON_INFRA = {"SERIALNO", "SPORDER", "PUMA", "STATE", "PWGTP"}

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

# Census API settings
_CENSUS_BASE = "https://api.census.gov/data"
_MAX_COLS_PER_REQUEST = 48  # Census API caps ~50 variables per GET
_MAX_API_WORKERS = 4


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _apply_vintage_aliases(
    variables: list[str],
    available: set[str],
    pums_year: int,
) -> tuple[list[str], dict[str, str]]:
    """Swap canonical variable names for their pre-2020 equivalents when needed.

    Returns ``(api_vars, rename_map)`` where *api_vars* contains the names
    to actually request from the Census API and *rename_map* maps
    ``old_name -> canonical_name`` for post-fetch renaming.
    """
    if pums_year >= 2020 or not available:  # noqa: PLR2004
        return variables, {}

    rename_map: dict[str, str] = {}  # old -> canonical
    out: list[str] = []
    for var in variables:
        alias = _PRE2020_ALIASES.get(var)
        if alias and var not in available and alias in available:
            out.append(alias)
            rename_map[alias] = var
        else:
            out.append(var)
    if rename_map:
        logger.info(
            "Vintage aliases applied (pre-2020): %s",
            {v: k for k, v in rename_map.items()},
        )
    return out, rename_map


def _available_variables(base_url: str) -> set[str]:
    """Fetch the set of valid variable names from the Census API.

    Hits ``{base_url}/variables.json`` (typically <1 MB) and returns
    the set of variable names.  Falls back to an empty set on error
    (skipping validation rather than blocking the pipeline).
    """
    url = f"{base_url}/variables.json"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return set(data.get("variables", {}).keys())
    except Exception:  # noqa: BLE001
        logger.warning("Could not fetch variable list from %s — skipping validation", url)
        return set()


def _validate_variables(
    requested: list[str],
    available: set[str],
    label: str,
    *,
    optional: set[str] | None = None,
    aliases: dict[str, str] | None = None,
) -> list[str]:
    """Check *requested* variables against *available* from the Census API.

    If *available* is empty (pre-flight failed), returns *requested*
    unchanged as a fallback.

    Variables in *optional* (e.g. replicate weights) are silently dropped
    with a log message if missing.  Variables in *aliases* whose alias
    **is** present are accepted (they will be swapped later).  All other
    missing variables are **required** — the function raises ``ValueError``
    with the closest matching variable names as suggestions (likely a rename
    across Census vintages).
    """
    if not available:
        return requested

    optional = optional or set()
    aliases = aliases or {}
    valid: list[str] = []
    dropped_optional: list[str] = []
    missing_required: list[tuple[str, list[str]]] = []

    for var in requested:
        if var in available:
            valid.append(var)
        elif var in aliases and aliases[var] in available:
            # Known vintage alias — keep the canonical name; will be
            # swapped for the actual API name in _apply_vintage_aliases.
            valid.append(var)
        elif var in optional:
            dropped_optional.append(var)
        else:
            # Find similar names as suggestions
            similar = difflib.get_close_matches(var, sorted(available), n=5, cutoff=0.5)
            missing_required.append((var, similar))

    if dropped_optional:
        logger.info(
            "%s: %d optional variable(s) not in this vintage (dropped): %s",
            label,
            len(dropped_optional),
            dropped_optional[:10],
        )

    if missing_required:
        lines = []
        for var, similar in missing_required:
            hint = f"  similar: {similar}" if similar else "  (no similar names found)"
            lines.append(f"  - {var}\n{hint}")
        msg = (
            f"{label}: {len(missing_required)} required variable(s) not found in "
            f"this PUMS vintage.  This usually means the variable was renamed "
            f"across Census years.\n" + "\n".join(lines)
        )
        raise ValueError(msg)

    return valid


#: Columns identifying one row of each table. SERIALNO alone repeats once per
#: household member, so persons need SPORDER too.
_HH_KEYS = ("SERIALNO",)
_PERSON_KEYS = ("SERIALNO", "SPORDER")


#: A replicate weight is a perturbation of the full weight, so the two track each
#: other. Measured on clean 2023 California data every replicate correlates 0.77
#: with WGTP; misattributed ones sat at 0.20. This bound is deliberately loose
#: because the sound value is a property of the sample -- another state or
#: vintage need not give 0.77 -- so it is only asked to separate "tracks its
#: weight" from "does not". The sharp test is the agreement bound below.
_MIN_REPLICATE_CORRELATION = 0.5

#: Replicates of the same weight are built the same way, so they must correlate
#: with it to the same degree, whatever that degree is. Sound data agrees to
#: within 0.009 across all 80; a file whose replicates were partly misattributed
#: split into two populations 0.605 apart. Being relative, this needs no
#: assumption about the sound value, which is what makes it the stronger check.
#: It cannot stand alone: if *every* replicate were misattributed they would
#: agree with each other on being wrong, which is what the bound above catches.
_MAX_REPLICATE_CORRELATION_SPREAD = 0.1


def _replicate_weight_problems(
    df: pl.DataFrame, weight_col: str, replicates: set[str]
) -> list[str]:
    """Replicate weights that do not track their own full weight.

    Unlike the household-size check there is no cross-table fact to test these
    against, and a shuffled weight is still a plausible weight -- which is how
    65% of them stayed wrong across months of runs without anything noticing.
    Correlation with the full weight is the invariant that does hold.
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


def check_pums_integrity(hh_df: pl.DataFrame, person_df: pl.DataFrame) -> None:
    """Verify PUMS records are internally consistent, or raise.

    Fetching assembles each table from several API requests, so a record can end
    up carrying another record's values while still looking well formed -- right
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
        hh_df: PUMS household records, before group-quarters filtering.
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
            + "\n\nThis is a fetch or cache defect, not a data-quality question: "
            "these facts are true of the Census extract by construction. If a cache "
            "is in play, delete it and re-fetch."
        )
        raise ValueError(msg)


def _drop_gq(hh_df: pl.DataFrame, person_df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Drop group-quarters households (TYPEHUGQ != 1) and their persons."""
    if "TYPEHUGQ" not in hh_df.columns:
        return hh_df, person_df
    n_before = len(hh_df)
    hh_df = hh_df.filter(pl.col("TYPEHUGQ") == 1)
    n_dropped = n_before - len(hh_df)
    if n_dropped > 0:
        logger.info(
            "Dropped %d / %d group-quarters HH records (TYPEHUGQ != 1)", n_dropped, n_before
        )

    # Always filter persons to surviving HH serials (handles stale caches
    # where GQ households were already removed but GQ persons were not).
    hh_serials = hh_df["SERIALNO"]
    n_per_before = len(person_df)
    person_df = person_df.filter(pl.col("SERIALNO").is_in(hh_serials))
    n_per_dropped = n_per_before - len(person_df)
    if n_per_dropped > 0:
        logger.info(
            "Dropped %d / %d group-quarters person records",
            n_per_dropped,
            n_per_before,
        )
    return hh_df, person_df


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def _cache_paths(
    cache_dir: Path | None,
    source: "PUMSSource",
    hh_vars: list[str],
    person_vars: list[str],
) -> tuple[Path | None, Path | None]:
    """Cache file paths for this variable set, or ``(None, None)`` if uncached.

    The filename carries a hash of the requested variables so a query with
    replicate weights cannot be served the cache of one without them.
    """
    if cache_dir is None:
        return None, None
    var_key = f"{','.join(hh_vars)}|{','.join(person_vars)}"
    digest = hashlib.sha256(var_key.encode()).hexdigest()[:10]
    tag = f"{source.state_fips}_{source.pums_year}_{digest}"
    pums_dir = cache_dir / "pums"
    return pums_dir / f"{tag}_hh.parquet", pums_dir / f"{tag}_person.parquet"


def _load_cached_pums(
    hh_cache: Path | None, per_cache: Path | None
) -> tuple[pl.DataFrame, pl.DataFrame] | None:
    """Load and verify a cached PUMS pair, or ``None`` if there is no cache.

    The cache is verified on the way out, not just on the way in. It outlives
    the run that wrote it, so an unchecked one lets a single bad fetch be
    believed indefinitely -- which is exactly how misattributed replicate
    weights persisted here across months of runs.
    """
    if hh_cache is None or per_cache is None:
        return None
    if not (hh_cache.exists() and per_cache.exists()):
        return None
    hh_df = pl.read_parquet(hh_cache)
    person_df = pl.read_parquet(per_cache)
    logger.info("Loaded PUMS from cache (%d HH, %d persons)", len(hh_df), len(person_df))
    check_pums_integrity(hh_df, person_df)
    return hh_df, person_df


def _fetch_verified_pums(
    base_url: str,
    hh_vars: list[str],
    person_vars: list[str],
    source: "PUMSSource",
    puma_geo: str,
    *,
    hh_rename: dict[str, str],
    per_rename: dict[str, str],
    hh_expected: set[str],
    person_expected: set[str],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Fetch both tables and return them only once they verify, retrying if not.

    The keyed join guarantees the chunks agree with each other, but it cannot
    see a chunk whose own response is internally wrong -- correct SERIALNOs with
    values shuffled beneath them. Joining on the key then propagates that
    faithfully, and the tables are self-consistent right up until
    :func:`check_pums_integrity` compares facts across them.

    So integrity is a retry condition, not just a failure. The fault is
    transient: the same request returns sound data on a later attempt. Without
    this the check turns an intermittent corruption into an intermittent
    outage, which is what it did the first time it fired in a real run.

    Raises:
        ValueError: If the data is still inconsistent after
            :data:`_MAX_FETCH_ATTEMPTS` whole-fetch attempts, which means the
            fault is not the transient one a retry clears.
    """
    last_error: ValueError | None = None
    for attempt in range(1, _MAX_FETCH_ATTEMPTS + 1):
        hh_df = _fetch_table(
            base_url, hh_vars, source.state_fips, puma_geo, label="households", keys=_HH_KEYS
        )
        person_df = _fetch_table(
            base_url, person_vars, source.state_fips, puma_geo, label="persons", keys=_PERSON_KEYS
        )
        # Canonical names first, so the checks below can name real columns.
        if hh_rename:
            hh_df = hh_df.rename(hh_rename)
        if per_rename:
            person_df = person_df.rename(per_rename)
        hh_df = _cast_pums_types(hh_df, hh_expected)
        person_df = _cast_pums_types(person_df, person_expected)

        try:
            check_pums_integrity(hh_df, person_df)
        except ValueError as err:
            last_error = err
            logger.warning(
                "PUMS failed its integrity check on attempt %d/%d, re-fetching. %s",
                attempt,
                _MAX_FETCH_ATTEMPTS,
                err,
            )
            continue
        if attempt > 1:
            logger.info("PUMS verified on attempt %d", attempt)
        return hh_df, person_df

    msg = (
        f"PUMS failed its integrity check on {_MAX_FETCH_ATTEMPTS} independent fetches, "
        f"so this is not the transient fault a retry clears.\n\n{last_error}"
    )
    raise ValueError(msg) from last_error


def fetch_pums_data(
    source: PUMSSource,
    extra_hh_vars: set[str] | None = None,
    extra_person_vars: set[str] | None = None,
    load_replicate_weights: bool = False,
    cache_dir: Path | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Download PUMS household and person microdata from the Census API.

    Args:
        source: State, year, and optional PUMA filter.
        extra_hh_vars: Additional household PUMS variable names to fetch
            beyond the defaults.
        extra_person_vars: Additional person PUMS variable names to fetch
            beyond the defaults.
        load_replicate_weights: If ``True``, also fetch the 80 replicate
            weight columns per table (``WGTP1`` to ``WGTP80`` and
            ``PWGTP1`` to ``PWGTP80``).  Required for MOE-based importance
            calculation.
        cache_dir: If set, raw PUMS data is cached as Parquet files under
            ``cache_dir/pums/``.  Subsequent calls with the same state/year
            load from cache instead of hitting the API.

    Returns:
        Tuple of ``(households, persons)`` Polars DataFrames with PUMS
        data typed to appropriate dtypes.
    """
    hh_extra = extra_hh_vars or set()
    person_extra = extra_person_vars or set()
    if load_replicate_weights:
        hh_extra = hh_extra | _HH_REPLICATE_WEIGHTS
        person_extra = person_extra | _PERSON_REPLICATE_WEIGHTS
    hh_vars = sorted(_HH_VARS | hh_extra)
    person_vars = sorted(_PERSON_VARS | person_extra)

    # Check cache first — filename includes a hash of the requested
    # variable set so different queries (e.g. with/without replicate
    # weights) get independent cache entries.
    hh_cache, per_cache = _cache_paths(cache_dir, source, hh_vars, person_vars)
    cached = _load_cached_pums(hh_cache, per_cache)
    if cached is not None:
        return _drop_gq(*cached)

    dataset_name = f"ACSPUMS1Y{source.pums_year}"
    base_url = f"{_CENSUS_BASE}/{source.pums_year}/acs/acs1/pums"
    puma_geo = ",".join(source.puma_ids) if source.puma_ids else "*"
    if source.puma_ids and len(puma_geo) >= _MAX_COLS_PER_REQUEST:
        puma_label = f"{len(source.puma_ids)} PUMAs"
    else:
        puma_label = puma_geo
    logger.info(
        "Fetching PUMS from Census API: %s (PUMAs: %s)",
        dataset_name,
        puma_label,
    )

    # Pre-flight: check which variables the API actually exposes for
    # this year.  Raises ValueError for missing control variables (likely
    # renamed across Census vintages) with similar-name suggestions.
    # Replicate weights are optional — silently dropped if absent.
    api_vars = _available_variables(base_url)
    optional_vars = _HH_REPLICATE_WEIGHTS | _PERSON_REPLICATE_WEIGHTS
    aliases = _PRE2020_ALIASES if source.pums_year < 2020 else {}  # noqa: PLR2004
    hh_vars = _validate_variables(
        hh_vars,
        api_vars,
        "households",
        optional=optional_vars,
        aliases=aliases,
    )
    person_vars = _validate_variables(
        person_vars,
        api_vars,
        "persons",
        optional=optional_vars,
        aliases=aliases,
    )

    # Swap canonical 2020+ names for their pre-2020 equivalents when needed.
    hh_vars, hh_rename = _apply_vintage_aliases(hh_vars, api_vars, source.pums_year)
    person_vars, per_rename = _apply_vintage_aliases(person_vars, api_vars, source.pums_year)

    hh_df, person_df = _fetch_verified_pums(
        base_url,
        hh_vars,
        person_vars,
        source,
        puma_geo,
        hh_rename=hh_rename,
        per_rename=per_rename,
        hh_expected=_HH_VARS | hh_extra,
        person_expected=_PERSON_VARS | person_extra,
    )

    # Save raw data to cache (before any filtering)
    if cache_dir is not None:
        hh_cache.parent.mkdir(parents=True, exist_ok=True)
        hh_df.write_parquet(hh_cache)
        person_df.write_parquet(per_cache)
        logger.info("Cached PUMS data to %s", hh_cache.parent)

    hh_df, person_df = _drop_gq(hh_df, person_df)

    logger.info(
        "Fetched %d household records, %d person records",
        len(hh_df),
        len(person_df),
    )
    return hh_df, person_df


def load_pums_from_files(
    hh_path: str,
    person_path: str,
    state_fips: str | None = None,
    puma_ids: list[str] | None = None,
    load_replicate_weights: bool = False,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Load PUMS data from local CSV/Parquet files.

    Args:
        hh_path: Path to household PUMS file (CSV or Parquet).
        person_path: Path to person PUMS file (CSV or Parquet).
        state_fips: Optional filter to a specific state FIPS code.
        puma_ids: Optional filter to specific PUMAs.
        load_replicate_weights: If ``True``, retain ``WGTP1`` to ``WGTP80``
            and ``PWGTP1`` to ``PWGTP80`` replicate weight columns.

    Returns:
        Tuple of ``(households, persons)`` Polars DataFrames.
    """
    hh_ext = hh_path.rsplit(".", 1)[-1].lower()
    person_ext = person_path.rsplit(".", 1)[-1].lower()

    if hh_ext == "parquet":
        hh_df = pl.read_parquet(hh_path)
    else:
        hh_df = pl.read_csv(hh_path, infer_schema_length=10_000)

    if person_ext == "parquet":
        person_df = pl.read_parquet(person_path)
    else:
        person_df = pl.read_csv(person_path, infer_schema_length=10_000)

    # Determine expected vars for type casting
    hh_known = set(hh_df.columns) & _HH_VARS
    person_known = set(person_df.columns) & _PERSON_VARS
    if load_replicate_weights:
        hh_known |= set(hh_df.columns) & _HH_REPLICATE_WEIGHTS
        person_known |= set(person_df.columns) & _PERSON_REPLICATE_WEIGHTS

    # Cast types
    hh_df = _cast_pums_types(hh_df, hh_known)
    person_df = _cast_pums_types(person_df, person_known)

    hh_df, person_df = _drop_gq(hh_df, person_df)

    if state_fips is not None and "ST" in hh_df.columns:
        hh_df = hh_df.filter(pl.col("ST") == state_fips)
        person_df = person_df.filter(pl.col("ST") == state_fips)

    if puma_ids is not None and "PUMA" in hh_df.columns:
        hh_df = hh_df.filter(pl.col("PUMA").is_in(puma_ids))
        person_df = person_df.filter(pl.col("PUMA").is_in(puma_ids))

    logger.info(
        "Loaded %d household records, %d person records from files",
        len(hh_df),
        len(person_df),
    )
    return hh_df, person_df


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


#: A response that fails its own integrity checks is retried this many times
#: before giving up. The corruption seen here is intermittent -- the same request
#: returns sound data on a later attempt -- so a retry turns a failed run into a
#: transparent recovery. It is deliberately small: repeated failure means
#: something systematic, and looping would only delay saying so.
_MAX_FETCH_ATTEMPTS = 4


def _census_get(
    base_url: str,
    cols: list[str],
    state_fips: str,
    puma_geo: str,
    *,
    label: str = "",
    keys: tuple[str, ...] = (),
) -> list[list[str]]:
    """Fetch one Census API request, verify it, and retry it if it is unsound.

    A response is only returned once it is rectangular, carries every requested
    column, and -- when *keys* is given -- identifies each row uniquely. The API
    intermittently returns responses that fail those checks, and the failure is
    not visible in the data afterwards: values are shifted between columns or
    rows are duplicated, all of them plausible. Retrying gets sound data.

    Args:
        base_url: Census API endpoint for the dataset and vintage.
        cols: Variables to request.
        state_fips: State to fetch, e.g. ``"06"``.
        puma_geo: PUMA filter, or ``"*"`` for all.
        label: Progress-bar and log label.
        keys: Columns that must uniquely identify a row of this response. Empty
            skips that check, for requests that do not carry a full key.

    Raises:
        RuntimeError: On HTTP or API errors, or if the response is still unsound
            after :data:`_MAX_FETCH_ATTEMPTS` attempts.
    """
    last_error: RuntimeError | None = None
    for attempt in range(1, _MAX_FETCH_ATTEMPTS + 1):
        rows = _census_get_once(base_url, cols, state_fips, puma_geo, label=label)
        try:
            _check_response_shape(rows, cols, base_url)
            _check_response_keys(rows, keys, base_url)
        except RuntimeError as err:
            last_error = err
            logger.warning(
                "%s: response failed its integrity check on attempt %d/%d, retrying. %s",
                label or "Census API",
                attempt,
                _MAX_FETCH_ATTEMPTS,
                err,
            )
            continue
        return rows

    msg = (
        f"Census API returned an unsound response {_MAX_FETCH_ATTEMPTS} times running, "
        f"so this is not the intermittent fault a retry clears.\n\n{last_error}"
    )
    raise RuntimeError(msg) from last_error


def _check_response_keys(rows: list[list[str]], keys: tuple[str, ...], url: str) -> None:
    """Verify *keys* identify each row of the response uniquely.

    A duplicated key is proof the response is wrong -- PUMS assigns one row per
    household, and one per person within a household -- and it would also make
    the chunk unjoinable. Seen live: a person chunk came back with 6 duplicate
    (SERIALNO, SPORDER) pairs where a narrow request for the same columns had
    none.

    Raises:
        RuntimeError: If any key value repeats.
    """
    if not keys or not rows:
        return
    header = rows[0]
    try:
        positions = [header.index(k) for k in keys]
    except ValueError:
        return
    seen: set[tuple[str, ...]] = set()
    duplicates: list[tuple[str, ...]] = []
    for row in rows[1:]:
        value = tuple(row[i] for i in positions)
        if value in seen:
            duplicates.append(value)
        seen.add(value)
    if duplicates:
        shown = ", ".join("/".join(d) for d in duplicates[:5])
        msg = (
            f"Census API returned {len(duplicates):,} duplicate "
            f"{', '.join(keys)} value(s), which PUMS does not contain: {shown}.\n"
            f"  URL: {url}"
        )
        raise RuntimeError(msg)


def _census_get_once(
    base_url: str,
    cols: list[str],
    state_fips: str,
    puma_geo: str,
    *,
    label: str = "",
) -> list[list[str]]:
    """Execute a single Census API GET and return the JSON rows.

    Streams the response with a ``tqdm`` progress bar when *label* is
    provided.  Raises ``RuntimeError`` on HTTP or API errors.
    """
    params = {
        "get": ",".join(cols),
        "for": f"public use microdata area:{puma_geo}",
        "in": f"state:{state_fips}",
        "key": _census_api_key(),
    }
    resp = requests.get(base_url, params=params, timeout=120, stream=True)
    resp.raise_for_status()

    # A missing/invalid key redirects (HTTP 200) to an HTML "Missing Key" page,
    # so raise_for_status() passes but the body is not JSON.  Fail clearly.
    if "missing_key" in resp.url:
        msg = (
            "Census API request failed: invalid or missing API key.\n"
            f"  URL: {resp.url}\n"
            f"  Ensure CENSUS_KEY is set to a valid key (sign up: {_KEY_SIGNUP_URL})."
        )
        raise RuntimeError(msg)

    total = int(resp.headers.get("content-length", 0))
    chunks: list[bytes] = []
    with tqdm(
        total=total or None,
        unit="B",
        unit_scale=True,
        desc=label or "Census API",
        leave=False,
    ) as bar:
        for chunk in resp.iter_content(chunk_size=64 * 1024):
            chunks.append(chunk)
            bar.update(len(chunk))

    body = b"".join(chunks)
    if total and len(body) != total:
        msg = (
            f"Census API response was truncated: received {len(body):,} of {total:,} "
            f"bytes.\n  URL: {resp.url}\n"
            "A short body can still parse as JSON if it ends on a row boundary, so "
            "this is checked rather than inferred."
        )
        raise RuntimeError(msg)
    try:
        data = json.loads(body)
    except json.JSONDecodeError as e:
        msg = (
            "Census API returned a body that could not be parsed as JSON "
            f"(likely an error page).\n"
            f"  URL: {resp.url}\n"
            f"  First bytes: {body[:200]!r}\n"
            f"  Ensure CENSUS_KEY is set to a valid key (sign up: {_KEY_SIGNUP_URL})."
        )
        raise RuntimeError(msg) from e
    if isinstance(data, dict) and "error" in data:
        msg = f"Census API error: {data['error']}"
        raise RuntimeError(msg)
    return data  # pyright: ignore[reportReturnType]


def _check_response_shape(rows: list[list[str]], requested: list[str], url: str) -> None:
    """Verify the response is rectangular and carries what was asked for.

    Two failures this catches, both of which produce wrong values rather than an
    error. A row with the wrong number of fields shifts every value after the
    defect into its neighbour's column, so a code lands where a count belongs --
    the shape of corruption seen here as TYPEHUGQ holding 12 and 3213 where the
    API itself returns 2 and 3. And a silently omitted column makes the frame
    narrower than requested, which later positional work would misread.

    Raises:
        RuntimeError: If the header is missing, a row is not the header's width,
            or a requested column is absent.
    """
    if not rows:
        msg = f"Census API returned no rows at all.\n  URL: {url}"
        raise RuntimeError(msg)

    header = rows[0]
    width = len(header)
    ragged = [(i, len(row)) for i, row in enumerate(rows[1:], start=1) if len(row) != width]
    if ragged:
        shown = ", ".join(f"row {i} has {n}" for i, n in ragged[:5])
        msg = (
            f"Census API returned {len(ragged):,} row(s) that are not {width} fields "
            f"wide, so values are shifted between columns: {shown}.\n"
            f"  URL: {url}"
        )
        raise RuntimeError(msg)

    missing = [c for c in requested if c not in header]
    if missing:
        msg = f"Census API response omits requested column(s): {', '.join(missing)}.\n  URL: {url}"
        raise RuntimeError(msg)


def _json_to_polars(rows: list[list[str]]) -> pl.DataFrame:
    """Convert Census API JSON (header + data rows) to a Polars DataFrame.

    Shape is verified by :func:`_check_response_shape` before this runs, so the
    positional read below is safe.
    """
    header = rows[0]
    return pl.DataFrame(
        {col: [row[i] for row in rows[1:]] for i, col in enumerate(header)},
        schema=dict.fromkeys(header, pl.Utf8),
    )


def _fetch_table(
    base_url: str,
    all_cols: list[str],
    state_fips: str,
    puma_geo: str,
    *,
    label: str = "table",
    keys: tuple[str, ...] = ("SERIALNO",),
) -> pl.DataFrame:
    """Fetch a full PUMS table, chunking columns if needed.

    The Census API limits ~50 variables per request. When *all_cols* exceeds
    that, the request is split into chunks and fetched in parallel, then joined
    back together **on *keys***.

    The join is what makes this safe, and it has to be a join. The API does not
    return a stable row order across requests: two 48-column requests for the
    same 167,075 California households were measured returning the same rows in
    orders differing at 110,255 positions. Stacking the chunks side by side by
    position -- which this did, while discarding each chunk's own copy of the
    key -- therefore attached most values to the wrong record, and left every
    row carrying exactly one key inherited from the first chunk, so the result
    looked well formed. It silently scrambled 65% of the replicate weights on
    every fetch that chunked.

    *keys* must identify a row uniquely and is added to every chunk request:
    ``SERIALNO`` for households, ``(SERIALNO, SPORDER)`` for persons, where
    ``SERIALNO`` alone repeats once per household member.

    Raises:
        ValueError: If a chunk does not carry exactly the rows of the first, or
            if *keys* does not uniquely identify a row -- either would mean the
            join silently dropped or multiplied records.
    """
    if len(all_cols) <= _MAX_COLS_PER_REQUEST:
        rows = _census_get(base_url, all_cols, state_fips, puma_geo, label=label, keys=keys)
        df = _json_to_polars(rows)
        logger.info("  %s: %d rows x %d cols", label, len(df), len(df.columns))
        return df

    # Split into chunks, each including every join key.
    # Use smaller chunks to spread work across _MAX_API_WORKERS threads,
    # while still respecting the Census API column limit.
    missing_keys = [k for k in keys if k not in all_cols]
    if missing_keys:
        msg = (
            f"{label}: cannot chunk without {', '.join(missing_keys)} in the requested "
            f"columns -- the chunks are joined on {', '.join(keys)}, so every chunk has "
            "to carry it."
        )
        raise ValueError(msg)

    non_key = [c for c in all_cols if c not in keys]
    cols_per_chunk = min(
        math.ceil(len(non_key) / _MAX_API_WORKERS),
        _MAX_COLS_PER_REQUEST - len(keys),  # leave room for the join keys
    )
    chunks: list[list[str]] = []
    for i in range(0, len(non_key), cols_per_chunk):
        chunk = [*keys, *non_key[i : i + cols_per_chunk]]
        chunks.append(chunk)

    n_chunks = len(chunks)
    logger.info(
        "  %s: %d cols across %d requests",
        label,
        len(all_cols),
        n_chunks,
    )

    # Fetch chunks in parallel
    parts: list[pl.DataFrame] = [None] * n_chunks  # type: ignore[list-item]
    with ThreadPoolExecutor(max_workers=min(_MAX_API_WORKERS, n_chunks)) as pool:
        futures = {
            pool.submit(
                _census_get,
                base_url,
                chunk,
                state_fips,
                puma_geo,
                label=f"{label} [{i + 1}/{n_chunks}]",
                keys=keys,
            ): i
            for i, chunk in enumerate(chunks)
        }
        for future in as_completed(futures):
            idx = futures[future]
            parts[idx] = _json_to_polars(future.result())

    # Join chunks on their keys (first chunk is the base, others add columns).
    result = parts[0]
    n_rows = result.height
    key_list = list(keys)
    if result.select(key_list).n_unique() != n_rows:
        msg = (
            f"{label}: {', '.join(keys)} does not uniquely identify a row "
            f"({result.select(key_list).n_unique():,} distinct of {n_rows:,}), so joining "
            "the column chunks on it would multiply records. Add the missing key column."
        )
        raise ValueError(msg)

    for i, part in enumerate(parts[1:], start=1):
        new_cols = [c for c in part.columns if c not in result.columns]
        result = result.join(part.select([*key_list, *new_cols]), on=key_list, how="left")
        if result.height != n_rows:
            msg = (
                f"{label}: joining chunk {i} changed the row count "
                f"({n_rows:,} -> {result.height:,}). The chunk did not carry the same "
                "records as the first one."
            )
            raise ValueError(msg)
        unmatched = result.select(pl.col(new_cols[0]).null_count()).item() if new_cols else 0
        if unmatched:
            msg = (
                f"{label}: {unmatched:,} of {n_rows:,} records found no match in chunk {i}. "
                "The chunks disagree about which records exist."
            )
            raise ValueError(msg)

    logger.info("  %s: %d rows x %d cols", label, len(result), len(result.columns))
    return result


def _cast_pums_types(df: pl.DataFrame, expected_vars: set[str]) -> pl.DataFrame:
    """Cast PUMS columns from string (Census API) to numeric types."""
    # String ID columns that should stay as strings
    string_cols = {"SERIALNO", "PUMA", "ST"}

    numeric_casts = []
    for col_name in df.columns:
        if col_name in string_cols:
            numeric_casts.append(pl.col(col_name).cast(pl.Utf8))
        elif col_name in expected_vars:
            # Try int first, fall back to float for income-like fields
            if col_name in ("HINCP", "PWGTP", "WGTP", "WKHP"):
                numeric_casts.append(
                    pl.col(col_name).cast(pl.Utf8).str.strip_chars().cast(pl.Float64, strict=False)
                )
            else:
                numeric_casts.append(
                    pl.col(col_name).cast(pl.Utf8).str.strip_chars().cast(pl.Int32, strict=False)
                )

    if numeric_casts:
        df = df.with_columns(numeric_casts)
    return df
