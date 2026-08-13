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
    if cache_dir is not None:
        pums_dir = cache_dir / "pums"
        var_key = f"{','.join(hh_vars)}|{','.join(person_vars)}"
        digest = hashlib.sha256(var_key.encode()).hexdigest()[:10]
        tag = f"{source.state_fips}_{source.pums_year}_{digest}"
        hh_cache = pums_dir / f"{tag}_hh.parquet"
        per_cache = pums_dir / f"{tag}_person.parquet"
        if hh_cache.exists() and per_cache.exists():
            hh_df = pl.read_parquet(hh_cache)
            person_df = pl.read_parquet(per_cache)
            logger.info(
                "Loaded PUMS from cache (%d HH, %d persons)",
                len(hh_df),
                len(person_df),
            )
            hh_df, person_df = _drop_gq(hh_df, person_df)
            return hh_df, person_df

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

    hh_df = _fetch_table(base_url, hh_vars, source.state_fips, puma_geo, label="households")
    person_df = _fetch_table(base_url, person_vars, source.state_fips, puma_geo, label="persons")

    # Rename vintage-specific columns back to canonical names so all
    # downstream code uses a single set of column names.
    if hh_rename:
        hh_df = hh_df.rename(hh_rename)
    if per_rename:
        person_df = person_df.rename(per_rename)

    # Cast types
    hh_df = _cast_pums_types(hh_df, _HH_VARS | hh_extra)
    person_df = _cast_pums_types(person_df, _PERSON_VARS | person_extra)

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


def _census_get(
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


def _json_to_polars(rows: list[list[str]]) -> pl.DataFrame:
    """Convert Census API JSON (header + data rows) to a Polars DataFrame."""
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
) -> pl.DataFrame:
    """Fetch a full PUMS table, chunking columns if needed.

    The Census API limits ~50 variables per request.  When *all_cols*
    exceeds that, we split into chunks (each including ``SERIALNO`` as a
    join key) and fetch them in parallel, then join horizontally.
    """
    join_key = "SERIALNO"

    if len(all_cols) <= _MAX_COLS_PER_REQUEST:
        rows = _census_get(base_url, all_cols, state_fips, puma_geo, label=label)
        df = _json_to_polars(rows)
        logger.info("  %s: %d rows x %d cols", label, len(df), len(df.columns))
        return df

    # Split into chunks, each including the join key.
    # Use smaller chunks to spread work across _MAX_API_WORKERS threads,
    # while still respecting the Census API column limit.
    non_key = [c for c in all_cols if c != join_key]
    cols_per_chunk = min(
        math.ceil(len(non_key) / _MAX_API_WORKERS),
        _MAX_COLS_PER_REQUEST - 1,  # leave room for the join key
    )
    chunks: list[list[str]] = []
    for i in range(0, len(non_key), cols_per_chunk):
        chunk = [join_key, *non_key[i : i + cols_per_chunk]]
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
            ): i
            for i, chunk in enumerate(chunks)
        }
        for future in as_completed(futures):
            idx = futures[future]
            parts[idx] = _json_to_polars(future.result())

    # Join chunks on SERIALNO (first chunk is base, others add columns)
    result = parts[0]
    for part in parts[1:]:
        new_cols = [c for c in part.columns if c not in result.columns]
        result = result.hstack(part.select(new_cols))

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
