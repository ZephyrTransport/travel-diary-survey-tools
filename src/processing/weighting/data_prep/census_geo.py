"""Census geography and block counts for the weighting.

Each input is a file in the census folder, named from the state and year:

``census_<vintage>/tl_<year>_<st>_puma<vv>.parquet``
    PUMA boundaries, from the TIGER release of the PUMS year (via pygris).
``census_<vintage>/tl_<vintage>_<st>_tabblock<vv>.parquet``
    Block boundaries (via pygris).
``census_<vintage>/block_population_<st>.parquet``, ``block_households_<st>.parquet``,
``county_names_<st>.parquet``
    From the decennial census API (PL 94-171 for 2020, SF1 for 2010).

The vintage is the decennial census the PUMS year's PUMAs are drawn on (2010
for 2012-2021, 2020 for 2022 on). A file missing from the folder is downloaded
there, and from then on every run reads it as it is.
"""

import logging
import os
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import geopandas as gpd
import pandas as pd
import polars as pl
import pygris
import requests
from tqdm import tqdm

logger = logging.getLogger(__name__)

_KEY_SIGNUP_URL = "https://api.census.gov/data/key_signup.html"

# Column names per decennial vintage.
_PUMA_ID = {2020: "PUMACE20", 2010: "PUMACE10"}
_BLOCK_ID = {2020: "GEOID20", 2010: "GEOID10"}

# Decennial census API endpoint per vintage, and its block variables.
_CENSUS_API = {
    2020: "https://api.census.gov/data/2020/dec/pl",
    2010: "https://api.census.gov/data/2010/dec/sf1",
}
_BLOCK_POP_VAR = {2020: "P1_001N", 2010: "P001001"}
_BLOCK_HOUSEHOLDS_VAR = {2020: "H1_002N", 2010: "H003002"}  # occupied housing units

#: Attempts per API request. The Census API returns 5xx under load, and a 2010
#: fetch makes one request per county; the delay grows with each attempt.
_MAX_CENSUS_ATTEMPTS = 4
_RETRY_BACKOFF_SECONDS = 2.0

#: Server-side statuses worth retrying. A 4xx is our request being wrong.
_TRANSIENT_STATUS = frozenset({500, 502, 503, 504})

_COUNTY_WORKERS = 8


def puma_vintage_for_pums_year(pums_year: int) -> int:
    """Return the PUMA geography vintage for a given PUMS year.

    ACS PUMS 2022+ uses 2020 PUMAs; 2012-2021 uses 2010 PUMAs.
    """
    if pums_year >= 2022:  # noqa: PLR2004
        return 2020
    if pums_year >= 2012:  # noqa: PLR2004
        return 2010
    msg = f"PUMS year {pums_year} is before 2012; not supported."
    raise ValueError(msg)


# ---------------------------------------------------------------------------
# Files in the census folder
# ---------------------------------------------------------------------------


def _vintage_dir(census_dir: Path, pums_year: int) -> tuple[Path, int]:
    vintage = puma_vintage_for_pums_year(pums_year)
    return Path(census_dir) / f"census_{vintage}", vintage


def _ensure(path: Path, fetch: Callable[[], Any]) -> None:
    """Write *fetch()* to *path* when it is not there yet.

    Written under a temporary name and then renamed, so an interrupted
    download never leaves a file that a later run would read as complete.
    """
    if path.exists():
        return
    logger.info("Not found, downloading: %s", path)
    data = fetch()
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(path.name + ".part")
    if isinstance(data, pl.DataFrame):
        data.write_parquet(partial)
    else:
        data.to_parquet(partial)
    partial.replace(path)


# ---------------------------------------------------------------------------
# Census API
# ---------------------------------------------------------------------------


def _census_api_key() -> str:
    """Return the Census data-API key from the environment.

    A keyless request is redirected to an HTML "Missing Key" page (HTTP 200).
    Sign up (free, instant) at ``_KEY_SIGNUP_URL`` and set ``CENSUS_KEY`` (or
    ``CENSUS_API_KEY``) in the environment or a ``.env`` file.
    """
    key = os.environ.get("CENSUS_KEY") or os.environ.get("CENSUS_API_KEY")
    if not key:
        msg = (
            "Census API key not found. The Census data API requires a key for "
            "every request.\n"
            f"  1. Sign up (free, instant): {_KEY_SIGNUP_URL}\n"
            "  2. Set CENSUS_KEY in your environment or .env file "
            "(the runner scripts call load_dotenv())."
        )
        raise RuntimeError(msg)
    return key


def _census_json(resp: requests.Response) -> Any:  # noqa: ANN401
    """Return parsed JSON from a Census API response, or a clear error.

    A missing or invalid key returns an HTML page with HTTP 200, so
    ``raise_for_status()`` passes but ``resp.json()`` fails opaquely.
    """
    resp.raise_for_status()
    content_type = resp.headers.get("Content-Type", "")
    if "missing_key" in resp.url or "json" not in content_type.lower():
        reason = (
            "invalid or missing API key"
            if "missing_key" in resp.url
            else f"non-JSON response ({content_type or 'unknown content-type'})"
        )
        msg = (
            f"Census API request failed: {reason}.\n"
            f"  URL: {resp.url}\n"
            f"  Ensure CENSUS_KEY is set to a valid key (sign up: {_KEY_SIGNUP_URL})."
        )
        raise RuntimeError(msg)
    try:
        return resp.json()
    except ValueError as e:  # requests raises a JSONDecodeError subclass of ValueError
        msg = (
            f"Census API returned a body that could not be parsed as JSON.\n"
            f"  URL: {resp.url}\n"
            f"  Parse error: {e}"
        )
        raise RuntimeError(msg) from e


def _is_transient(err: Exception) -> bool:
    """Whether *err* is a server-side failure that a later attempt may clear."""
    if isinstance(err, requests.HTTPError) and err.response is not None:
        return err.response.status_code in _TRANSIENT_STATUS
    return isinstance(err, requests.ConnectionError | requests.Timeout)


def _census_get(url: str, params: dict[str, str], timeout: int) -> Any:  # noqa: ANN401
    """GET one Census API request, retrying server failures.

    Raises:
        requests.RequestException: If the request fails for a reason a retry
            cannot clear, or still fails after :data:`_MAX_CENSUS_ATTEMPTS`.
    """
    params = {**params, "key": _census_api_key()}
    for attempt in range(1, _MAX_CENSUS_ATTEMPTS + 1):
        try:
            return _census_json(requests.get(url, params=params, timeout=timeout))
        except Exception as err:
            if not _is_transient(err) or attempt == _MAX_CENSUS_ATTEMPTS:
                raise
            delay = _RETRY_BACKOFF_SECONDS * attempt
            logger.warning(
                "Census API request for %s failed (attempt %d/%d): %s. Retrying in %.0fs.",
                params["in"],
                attempt,
                _MAX_CENSUS_ATTEMPTS,
                err,
                delay,
            )
            time.sleep(delay)
    msg = "Census API: exhausted retries"  # pragma: no cover - loop always returns/raises
    raise RuntimeError(msg)


def _parse_block_response(data: list[list[str]], variable: str) -> dict[str, int]:
    """Parse a Census API JSON response into ``{geoid: count}``."""
    header, *rows = data
    value_i = header.index(variable)
    st_i, co_i, tr_i, bl_i = (header.index(c) for c in ("state", "county", "tract", "block"))
    return {r[st_i] + r[co_i] + r[tr_i] + r[bl_i]: int(r[value_i]) for r in rows}


def _county_names(state_fips: str, vintage: int) -> pl.DataFrame:
    header, *rows = _census_get(
        _CENSUS_API[vintage],
        {"get": "NAME", "for": "county:*", "in": f"state:{state_fips}"},
        timeout=60,
    )
    name_i, st_i, co_i = (header.index(c) for c in ("NAME", "state", "county"))
    # The API names a county with its state, as in "Alameda County, California"
    return pl.DataFrame(
        {
            "county_fips": [r[st_i] + r[co_i] for r in rows],
            "county_name": [r[name_i].split(",")[0] for r in rows],
        }
    ).sort("county_fips")


def _fetch_block_counts(state_fips: str, vintage: int, variable: str, column: str) -> pl.DataFrame:
    """Fetch one count per Census block from the decennial census API.

    The 2020 PL endpoint returns a whole state in one request. The 2010 SF1
    endpoint does not, so 2010 is fetched one county at a time.

    Returns ``[block_id, <column>]`` for every block in *state_fips*.

    Raises:
        ValueError: If the API returns no blocks.
    """
    url = _CENSUS_API[vintage]
    logger.info("Fetching %d block %s for state %s", vintage, variable, state_fips)
    if vintage >= 2020:  # noqa: PLR2004
        counts = _parse_block_response(
            _census_get(
                url,
                {"get": variable, "for": "block:*", "in": f"state:{state_fips} county:* tract:*"},
                timeout=300,
            ),
            variable,
        )
    else:
        counties = [c[2:] for c in _county_names(state_fips, vintage)["county_fips"]]
        counts = {}
        with ThreadPoolExecutor(max_workers=_COUNTY_WORKERS) as pool:
            futures = [
                pool.submit(
                    _census_get,
                    url,
                    {"get": variable, "for": "block:*", "in": f"state:{state_fips} county:{c}"},
                    120,
                )
                for c in counties
            ]
            for future in tqdm(as_completed(futures), total=len(futures), unit="county"):
                counts.update(_parse_block_response(future.result(), variable))

    if not counts:
        msg = f"The Census API returned no {variable} blocks for state {state_fips} ({url})."
        raise ValueError(msg)
    logger.info("Fetched %s for %d blocks", variable, len(counts))
    return pl.DataFrame({"block_id": list(counts), column: list(counts.values())}).sort("block_id")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_puma_gdf(state_fips: str, pums_year: int, census_dir: Path) -> gpd.GeoDataFrame:
    """PUMA polygons for *state_fips*, from the TIGER release of *pums_year*.

    Returns a GeoDataFrame with ``puma_id`` (str) and ``geometry``.
    """
    folder, vintage = _vintage_dir(census_dir, pums_year)
    path = folder / f"tl_{pums_year}_{state_fips}_puma{str(vintage)[2:]}.parquet"
    _ensure(path, lambda: pygris.pumas(state=state_fips, year=pums_year))
    id_col = _PUMA_ID[vintage]
    gdf = gpd.read_parquet(path, columns=[id_col, "geometry"])
    return gdf.rename(columns={id_col: "puma_id"}).assign(
        puma_id=lambda df: df["puma_id"].astype(str)
    )


def get_block_gdf(state_fips: str, pums_year: int, census_dir: Path) -> gpd.GeoDataFrame:
    """Block polygons with their decennial population, for dasymetric allocation.

    Uses the same vintage as the PUMAs, so block GEOIDs match the sample plan.

    Returns a GeoDataFrame with ``block_id`` (str), ``block_pop`` (int), and
    ``geometry``.

    Raises:
        ValueError: If a block has no population row, which means the two files
            are from different vintages or one is damaged.
    """
    folder, vintage = _vintage_dir(census_dir, pums_year)
    blocks_path = folder / f"tl_{vintage}_{state_fips}_tabblock{str(vintage)[2:]}.parquet"
    pop_path = folder / f"block_population_{state_fips}.parquet"
    _ensure(blocks_path, lambda: pygris.blocks(state=state_fips, year=vintage))
    _ensure(
        pop_path,
        lambda: _fetch_block_counts(state_fips, vintage, _BLOCK_POP_VAR[vintage], "block_pop"),
    )

    geoid_col = _BLOCK_ID[vintage]
    gdf = gpd.read_parquet(blocks_path, columns=[geoid_col, "geometry"])
    gdf = gdf.rename(columns={geoid_col: "block_id"})
    gdf["block_id"] = gdf["block_id"].astype(str)
    pop = pd.read_parquet(pop_path).set_index("block_id")["block_pop"]
    gdf["block_pop"] = gdf["block_id"].map(pop)
    unmatched = gdf["block_pop"].isna()
    if unmatched.any():
        msg = (
            f"{unmatched.sum():,} of {len(gdf):,} blocks in {blocks_path.name} have no "
            f"population in {pop_path.name}, e.g. {gdf.loc[unmatched, 'block_id'].head(3).tolist()}"
        )
        raise ValueError(msg)
    gdf["block_pop"] = gdf["block_pop"].astype(int)
    logger.info(
        "Total block population for state %s vintage %d: %d",
        state_fips,
        vintage,
        gdf["block_pop"].sum(),
    )
    return gdf


def get_block_households(state_fips: str, pums_year: int, census_dir: Path) -> pl.DataFrame:
    """Decennial households per Census block, for sample-plan base weights.

    Uses the same vintage as the block geometries, so block GEOIDs match them
    and their first 12 characters match the sample plan's block groups.

    Returns a DataFrame with columns ``[block_id, households]``.
    """
    folder, vintage = _vintage_dir(census_dir, pums_year)
    path = folder / f"block_households_{state_fips}.parquet"
    _ensure(
        path,
        lambda: _fetch_block_counts(
            state_fips, vintage, _BLOCK_HOUSEHOLDS_VAR[vintage], "households"
        ),
    )
    households = pl.read_parquet(path)
    logger.info(
        "Total block households for state %s vintage %d: %d",
        state_fips,
        vintage,
        households["households"].sum(),
    )
    return households


def get_county_names(state_fips: str, pums_year: int, census_dir: Path) -> pl.DataFrame:
    """The Census name of every county in *state_fips*, such as "Alameda County".

    Returns a DataFrame with columns ``[county_fips, county_name]``, where
    ``county_fips`` is the 5-character state and county code that starts every
    block GEOID.
    """
    folder, vintage = _vintage_dir(census_dir, pums_year)
    path = folder / f"county_names_{state_fips}.parquet"
    _ensure(path, lambda: _county_names(state_fips, vintage))
    return pl.read_parquet(path)
