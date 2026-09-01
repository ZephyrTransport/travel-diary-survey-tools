"""Census TIGER geography loading.

PUMA and block geometries share the same decennial vintage (2010 for
pre-2022 PUMS, 2020 for 2022+) so that block GEOIDs match the sample
plan.  Block population is fetched from the Census decennial API
(PL 94-171 for 2020, SF1 for 2010) and joined by GEOID — the TIGER
shapefiles themselves do not always carry population columns.

Processed GeoDataFrames are cached as GeoParquet under
``<pipeline-cache-dir>/weighting/census_geo/`` for fast repeat loads.
"""

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import geopandas as gpd
import pygris
import requests
from tqdm import tqdm

logger = logging.getLogger(__name__)

_KEY_SIGNUP_URL = "https://api.census.gov/data/key_signup.html"

# Explicit column mappings per decennial vintage.
_PUMA_ID = {2020: "PUMACE20", 2010: "PUMACE10"}
_BLOCK_ID = {2020: "GEOID20", 2010: "GEOID10"}

# Decennial census API endpoints and total-population variable names.
_BLOCK_POP_API: dict[int, tuple[str, str]] = {
    2020: ("https://api.census.gov/data/2020/dec/pl", "P1_001N"),
    2010: ("https://api.census.gov/data/2010/dec/sf1", "P001001"),
}


# ---------------------------------------------------------------------------
# Vintage helper
# ---------------------------------------------------------------------------


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
# Census API helpers
# ---------------------------------------------------------------------------


def _census_api_key() -> str:
    """Return the Census data-API key from the environment.

    The Census data API requires a key on every request; a keyless request
    is redirected to an HTML "Missing Key" page (HTTP 200), which is not
    valid JSON.  Sign up (free, instant) at ``_KEY_SIGNUP_URL`` and set
    ``CENSUS_KEY`` (or ``CENSUS_API_KEY``) in the environment or a ``.env``
    file loaded by the runner script.
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

    A missing/invalid key makes the Census API return an HTML "Missing Key"
    page with HTTP 200, so ``raise_for_status()`` passes but ``resp.json()``
    fails with an opaque ``JSONDecodeError``.  This helper detects that case
    (and any other non-JSON body) and raises an actionable ``RuntimeError``.
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


def _parse_block_response(data: list[list[str]], pop_var: str) -> dict[str, int]:
    """Parse a Census API JSON response into ``{geoid: population}``."""
    header, *rows = data
    pop_i = header.index(pop_var)
    st_i, co_i, tr_i, bl_i = (header.index(c) for c in ("state", "county", "tract", "block"))
    return {r[st_i] + r[co_i] + r[tr_i] + r[bl_i]: int(r[pop_i]) for r in rows}


#: Attempts per county request before giving up. The Census API returns 5xx
#: under load often enough that a single failure must not end a run that is
#: making 58 of these; the delay grows so a struggling server is not hammered.
_MAX_CENSUS_ATTEMPTS = 4
_RETRY_BACKOFF_SECONDS = 2.0

#: Server-side statuses worth retrying. A 4xx is our request being wrong and
#: will fail identically however many times it is sent.
_TRANSIENT_STATUS = frozenset({500, 502, 503, 504})


def _is_transient(err: Exception) -> bool:
    """Whether *err* is a server-side failure that a later attempt may clear."""
    if isinstance(err, requests.HTTPError) and err.response is not None:
        return err.response.status_code in _TRANSIENT_STATUS
    return isinstance(err, requests.ConnectionError | requests.Timeout)


def _fetch_county_blocks(
    url: str,
    pop_var: str,
    state_fips: str,
    county: str,
    key: str,
) -> dict[str, int]:
    """Fetch block population for a single county, retrying server failures.

    Raises:
        requests.RequestException: If the request fails for a reason a retry
            cannot clear, or still fails after :data:`_MAX_CENSUS_ATTEMPTS`.
    """
    params = {
        "get": pop_var,
        "for": "block:*",
        "in": f"state:{state_fips} county:{county}",
        "key": key,
    }
    for attempt in range(1, _MAX_CENSUS_ATTEMPTS + 1):
        try:
            return _parse_block_response(
                _census_json(requests.get(url, params=params, timeout=120)), pop_var
            )
        except Exception as err:
            if not _is_transient(err) or attempt == _MAX_CENSUS_ATTEMPTS:
                raise
            delay = _RETRY_BACKOFF_SECONDS * attempt
            logger.warning(
                "County %s block request failed (attempt %d/%d): %s. Retrying in %.0fs.",
                county,
                attempt,
                _MAX_CENSUS_ATTEMPTS,
                err,
                delay,
            )
            time.sleep(delay)
    msg = f"County {county}: exhausted retries"  # pragma: no cover - loop always returns/raises
    raise RuntimeError(msg)


def _fetch_block_population(state_fips: str, vintage: int) -> dict[str, int]:
    """Fetch total population per Census block from the decennial census API.

    The 2020 PL endpoint supports ``county:* tract:*`` in a single
    request.  The 2010 SF1 endpoint does not, so we first list counties
    and then fetch blocks county-by-county (parallelised with threads).

    Returns ``{geoid: population}`` for every block in *state_fips*.
    """
    url, pop_var = _BLOCK_POP_API[vintage]
    key = _census_api_key()
    logger.info(
        "Fetching %d decennial block population for state %s from Census API",
        vintage,
        state_fips,
    )

    # 2020 PL accepts a single statewide request -- every block in the state in
    # one response, which for California is around half a million rows. The API
    # returns 500 for it often enough that it cannot be the only route, so a
    # failure falls through to the same county-by-county walk 2010 requires
    # rather than ending the run.
    if vintage >= 2020:  # noqa: PLR2004
        try:
            resp = requests.get(
                url,
                params={
                    "get": pop_var,
                    "for": "block:*",
                    "in": f"state:{state_fips} county:* tract:*",
                    "key": key,
                },
                timeout=300,
            )
            result = _parse_block_response(_census_json(resp), pop_var)
        except (requests.RequestException, RuntimeError) as err:
            logger.warning(
                "Statewide block request failed (%s); falling back to one request "
                "per county, which asks the API for far less at a time.",
                err,
            )
        else:
            logger.info("Fetched population for %d blocks", len(result))
            return result

    # County-by-county: required for 2010 SF1, and the fallback for 2020.
    resp = requests.get(
        url,
        params={"get": "NAME", "for": "county:*", "in": f"state:{state_fips}", "key": key},
        timeout=60,
    )
    county_data = _census_json(resp)
    co_i = county_data[0].index("county")
    counties = [r[co_i] for r in county_data[1:]]
    logger.info("Fetching blocks for %d counties in state %s", len(counties), state_fips)

    result: dict[str, int] = {}
    block_count = 0
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {
            pool.submit(_fetch_county_blocks, url, pop_var, state_fips, c, key): c for c in counties
        }
        with tqdm(total=len(counties), unit="county") as pbar:
            for future in as_completed(futures):
                county_pop = future.result()
                result.update(county_pop)
                block_count += len(county_pop)
                pbar.set_postfix(blocks=f"{block_count:,}")
                pbar.update(1)

    logger.info("Fetched population for %d blocks", len(result))
    return result


# ---------------------------------------------------------------------------
# GeoParquet cache
# ---------------------------------------------------------------------------


def _geo_cache_path(key: str, cache_dir: Path | None) -> Path | None:
    """Return the cache path for a census GeoParquet file, or None."""
    if cache_dir is None:
        return None
    return cache_dir / "census_geo" / f"{key}.parquet"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_puma_gdf(
    state_fips: str,
    pums_year: int,
    *,
    cache_dir: Path | None = None,
) -> gpd.GeoDataFrame:
    """Download PUMA polygons for *state_fips* via pygris.

    Returns a GeoDataFrame with ``puma_id`` (str) and ``geometry``.
    """
    vintage = puma_vintage_for_pums_year(pums_year)
    path = _geo_cache_path(f"puma_{state_fips}_{vintage}", cache_dir)
    if path and path.exists():
        logger.debug("Cache hit: %s", path)
        return gpd.read_parquet(path)

    logger.info(
        "Downloading %d-vintage PUMAs (TIGER %d) for state %s", vintage, pums_year, state_fips
    )
    id_col = _PUMA_ID[vintage]
    gdf = pygris.pumas(state=state_fips, year=pums_year, cache=True)
    gdf = (
        gdf[[id_col, "geometry"]]
        .rename(columns={id_col: "puma_id"})
        .assign(puma_id=lambda df: df["puma_id"].astype(str))
    )

    if path:
        path.parent.mkdir(parents=True, exist_ok=True)
        gdf.to_parquet(path)
        logger.debug("Cached → %s", path)
    return gdf


def get_block_gdf(
    state_fips: str,
    pums_year: int,
    *,
    cache_dir: Path | None = None,
) -> gpd.GeoDataFrame:
    """Download block geometries and join decennial population.

    Uses the same vintage as PUMAs so block GEOIDs match the sample
    plan.  Population is fetched from the Census decennial API and
    joined by GEOID.

    The population is used for dasymetric block-level weighting of cross
    PUMA-to-custom zone allocations.

    Returns a GeoDataFrame with ``block_id`` (str), ``block_pop`` (int),
    and ``geometry``.
    """
    vintage = puma_vintage_for_pums_year(pums_year)
    path = _geo_cache_path(f"block_{state_fips}_{vintage}", cache_dir)
    if path and path.exists():
        logger.debug("Cache hit: %s", path)
        return gpd.read_parquet(path)

    geoid_col = _BLOCK_ID[vintage]
    logger.info("Downloading %d TIGER blocks for state %s", vintage, state_fips)
    gdf = pygris.blocks(state=state_fips, year=vintage, cache=True)

    pop = _fetch_block_population(state_fips, vintage)
    if not pop:
        msg = (
            f"Fetched empty block population for state {state_fips} "
            f"vintage {vintage}. Check the Census API endpoint."
        )
        raise ValueError(msg)

    gdf = gdf[[geoid_col, "geometry"]].rename(columns={geoid_col: "block_id"})
    gdf["block_id"] = gdf["block_id"].astype(str)
    gdf["block_pop"] = gdf["block_id"].map(pop).fillna(0).astype(int)

    # Report population count sanity check.
    total_pop = gdf["block_pop"].sum()
    logger.info(
        "Total block population for state %s vintage %d: %d",
        state_fips,
        vintage,
        total_pop,
    )

    if path:
        path.parent.mkdir(parents=True, exist_ok=True)
        gdf.to_parquet(path)
        logger.debug("Cached → %s", path)
    return gdf
