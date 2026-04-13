"""PUMA-specific crosswalk wrapper.

``PumaCrosswalk`` fetches Census PUMA and block geographies and
delegates the heavy lifting to [`build_crosswalk`][utils.crosswalk.build_crosswalk].

See ``docs/pipeline_steps/weighting/crosswalk.md`` for the detailed
crosswalk explanation, diagrams, and worked example.

1. Load target zone polygons; auto-discover overlapping PUMAs.
2. Download/cache TIGER PUMA and block shapefiles (via ``census_geo``).
3. Rasterize block population into a density grid.
4. Rasterize PUMA IDs into a categorical label grid.
5. ``exactextract``: compute ``sum(population)`` per target zone, grouped by
   PUMA label.
6. Normalise: ``allocation_weight = pop(puma, target) / pop(puma)`` per PUMA.

Resolution only affects within-block population distribution granularity —
boundary accuracy is exact at any resolution due to ``exactextract``'s
analytical sub-cell coverage.

Outputs:

* ``crosswalk_df``: ``puma_id``, ``study_geoid``, ``population``,
  ``allocation_weight``.
* ``puma_ids``: list of PUMAs overlapping the study area.

Example:
    ```python

    xw = PumaCrosswalk(geo_cfg, state_fips="06", pums_year=2023)
    households = xw.assign_households(households)
    pums_hh, pums_per = xw.allocate_pums_weights(pums_hh, pums_per)
    ```
"""

import hashlib
import json
import logging
from pathlib import Path

import geopandas as gpd
import polars as pl
from pydantic import BaseModel, field_validator
from shapely.ops import unary_union

from processing.weighting.data_prep.census_geo import get_block_gdf, get_puma_gdf
from utils.crosswalk import build_crosswalk

logger = logging.getLogger(__name__)


def _build_zone_remap(zone_groups: dict[str, list[str]]) -> dict[str, str]:
    """Invert ``{group: [zone, ...]}`` → ``{zone: group}``."""
    remap: dict[str, str] = {}
    for group, zones in zone_groups.items():
        for z in zones:
            if z in remap:
                msg = f"Zone {z!r} appears in multiple groups ({remap[z]!r} and {group!r})"
                raise ValueError(msg)
            remap[z] = group
    return remap


# ---------------------------------------------------------------------------
# Config models (parsed from YAML)
# ---------------------------------------------------------------------------
class TargetZoneConfig(BaseModel):
    """Target zone polygon source."""

    file: str
    id_field: str | None = None


class GeographyConfig(BaseModel):
    """Geography block of the weighting YAML config.

    Attributes:
        target_zones: Polygon file and optional zone-ID column.
        resolution: Raster cell size in metres (default 250).
    """

    target_zones: TargetZoneConfig
    resolution: int = 100
    min_allocation: float = 0.0

    @field_validator("resolution")
    @classmethod
    def _positive_resolution(cls, v: int) -> int:
        if v <= 0:
            msg = f"Resolution must be positive, got {v}"
            raise ValueError(msg)
        return v

    @field_validator("min_allocation")
    @classmethod
    def _valid_min_allocation(cls, v: float) -> float:
        if not 0 <= v < 1:
            msg = f"min_allocation must be in [0, 1), got {v}"
            raise ValueError(msg)
        return v


def _crosswalk_cache_key(
    config: "GeographyConfig",
    state_fips: str,
    pums_year: int,
    zone_groups: dict[str, list[str]] | None,
) -> str:
    """Compute a deterministic hash from all crosswalk inputs."""
    h = hashlib.sha256()
    # Target zones: hash file contents so edits invalidate the cache
    target_path = Path(config.target_zones.file)
    if target_path.exists():
        h.update(target_path.read_bytes())
    h.update(
        json.dumps(
            {
                "id_field": config.target_zones.id_field,
                "resolution": config.resolution,
                "min_allocation": config.min_allocation,
                "state_fips": state_fips,
                "pums_year": pums_year,
                "zone_groups": zone_groups or {},
            },
            sort_keys=True,
        ).encode()
    )
    return h.hexdigest()[:12]


# ---------------------------------------------------------------------------
# PumaCrosswalk
# ---------------------------------------------------------------------------
class PumaCrosswalk:
    """Population-weighted PUMA-to-target-zone crosswalk.

    Loads target zones and Census geographies on construction, rasterizes
    block population, and cross-tabulates to produce allocation weights.
    Exposes ``crosswalk_df``, ``puma_ids``, and ``target_gdf`` for
    downstream use.  The crosswalk produces both ``study_geoid`` (raw
    target zone from the shapefile) and ``ctrl_geoid`` (balancing
    geography — equal to ``study_geoid`` unless zone groups are
    configured).
    """

    def __init__(
        self,
        config: GeographyConfig,
        state_fips: str,
        pums_year: int,
        cache_dir: Path | None = None,
        zone_groups: dict[str, list[str]] | None = None,
    ) -> None:
        """Build crosswalk from *config* and Census geographies."""
        logger.info("Initializing PumaCrosswalk with config: %s", config)

        # -- zone groups --------------------------------------------------
        self.zone_groups = zone_groups or {}
        self._zone_remap = _build_zone_remap(self.zone_groups) if self.zone_groups else {}

        # -- target zones (always needed downstream) ----------------------
        self.target_gdf = _load_target_zones(
            config.target_zones.file,
            config.target_zones.id_field,
        )

        # -- cache check --------------------------------------------------
        cache_key = _crosswalk_cache_key(config, state_fips, pums_year, zone_groups)
        cache_dir_xw = (cache_dir / "crosswalk" / cache_key) if cache_dir else None

        if cache_dir_xw and (cache_dir_xw / "crosswalk.parquet").exists():
            logger.info("Loading cached crosswalk from %s", cache_dir_xw)
            self.crosswalk_df = pl.read_parquet(cache_dir_xw / "crosswalk.parquet")
            self.puma_gdf = gpd.read_parquet(cache_dir_xw / "puma_clipped.parquet")
            self.block_gdf = gpd.read_parquet(cache_dir_xw / "block_clipped.parquet")
            self.puma_ids = sorted(self.puma_gdf["puma_id"].unique().tolist())
        else:
            # -- Census geographies ---------------------------------------
            puma_gdf = get_puma_gdf(state_fips, pums_year, cache_dir=cache_dir)
            block_gdf = get_block_gdf(state_fips, pums_year, cache_dir=cache_dir)

            # -- clip to study area ---------------------------------------
            logger.info("Clipping Census geographies to target zone extent")
            study_crs = puma_gdf.crs or "EPSG:4326"
            study_area = unary_union(self.target_gdf.to_crs(study_crs).geometry)

            self.block_gdf = block_gdf[block_gdf.intersects(study_area)].copy()
            self.puma_gdf = puma_gdf[puma_gdf.intersects(study_area)].copy()
            self.puma_ids = sorted(self.puma_gdf["puma_id"].unique().tolist())

            if not self.puma_ids:
                msg = "No PUMAs overlap the target zone geometry."
                raise ValueError(msg)
            logger.info(
                "Overlapping PUMAs: %d (number may be high due to some may just be touching)",
                len(self.puma_ids),
            )

            # -- rasterize & cross-tabulate -------------------------------
            self.crosswalk_df = build_crosswalk(
                source_gdf=self.puma_gdf,
                target_gdf=self.target_gdf,
                weight_gdf=self.block_gdf,
                source_id_col="puma_id",
                target_id_col="study_geoid",
                weight_col="block_pop",
                resolution=config.resolution,
                min_allocation=config.min_allocation,
            ).rename({"source_id": "puma_id", "target_id": "study_geoid"})

            self.crosswalk_df = self.crosswalk_df.with_columns(
                pl.col("study_geoid").replace(self._zone_remap).alias("ctrl_geoid")
            )

            # -- write cache ----------------------------------------------
            if cache_dir_xw:
                cache_dir_xw.mkdir(parents=True, exist_ok=True)
                self.crosswalk_df.write_parquet(cache_dir_xw / "crosswalk.parquet")
                self.puma_gdf.to_parquet(cache_dir_xw / "puma_clipped.parquet")
                self.block_gdf.to_parquet(cache_dir_xw / "block_clipped.parquet")
                logger.info("Cached crosswalk → %s", cache_dir_xw)

        if self.zone_groups:
            logger.info(
                "Zone groups applied: %d study zones → %d ctrl zones",
                self.crosswalk_df["study_geoid"].n_unique(),
                self.crosswalk_df["ctrl_geoid"].n_unique(),
            )

    # -- public methods ---------------------------------------------------

    @property
    def zone_populations(self) -> pl.DataFrame:
        """Census block population totals per balancing zone.

        Returns a DataFrame with columns ``[geo_id, target_population]``
        where ``target_population`` is the sum of Census block population
        allocated to each ``ctrl_geoid``.
        """
        return (
            self.crosswalk_df.group_by("ctrl_geoid")
            .agg(pl.col("population").sum().alias("target_population"))
            .rename({"ctrl_geoid": "geo_id"})
            .with_columns(pl.col("geo_id").cast(pl.Utf8))
            .sort("geo_id")
        )

    @property
    def block_group_populations(self) -> pl.DataFrame:
        """Census block population summed to block-group level.

        Block group GEOID = first 12 characters of the 15-character
        Census block GEOID (FIPS state + county + tract + block group).

        Returns a DataFrame with columns ``[bg_geo_id, bg_population]``.
        """
        bg_df = pl.from_pandas(self.block_gdf[["block_id", "block_pop"]].copy()).with_columns(
            pl.col("block_id").str.slice(0, 12).alias("bg_geo_id"),
        )
        return (
            bg_df.group_by("bg_geo_id")
            .agg(pl.col("block_pop").sum().alias("bg_population"))
            .sort("bg_geo_id")
        )

    def assign_block_groups(
        self,
        households: pl.DataFrame,
        lon_col: str = "home_lon",
        lat_col: str = "home_lat",
    ) -> pl.DataFrame:
        """Point-in-polygon assignment of households to Census block groups.

        Returns *households* with a ``bg_geo_id`` column added (null for
        points outside all block groups).  Block group boundaries are
        derived by dissolving Census blocks on the first 12 characters
        of their GEOID.
        """
        # Dissolve blocks → block groups
        bg_gdf = self.block_gdf.copy()
        bg_gdf["bg_geo_id"] = bg_gdf["block_id"].str[:12]
        bg_dissolved = bg_gdf.dissolve(by="bg_geo_id").reset_index()[["bg_geo_id", "geometry"]]

        points = gpd.GeoDataFrame(
            {"hh_id": households["hh_id"].to_list()},
            geometry=gpd.points_from_xy(
                households[lon_col].to_list(),
                households[lat_col].to_list(),
            ),
            crs="EPSG:4326",
        )
        bg_dissolved = bg_dissolved.to_crs("EPSG:4326")
        joined = gpd.sjoin(points, bg_dissolved, how="left", predicate="within")
        joined = joined.drop(columns=["geometry", "index_right"])

        result_pl = pl.from_pandas(joined[["hh_id", "bg_geo_id"]]).unique(
            subset=["hh_id"], keep="first"
        )
        return households.join(result_pl, on="hh_id", how="left")

    def assign_households(
        self,
        households: pl.DataFrame,
        lon_col: str = "home_lon",
        lat_col: str = "home_lat",
    ) -> pl.DataFrame:
        """Point-in-polygon assignment of households to target zones.

        Returns *households* with ``study_geoid`` and ``ctrl_geoid``
        columns added (null for points outside all zones).
        """
        points = gpd.GeoDataFrame(
            {"hh_id": households["hh_id"].to_list()},
            geometry=gpd.points_from_xy(
                households[lon_col].to_list(),
                households[lat_col].to_list(),
            ),
            crs="EPSG:4326",
        )
        target = self.target_gdf.to_crs("EPSG:4326")[["study_geoid", "geometry"]]
        joined = gpd.sjoin(points, target, how="left", predicate="within")
        joined = joined.drop(columns=["geometry", "index_right"])

        result_pl = (
            pl.from_pandas(joined[["hh_id", "study_geoid"]])
            .unique(subset=["hh_id"], keep="first")
            .with_columns(pl.col("study_geoid").replace(self._zone_remap).alias("ctrl_geoid"))
        )
        return households.join(result_pl, on="hh_id", how="left")

    def allocate_pums_weights(
        self,
        hh_df: pl.DataFrame,
        person_df: pl.DataFrame,
        geo_col: str = "PUMA",
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Join crosswalk to PUMS and scale weights by allocation factor.

        Produces ``_xw_WGTP`` / ``_xw_PWGTP`` columns (original weight
        times ``allocation_weight``) and adds ``study_geoid`` and
        ``ctrl_geoid`` to both frames.
        """
        xw = self.crosswalk_df.select("puma_id", "study_geoid", "ctrl_geoid", "allocation_weight")

        hh_xw = hh_df.join(
            xw, left_on=pl.col(geo_col).cast(pl.Utf8), right_on="puma_id", how="inner"
        ).with_columns(
            (pl.col("WGTP").cast(pl.Float64) * pl.col("allocation_weight")).alias("_xw_WGTP"),
        )

        # Scale household replicate weights if present
        hh_rep_cols = [c for c in hh_xw.columns if c.startswith("WGTP") and c[4:].isdigit()]
        if hh_rep_cols:
            hh_xw = hh_xw.with_columns(
                [
                    (pl.col(c).cast(pl.Float64) * pl.col("allocation_weight")).alias(f"_xw_{c}")
                    for c in hh_rep_cols
                ]
            )

        person_xw = (
            person_df.join(hh_df.select("SERIALNO", geo_col), on="SERIALNO", how="left")
            .join(xw, left_on=pl.col(geo_col).cast(pl.Utf8), right_on="puma_id", how="inner")
            .with_columns(
                (pl.col("PWGTP").cast(pl.Float64) * pl.col("allocation_weight")).alias("_xw_PWGTP"),
            )
        )

        # Scale person replicate weights if present
        per_rep_cols = [c for c in person_xw.columns if c.startswith("PWGTP") and c[5:].isdigit()]
        if per_rep_cols:
            person_xw = person_xw.with_columns(
                [
                    (pl.col(c).cast(pl.Float64) * pl.col("allocation_weight")).alias(f"_xw_{c}")
                    for c in per_rep_cols
                ]
            )
        logger.info(
            "Crosswalk join: %d HH -> %d, %d persons -> %d",
            len(hh_df),
            len(hh_xw),
            len(person_df),
            len(person_xw),
        )
        return hh_xw, person_xw

    def allocate_pums_incidence(
        self,
        pums_incidence: pl.DataFrame,
        *,
        puma_col: str = "PUMA",
        weight_col: str = "WGTP",
    ) -> pl.DataFrame:
        """Assign target-zone IDs and scale PUMS weights via the crosswalk.

        Each PUMS household is duplicated per overlapping target zone, and
        its weight is scaled by the zone's ``allocation_weight``.

        Args:
            pums_incidence: Pivoted PUMS incidence table.  Must contain *puma_col* and
                *weight_col*.
            puma_col: Column identifying the PUMA (default ``"PUMA"``).
            weight_col: Original PUMS weight column to scale (default ``"WGTP"``).

        Returns:
            Incidence table with ``study_geoid`` and ``ctrl_geoid`` added,
            *weight_col* scaled by ``allocation_weight``, and rows
            multiplied per zone overlap.
        """
        xw = self.crosswalk_df.select(
            "puma_id",
            "study_geoid",
            "ctrl_geoid",
            "allocation_weight",
        )

        result = (
            pums_incidence.join(
                xw,
                left_on=pl.col(puma_col).cast(pl.Utf8),
                right_on="puma_id",
                how="inner",
            )
            .with_columns(
                (pl.col(weight_col).cast(pl.Float64) * pl.col("allocation_weight")).alias(
                    weight_col
                ),
            )
            .drop("allocation_weight")
        )

        # Guard: every crosswalk ctrl_geoid must appear in the result.
        # A missing zone means the balancer will have no seed data for it.
        expected_geos = set(xw["ctrl_geoid"].unique().to_list())
        actual_geos = set(result["ctrl_geoid"].unique().to_list())
        missing_geos = expected_geos - actual_geos

        if missing_geos:
            pums_puma_ids = pums_incidence[puma_col].cast(pl.Utf8).unique()
            xw_puma_ids = xw["puma_id"].unique()
            if actual_geos:
                msg = (
                    f"{len(missing_geos)} control zone(s) from the crosswalk received "
                    f"zero PUMS allocation: {sorted(missing_geos)}"
                )
            else:
                pums_samples = pums_puma_ids.sort().head(5).to_list()
                xw_samples = xw_puma_ids.sort().head(5).to_list()
                msg = (
                    f"ALL {len(missing_geos)} control zone(s) missing after PUMA join \u2014 "
                    f"no PUMS records matched the crosswalk.  This is almost certainly "
                    f"a PUMA ID type mismatch (e.g. integer vs. zero-padded string).  "
                    f"PUMS PUMAs (first 5): {pums_samples}.  "
                    f"Crosswalk PUMAs (first 5): {xw_samples}."
                )
            raise ValueError(msg)

        logger.info(
            "PUMS zone allocation: %d HHs \u2192 %d rows (%d zones)",
            len(pums_incidence),
            len(result),
            result["study_geoid"].n_unique(),
        )
        return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _load_target_zones(
    source: str | Path | gpd.GeoDataFrame,
    id_field: str | None,
) -> gpd.GeoDataFrame:
    """Load and normalise target zone polygons.

    The target zones is a custom polygon or multi-polygon file defining
    the custom geographies for which the controls should be balanced.
    For example, counties or target districts.

    The file must contain a column with unique identifiers for each zone,
    which will be used as the ``study_geoid`` in the crosswalk.
    If *id_field* is not provided, the zones will be dissolved into a
    single geometry with a dummy ``study_geoid`` of "1".
    """
    gdf = source.copy() if isinstance(source, gpd.GeoDataFrame) else gpd.read_file(source)
    if id_field is None:
        gdf = gdf.dissolve()
        gdf["study_geoid"] = "1"
    else:
        if id_field not in gdf.columns:
            msg = f"id_field {id_field!r} not found. Available: {list(gdf.columns)}"
            raise ValueError(msg)
        gdf = gdf.rename(columns={id_field: "study_geoid"})
        gdf["study_geoid"] = gdf["study_geoid"].astype(str)
    return gdf[["study_geoid", "geometry"]].reset_index(drop=True)
