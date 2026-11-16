"""PUMA-specific crosswalk wrapper.

``PumaCrosswalk`` reads Census PUMA and block geographies and
delegates the heavy lifting to [`build_crosswalk`][utils.crosswalk.build_crosswalk].

See ``docs/pipeline_steps/weighting/crosswalk.md`` for the detailed
crosswalk explanation, diagrams, and worked example.

1. Load target zone polygons; auto-discover overlapping PUMAs.
2. Read the TIGER PUMA and block boundaries (via ``census_geo``).
3. Rasterize block population into a density grid.
4. Rasterize PUMA IDs into a categorical label grid.
5. ``exactextract``: compute ``sum(population)`` per target zone, grouped by
   PUMA label.
6. Normalise: ``allocation_weight = pop(puma, target) / pop(puma)`` per PUMA.

Resolution only affects within-block population distribution granularity —
boundary accuracy is exact at any resolution due to ``exactextract``'s
analytical sub-cell coverage.

Outputs:

* ``crosswalk_df``: ``puma_id``, ``target_zone_id``, ``population``,
  ``allocation_weight``.
* ``puma_ids``: list of PUMAs overlapping the study area.

Example:
    ```python

    xw = PumaCrosswalk(geo_cfg, state_fips="06", pums_year=2023, census_dir=census_dir)
    households = xw.assign_households(households)
    pums_hh, pums_per = xw.allocate_pums_weights(pums_hh, pums_per)
    ```
"""

import logging
from pathlib import Path

import geopandas as gpd
import polars as pl
from pydantic import BaseModel, ConfigDict, Field, field_validator
from shapely.ops import unary_union

from processing.weighting.data_prep.census_geo import get_block_gdf, get_puma_gdf
from utils.crosswalk import build_crosswalk

logger = logging.getLogger(__name__)

# Counties under this share of a zone's population are left out of its name.
_MIN_COUNTY_SHARE = 0.01


def zone_to_group(zone_groups: dict[str, list[str]]) -> dict[str, str]:
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

    model_config = ConfigDict(extra="forbid")

    file: str
    id_field: str | None = Field(description="Zone-ID column; null for one study-area boundary.")


class GeographyConfig(BaseModel):
    """Geography block of the weighting YAML config.

    Attributes:
        target_zones: Polygon file and optional zone-ID column.
        resolution: Raster cell size in metres.
        min_allocation: PUMA slivers allocated less than this share are dropped.
        zone_groups: Named groups of target zones balanced as one; null for none.
    """

    model_config = ConfigDict(extra="forbid")

    target_zones: TargetZoneConfig
    resolution: int
    min_allocation: float
    zone_groups: dict[str, list[str]] | None

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


# ---------------------------------------------------------------------------
# PumaCrosswalk
# ---------------------------------------------------------------------------
class PumaCrosswalk:
    """Population-weighted PUMA-to-target-zone crosswalk.

    Loads target zones and Census geographies on construction, rasterizes
    block population, and cross-tabulates to produce allocation weights.
    Exposes ``crosswalk_df``, ``puma_ids``, and ``target_gdf`` for
    downstream use.  The crosswalk produces both ``target_zone_id`` (raw
    target zone from the shapefile) and ``zone_group_id`` (the
    zone group the balancer fits — equal to ``target_zone_id`` unless zone
    groups are configured).
    """

    def __init__(
        self,
        config: GeographyConfig,
        state_fips: str,
        pums_year: int,
        census_dir: Path,
    ) -> None:
        """Build crosswalk from *config* and the Census geographies in *census_dir*."""
        logger.info("Initializing PumaCrosswalk with config: %s", config)

        # -- zone groups --------------------------------------------------
        self.zone_groups = config.zone_groups or {}
        self._zone_remap = zone_to_group(self.zone_groups)

        # -- target zones (always needed downstream) ----------------------
        self.target_gdf = _load_target_zones(
            config.target_zones.file,
            config.target_zones.id_field,
        )

        # -- Census geographies ---------------------------------------
        puma_gdf = get_puma_gdf(state_fips, pums_year, census_dir)
        block_gdf = get_block_gdf(state_fips, pums_year, census_dir)

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
            target_id_col="target_zone_id",
            weight_col="block_pop",
            resolution=config.resolution,
            min_allocation=config.min_allocation,
        ).rename({"source_id": "puma_id", "target_id": "target_zone_id"})

        self.crosswalk_df = self.crosswalk_df.with_columns(
            pl.col("target_zone_id").replace(self._zone_remap).alias("zone_group_id")
        )

        if self.zone_groups:
            logger.info(
                "Zone groups applied: %d target zones → %d zone groups",
                self.crosswalk_df["target_zone_id"].n_unique(),
                self.crosswalk_df["zone_group_id"].n_unique(),
            )

    # -- public methods ---------------------------------------------------

    @property
    def allocated_puma_ids(self) -> list[str]:
        """PUMAs that allocate PUMS households to some zone.

        Fewer than ``puma_ids``: a PUMA that only touches the study area, or whose
        share falls below ``min_allocation``, is in ``puma_ids`` but allocates nothing.
        """
        return self.crosswalk_df["puma_id"].unique().sort().to_list()

    def assign_block_groups(self, households: pl.DataFrame) -> pl.DataFrame:
        """Point-in-polygon assignment of households to Census block groups.

        Returns *households* with a ``bg_geo_id`` column added (null for
        points outside all block groups).  A block group is the first 12
        characters of its blocks' GEOIDs, so each point is matched to the
        block it lies in rather than to block groups dissolved from blocks,
        which is far slower and gives the same answer.

        A point on the edge between blocks lies within neither, though it
        does lie within their block group when they share one. Such a point
        takes the block group of the blocks it touches when they all agree,
        as it would against dissolved block groups.
        """
        points = gpd.GeoDataFrame(
            {"hh_id": households["hh_id"].to_numpy()},
            geometry=gpd.points_from_xy(
                households["home_lon"].to_numpy(),
                households["home_lat"].to_numpy(),
            ),
            crs="EPSG:4326",
        )
        blocks = self.block_gdf[["block_id", "geometry"]].to_crs("EPSG:4326")
        schema = {"hh_id": households.schema["hh_id"], "bg_geo_id": pl.Utf8}

        def block_groups(predicate: str, pts: gpd.GeoDataFrame) -> pl.DataFrame:
            joined = gpd.sjoin(pts, blocks, how="inner", predicate=predicate)
            return pl.DataFrame(
                {
                    "hh_id": joined["hh_id"].to_numpy(),
                    "bg_geo_id": joined["block_id"].str[:12].to_list(),
                },
                schema=schema,
            )

        within = block_groups("within", points)
        on_edge = points[~points["hh_id"].isin(within["hh_id"].to_numpy())]
        edge_bg = (
            block_groups("intersects", on_edge)
            .rename({"bg_geo_id": "_bg"})
            .group_by("hh_id")
            .agg(pl.col("_bg").unique())
            .filter(pl.col("_bg").list.len() == 1)
            .select("hh_id", pl.col("_bg").list.first().alias("bg_geo_id"))
        )

        result_pl = pl.concat([within, edge_bg]).unique(subset=["hh_id"], keep="first")
        return households.join(result_pl, on="hh_id", how="left")

    def assign_households(self, households: pl.DataFrame) -> pl.DataFrame:
        """Point-in-polygon assignment of households to target zones and PUMAs.

        Returns *households* with ``target_zone_id``, ``zone_group_id`` and
        ``home_puma`` columns added (each null for points outside all polygons).
        The PUMAs are the ones the PUMS is allocated from, so both sides of the
        ``h_puma`` target share one set of boundaries.
        """
        points = gpd.GeoDataFrame(
            {"hh_id": households["hh_id"].to_list()},
            geometry=gpd.points_from_xy(
                households["home_lon"].to_list(),
                households["home_lat"].to_list(),
            ),
            crs="EPSG:4326",
        )

        def within(polygons: gpd.GeoDataFrame, id_col: str) -> pl.DataFrame:
            shapes = polygons.to_crs("EPSG:4326")[[id_col, "geometry"]]
            joined = gpd.sjoin(points, shapes, how="left", predicate="within")
            return pl.from_pandas(joined[["hh_id", id_col]]).unique(subset=["hh_id"], keep="first")

        zones = within(self.target_gdf, "target_zone_id").with_columns(
            pl.col("target_zone_id").replace(self._zone_remap).alias("zone_group_id")
        )
        pumas = within(self.puma_gdf, "puma_id").rename({"puma_id": "home_puma"})
        return households.join(zones, on="hh_id", how="left").join(pumas, on="hh_id", how="left")

    def assign_blocks(self, blocks: gpd.GeoDataFrame) -> pl.DataFrame:
        """Assign Census blocks to the study's PUMAs and zone groups.

        Each block is placed by a point inside it, so it gets exactly one PUMA
        and at most one zone. Pass every block in the state, not just the ones
        in the study area: a PUMA's Census total must count all of its blocks.

        Returns ``[block_id, puma_id, target_zone_id, zone_group_id]`` for blocks
        inside the study's PUMAs, with both zone columns null outside the zones.
        """
        pumas = self.puma_gdf[["puma_id", "geometry"]]
        minx, miny, maxx, maxy = pumas.to_crs(blocks.crs).total_bounds
        nearby = blocks.cx[minx:maxx, miny:maxy]
        points = gpd.GeoDataFrame(
            {"block_id": nearby["block_id"].to_numpy()},
            geometry=nearby.geometry.representative_point().to_numpy(),
            crs=blocks.crs,
        )

        def within(polygons: gpd.GeoDataFrame, id_col: str) -> pl.DataFrame:
            shapes = polygons.to_crs(blocks.crs)[[id_col, "geometry"]]
            joined = gpd.sjoin(points, shapes, how="inner", predicate="within")
            return pl.from_pandas(joined[["block_id", id_col]]).unique(
                subset=["block_id"], keep="first"
            )

        zones = within(self.target_gdf, "target_zone_id").with_columns(
            pl.col("target_zone_id").replace(self._zone_remap).alias("zone_group_id")
        )
        return within(pumas, "puma_id").join(zones, on="block_id", how="left")

    def zone_counties(self, county_names: pl.DataFrame) -> dict[str, str]:
        """Name the Census counties each target zone covers, by population share.

        The names come from the Census, not from the zone IDs in the config, so a
        zone ID put in the wrong group shows up as a county that doesn't belong.
        A zone inside one county reads "Alameda County"; one spanning several
        reads "Alameda County 92%, Contra Costa County 8%". Counties under 1% of
        a zone's population are left out.

        Args:
            county_names: ``[county_fips, county_name]``, from
                [`get_county_names`][processing.weighting.data_prep.census_geo.get_county_names].

        Raises:
            ValueError: A county the zones cover has no Census name.
        """
        blocks = self.assign_blocks(self.block_gdf).join(
            pl.from_pandas(self.block_gdf[["block_id", "block_pop"]]), on="block_id"
        )
        shares = (
            blocks.filter(pl.col("target_zone_id").is_not_null())
            .group_by("target_zone_id", pl.col("block_id").str.slice(0, 5).alias("county_fips"))
            .agg(pl.col("block_pop").sum())
            .with_columns(
                (pl.col("block_pop") / pl.col("block_pop").sum().over("target_zone_id")).alias(
                    "share"
                )
            )
            .filter(pl.col("share") >= _MIN_COUNTY_SHARE)
            .join(county_names, on="county_fips", how="left")
            .sort("target_zone_id", "share", descending=[False, True])
        )
        unnamed = shares.filter(pl.col("county_name").is_null())["county_fips"].unique().to_list()
        if unnamed:
            msg = f"The Census has no name for counties {sorted(unnamed)}"
            raise ValueError(msg)

        labels: dict[str, str] = {}
        for (zone,), rows in shares.group_by("target_zone_id", maintain_order=True):
            names = rows["county_name"].to_list()
            if rows.height == 1:
                labels[str(zone)] = names[0]
            else:
                parts = zip(names, rows["share"].to_list(), strict=True)
                labels[str(zone)] = ", ".join(f"{n} {s:.0%}" for n, s in parts)
        return labels

    def allocate_pums_weights(
        self,
        hh_df: pl.DataFrame,
        person_df: pl.DataFrame,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Join crosswalk to PUMS and scale weights by allocation factor.

        Produces ``_xw_WGTP`` / ``_xw_PWGTP`` columns (original weight
        times ``allocation_weight``), and the same for each replicate weight,
        and adds ``target_zone_id`` and ``zone_group_id`` to both frames.
        """
        xw = self.crosswalk_df.select(
            "puma_id", "target_zone_id", "zone_group_id", "allocation_weight"
        )
        puma = pl.col("PUMA").cast(pl.Utf8)

        def scaled(df: pl.DataFrame, weight: str) -> pl.DataFrame:
            # The weight, then its replicates (WGTP1, ...), times the allocation weight
            reps = [c for c in df.columns if c.startswith(weight) and c[len(weight) :].isdigit()]
            return df.with_columns(
                (pl.col(c).cast(pl.Float64) * pl.col("allocation_weight")).alias(f"_xw_{c}")
                for c in [weight, *reps]
            )

        hh_xw = scaled(hh_df.join(xw, left_on=puma, right_on="puma_id", how="inner"), "WGTP")
        person_xw = scaled(
            person_df.join(hh_df.select("SERIALNO", "PUMA"), on="SERIALNO", how="left").join(
                xw, left_on=puma, right_on="puma_id", how="inner"
            ),
            "PWGTP",
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
        weight_col: str = "WGTP",
    ) -> pl.DataFrame:
        """Assign target-zone IDs and scale PUMS weights via the crosswalk.

        Each PUMS household is duplicated per overlapping target zone, and
        its weight is scaled by the zone's ``allocation_weight``.

        Args:
            pums_incidence: Pivoted PUMS incidence table.  Must contain ``PUMA`` and
                *weight_col*.
            weight_col: Original PUMS weight column to scale (default ``"WGTP"``).

        Returns:
            Incidence table with ``target_zone_id`` and ``zone_group_id`` added,
            *weight_col* scaled by ``allocation_weight``, and rows
            multiplied per zone overlap.
        """
        xw = self.crosswalk_df.select(
            "puma_id",
            "target_zone_id",
            "zone_group_id",
            "allocation_weight",
        )

        result = (
            pums_incidence.join(
                xw,
                left_on=pl.col("PUMA").cast(pl.Utf8),
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

        # Guard: every crosswalk zone_group_id must appear in the result.
        # A missing zone means the balancer will have no seed data for it.
        expected_geos = set(xw["zone_group_id"].unique().to_list())
        actual_geos = set(result["zone_group_id"].unique().to_list())
        missing_geos = expected_geos - actual_geos

        if missing_geos:
            pums_puma_ids = pums_incidence["PUMA"].cast(pl.Utf8).unique()
            xw_puma_ids = xw["puma_id"].unique()
            if actual_geos:
                msg = (
                    f"{len(missing_geos)} zone group(s) from the crosswalk received "
                    f"zero PUMS allocation: {sorted(missing_geos)}"
                )
            else:
                pums_samples = pums_puma_ids.sort().head(5).to_list()
                xw_samples = xw_puma_ids.sort().head(5).to_list()
                msg = (
                    f"ALL {len(missing_geos)} zone group(s) missing after PUMA join \u2014 "
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
            result["target_zone_id"].n_unique(),
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
    which will be used as the ``target_zone_id`` in the crosswalk.
    If *id_field* is not provided, the zones will be dissolved into a
    single geometry with a dummy ``target_zone_id`` of "1".
    """
    gdf = source.copy() if isinstance(source, gpd.GeoDataFrame) else gpd.read_file(source)
    if id_field is None:
        gdf = gdf.dissolve()
        gdf["target_zone_id"] = "1"
    else:
        if id_field not in gdf.columns:
            msg = f"id_field {id_field!r} not found. Available: {list(gdf.columns)}"
            raise ValueError(msg)
        gdf = gdf.rename(columns={id_field: "target_zone_id"})
        gdf["target_zone_id"] = gdf["target_zone_id"].astype(str)
    return gdf[["target_zone_id", "geometry"]].reset_index(drop=True)
