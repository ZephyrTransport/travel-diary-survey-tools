"""The crosswalk map: how PUMAs overlap the target zones, for the weighting report."""

import math
from collections import defaultdict
from collections.abc import Mapping

import geopandas as gpd
import plotly.graph_objects as go
import polars as pl

from processing.weighting.data_prep.crosswalk import zone_to_group

from .table_html import profile_key

_MAP_SIMPLIFY_M = 10  # boundary tolerance; sub-pixel at regional zoom


# Target zones — per-zone traces for group-aware colouring
_GROUP_FILLS = [
    "rgba(70,130,180,0.15)",  # steel blue (default / ungrouped)
    "rgba(180,100,50,0.15)",  # burnt orange
    "rgba(100,170,80,0.15)",  # olive green
    "rgba(150,80,160,0.15)",  # plum
    "rgba(200,170,50,0.15)",  # gold
    "rgba(80,170,170,0.15)",  # teal
]


_GROUP_BORDERS = [
    "steelblue",
    "rgb(180,100,50)",
    "rgb(100,170,80)",
    "rgb(150,80,160)",
    "rgb(200,170,50)",
    "rgb(80,170,170)",
]


def _build_tooltips(
    target_4326: gpd.GeoDataFrame,
    xw: pl.DataFrame,
    sample_counts: dict[str | None, dict[str, int]],
) -> dict[str, str]:
    """Build per-zone tooltips showing PUMA allocation weights.

    The map is rendered once for the whole run, so every profile's sample count
    for a zone is listed together rather than the map being drawn per profile.
    """
    tooltips: dict[str, str] = {}
    for zone_id in target_4326["target_zone_id"]:
        rows = xw.filter(pl.col("target_zone_id") == zone_id).sort(
            "allocation_weight",
            descending=True,
        )
        if rows.is_empty():
            tooltips[zone_id] = f"Zone {zone_id}: no PUMA overlap"
            continue
        zone_pop = rows["population"].sum()
        header = f"Zone {zone_id} (Census block population {zone_pop:,.0f})"
        seeded = [
            f"{profile_key(profile)} {counts[zone_id]:,}"
            for profile, counts in sample_counts.items()
            if zone_id in counts
        ]
        if seeded:
            header += " — sample households: " + " · ".join(seeded)
        lines = [header]
        for r in rows.iter_rows(named=True):
            aw = r["allocation_weight"] * 100
            # Drop slivers below 0.01% allocation to reduce tooltip noise (outer joins)
            if aw < 0.01:  # noqa: PLR2004
                continue
            lines.append(f"  PUMA {r['puma_id']}: {aw:.2f}%")
        tooltips[zone_id] = "<br>".join(lines)
    return tooltips


def _build_zone_labels_and_colors(
    target_4326: gpd.GeoDataFrame,
    zone_to_group_idx: dict[str, int],
    zone_to_group_name: dict[str, str],
) -> tuple[list[str], list[str]]:
    """Build zone labels and colors for centroid labels."""
    zone_labels: list[str] = []
    zone_colors: list[str] = []
    for gid in target_4326["target_zone_id"]:
        gname = zone_to_group_name.get(gid)
        label = f"Zone {gid} ({gname})" if gname else f"Zone {gid}"
        zone_labels.append(label)
        gi = zone_to_group_idx.get(gid, 0)
        zone_colors.append(_GROUP_BORDERS[gi])
    return zone_labels, zone_colors


def _build_zone_group_index(
    target_4326: gpd.GeoDataFrame,
    zone_groups: dict[str, list[str]] | None = None,
) -> dict[str, int]:
    """Build mapping from zone ID to group index."""
    zone_to_group_idx: dict[str, int] = {}
    next_idx = 1  # 0 is reserved; explicit groups start at 1
    if zone_groups:
        for zones in zone_groups.values():
            idx = next_idx % len(_GROUP_FILLS)
            for z in zones:
                zone_to_group_idx[z] = idx
            next_idx += 1

    # Assign each ungrouped zone its own cycling colour
    for _, row in target_4326.iterrows():
        gid = row["target_zone_id"]
        if gid not in zone_to_group_idx:
            zone_to_group_idx[gid] = next_idx % len(_GROUP_FILLS)
            next_idx += 1

    return zone_to_group_idx


def _add_zone_traces(
    fig: go.Figure,
    target_4326: gpd.GeoDataFrame,
    zone_to_group_idx: dict[str, int],
) -> None:
    """Add target zone traces grouped by color."""
    group_rows: dict[int, list] = defaultdict(list)
    group_tooltips: dict[int, list[str]] = defaultdict(list)
    for _, row in target_4326.iterrows():
        gi = zone_to_group_idx.get(row["target_zone_id"], 0)
        group_rows[gi].append(row)
        group_tooltips[gi].append(row["tooltip"])

    for gi, rows in group_rows.items():
        fill = _GROUP_FILLS[gi]
        border = _GROUP_BORDERS[gi]
        batch = gpd.GeoDataFrame(rows, crs=target_4326.crs)
        fig.add_trace(
            go.Choroplethmap(
                geojson=batch.__geo_interface__,
                locations=batch.index,
                z=[0] * len(batch),
                colorscale=[[0, fill], [1, fill]],
                marker_line_color=border,
                marker_line_width=2,
                showscale=False,
                text=group_tooltips[gi],
                hoverinfo="text",
            )
        )


def _add_zone_label_traces(
    fig: go.Figure,
    zone_centroids: gpd.GeoSeries,
    zone_labels: list[str],
    zone_colors: list[str],
) -> None:
    """Add zone label traces grouped by color."""
    label_groups: dict[str, tuple[list[float], list[float], list[str]]] = defaultdict(
        lambda: ([], [], [])
    )
    for i, pt in enumerate(zone_centroids):
        c = zone_colors[i]
        lons, lats, texts = label_groups[c]
        lons.append(pt.x)  # pyright: ignore[reportAttributeAccessIssue]
        lats.append(pt.y)  # pyright: ignore[reportAttributeAccessIssue]
        texts.append(zone_labels[i])

    for color, (lons, lats, texts) in label_groups.items():
        fig.add_trace(
            go.Scattermap(
                lon=lons,
                lat=lats,
                mode="text",
                text=texts,
                textfont={"size": 12, "color": color},
                hoverinfo="skip",
                showlegend=False,
            )
        )


def _simplified(gdf: gpd.GeoDataFrame, tolerance_m: float) -> gpd.GeoDataFrame:
    """Reproject to WGS84, thinning the boundary to *tolerance_m* on the way.

    TIGER polygons carry far more vertices than a regional map can resolve, and
    they dominate the report's size -- on a Bay Area run, 72 PUMAs account for
    95% of the file. Douglas-Peucker at a few metres is imperceptible at this
    zoom and removes most of them. ``preserve_topology`` keeps each polygon
    valid; adjacent polygons are still simplified independently, so a tolerance
    far above a pixel would start to show hairlines between zones.
    """
    if tolerance_m <= 0:
        return gdf.to_crs("EPSG:4326")
    metric = gdf.estimate_utm_crs()
    thinned = gdf.to_crs(metric)
    thinned = thinned.set_geometry(thinned.geometry.simplify(tolerance_m))
    return thinned.to_crs("EPSG:4326")


def crosswalk_figure(
    puma_gdf: gpd.GeoDataFrame,
    target_gdf: gpd.GeoDataFrame,
    crosswalk_df: pl.DataFrame,
    seeds: Mapping[str | None, pl.DataFrame] | None = None,
    zone_groups: dict[str, list[str]] | None = None,
) -> go.Figure:
    """Build an interactive Plotly map of the crosswalk.

    Layers:
    - PUMA boundaries (dashed grey) — full extent
    - Study area outline (bold black)
    - Target zones (solid border, transparent fill) with tooltip
      showing PUMA allocation weights from the crosswalk.

    The geometry is run-level: it comes from the crosswalk, which is built once
    however many profiles are fitted, so the map is drawn once and every
    profile's per-zone seed count is listed in the same tooltip.

    Args:
        puma_gdf: PUMA boundary polygons (must have ``puma_id`` column).
        target_gdf: Target zone polygons (must have ``target_zone_id`` column).
        crosswalk_df: Crosswalk table with ``puma_id``, ``target_zone_id``,
            ``population``, ``allocation_weight``.
        seeds: Each profile's seed, keyed by profile name (must contain
            ``target_zone_id``).  Per-zone counts appear in the tooltip.
        zone_groups: Optional zone group mapping.  When provided, grouped zones
            share a fill colour and labels include the group name.

    Returns:
        go.Figure
    """
    puma_4326 = _simplified(puma_gdf, _MAP_SIMPLIFY_M)
    target_4326 = _simplified(target_gdf, _MAP_SIMPLIFY_M)
    study_boundary = target_4326.dissolve()

    # Build tooltip per target zone showing allocation weights
    xw = crosswalk_df.select(
        "puma_id",
        "target_zone_id",
        "population",
        "allocation_weight",
    )

    # Per-zone seed counts, one entry per fitted profile (Polars only)
    sample_counts: dict[str | None, dict[str, int]] = {}
    for profile, seed in (seeds or {}).items():
        if "target_zone_id" not in seed.columns:
            continue
        sample_counts[profile] = dict(
            seed.filter(pl.col("target_zone_id").is_not_null())
            .group_by("target_zone_id")
            .len()
            .iter_rows()
        )

    tooltips = _build_tooltips(target_4326, xw, sample_counts)
    target_4326 = target_4326.copy()
    target_4326["tooltip"] = target_4326["target_zone_id"].map(tooltips)

    fig = go.Figure()

    # PUMAs -- full boundaries, outline only
    fig.add_trace(
        go.Choroplethmap(
            geojson=puma_4326.__geo_interface__,
            locations=puma_4326.index,
            z=[0] * len(puma_4326),
            colorscale=[[0, "rgba(0,0,0,0)"], [1, "rgba(0,0,0,0)"]],
            marker_line_color="rgba(100,100,100,0.7)",
            marker_line_width=1,
            showscale=False,
            hoverinfo="skip",
        )
    )

    # Study area -- bold black outline
    fig.add_trace(
        go.Choroplethmap(
            geojson=study_boundary.__geo_interface__,
            locations=study_boundary.index,
            z=[0] * len(study_boundary),
            colorscale=[[0, "rgba(0,0,0,0)"], [1, "rgba(0,0,0,0)"]],
            marker_line_color="black",
            marker_line_width=3,
            showscale=False,
            hoverinfo="skip",
        )
    )

    # Build zone grouping
    zone_to_group_idx = _build_zone_group_index(target_4326, zone_groups)

    # Add target zone traces
    _add_zone_traces(fig, target_4326, zone_to_group_idx)

    # PUMA labels at centroids (grey)
    puma_centroids = puma_4326.to_crs("EPSG:5070").centroid.to_crs("EPSG:4326")
    fig.add_trace(
        go.Scattermap(
            lon=[p.x for p in puma_centroids],  # pyright: ignore[reportAttributeAccessIssue]
            lat=[p.y for p in puma_centroids],  # pyright: ignore[reportAttributeAccessIssue]
            mode="text",
            text=[f"PUMA {pid}" for pid in puma_4326["puma_id"]],
            textfont={"size": 10, "color": "gray"},
            hoverinfo="skip",
            showlegend=False,
        )
    )

    # Zone labels at centroids — colour-matched to group
    zone_centroids = target_4326.to_crs("EPSG:5070").centroid.to_crs("EPSG:4326")
    zone_to_group_name = zone_to_group(zone_groups or {})
    zone_labels, zone_colors = _build_zone_labels_and_colors(
        target_4326, zone_to_group_idx, zone_to_group_name
    )

    # Add zone label traces
    _add_zone_label_traces(fig, zone_centroids, zone_labels, zone_colors)

    # Layout -- centre on PUMA extent and estimate zoom from longitude span.
    bounds = puma_4326.total_bounds  # (minx, miny, maxx, maxy)
    center_lon = (bounds[0] + bounds[2]) / 2
    center_lat = (bounds[1] + bounds[3]) / 2
    lon_span = bounds[2] - bounds[0]
    # Mercator zoom: 360 / 2^z ≈ visible longitude span at the equator.
    # Adjust for map width (960 px / 256 px tile) and add padding.
    zoom = math.log2(360 / max(lon_span, 0.01)) + math.log2(960 / 256) - 0.5
    fig.update_layout(
        map={
            "style": "carto-positron",
            "center": {"lon": center_lon, "lat": center_lat},
            "zoom": max(zoom, 3),
        },
        autosize=False,
        width=960,
        height=700,
        margin={"l": 0, "r": 0, "t": 30, "b": 0},
        title="Crosswalk: PUMA to Target Zones",
    )

    return fig
