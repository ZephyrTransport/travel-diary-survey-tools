"""Generic population-weighted geography crosswalk utilities.

Provides [`build_crosswalk`][utils.crosswalk.build_crosswalk], a generic function that maps *source*
polygons to *target* polygons via a *weight* polygon layer (e.g. Census
blocks with population counts).  The three layers are rasterized onto a
common grid and ``exactextract`` performs the zonal cross-tabulation with
sub-pixel coverage fractions.

See ``src/utils/CROSSWALK.md`` for the mathematical formulation.

Example:
    ```python
    from utils.crosswalk import build_crosswalk

    xw = build_crosswalk(
        source_gdf=pumas, target_gdf=counties, weight_gdf=blocks,
        source_id_col="PUMACE20", target_id_col="COUNTYFP",
        weight_col="block_pop", resolution=250,
    )
    ```
"""

import contextlib
import logging
from collections.abc import Generator

import geopandas as gpd
import numpy as np
import polars as pl
import rasterio
import rasterio.features
import rasterio.transform
from exactextract import exact_extract
from rasterio.crs import CRS
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds
from shapely.ops import unary_union

logger = logging.getLogger(__name__)

_ALBERS_CRS = CRS.from_epsg(5070)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def build_crosswalk(
    source_gdf: gpd.GeoDataFrame,
    target_gdf: gpd.GeoDataFrame,
    weight_gdf: gpd.GeoDataFrame,
    *,
    source_id_col: str = "source_id",
    target_id_col: str = "target_id",
    weight_col: str = "block_pop",
    resolution: int = 100,
    min_allocation: float = 0.0,
) -> pl.DataFrame:
    """Build a population-weighted crosswalk between two polygon layers.

    Args:
        source_gdf: Source zone polygons (e.g. PUMAs).  Must contain
            *source_id_col*.
        target_gdf: Target zone polygons (e.g. counties, TAZs).  Must
            contain *target_id_col*.
        weight_gdf: Weight polygons carrying a numeric attribute (e.g.
            Census blocks with ``block_pop``).  Must contain *weight_col*
            and geometry.
        source_id_col: Column in *source_gdf* identifying source zones.
        target_id_col: Column in *target_gdf* identifying target zones.
        weight_col: Numeric column in *weight_gdf* used as the allocation
            weight (typically population).
        resolution: Raster cell size in metres (EPSG:5070).
        min_allocation: Drop source-target pairs whose allocation weight
            is below this fraction (e.g. 0.02 = 2%).  Remaining weights
            are **not** re-normalised; the dropped slivers simply become
            part of the out-of-region remainder.  Default 0.0 (keep all).

    Returns:
        DataFrame with columns
        ``[source_id, target_id, population, allocation_weight]``.
        ``allocation_weight`` is the fraction of each source zone's
        **total** population that falls in the target zone.  Weights sum
        to <= 1.0 per ``source_id``; the remainder is population outside
        all target zones.
    """
    # Validate required columns
    for gdf, col, label in [
        (source_gdf, source_id_col, "source_gdf"),
        (target_gdf, target_id_col, "target_gdf"),
        (weight_gdf, weight_col, "weight_gdf"),
    ]:
        if col not in gdf.columns:
            msg = f"{label} missing column {col!r}. Available: {list(gdf.columns)}"
            raise ValueError(msg)

    # Project everything to equal-area CRS
    src_albers = source_gdf.to_crs(_ALBERS_CRS)
    tgt_albers = target_gdf.to_crs(_ALBERS_CRS)
    wgt_albers = weight_gdf.to_crs(_ALBERS_CRS)

    # Normalise ID columns to canonical names
    src_albers = src_albers.rename(columns={source_id_col: "source_id"})
    tgt_albers = tgt_albers.rename(columns={target_id_col: "target_id"})

    # Clip weight layer to source extent and drop zero-weight features
    src_bounds = unary_union(src_albers.geometry).bounds
    wgt_albers = wgt_albers.cx[
        src_bounds[0] : src_bounds[2],
        src_bounds[1] : src_bounds[3],
    ]
    wgt_albers = wgt_albers[wgt_albers[weight_col] > 0]
    n_dropped = len(weight_gdf) - len(wgt_albers)
    if n_dropped:
        logger.info(
            "Weight layer: kept %d features (dropped %d out-of-bounds or zero-weight)",
            len(wgt_albers),
            n_dropped,
        )

    # Compute raster grid from union of source extent
    total_weight = float(wgt_albers[weight_col].sum())
    src_bounds = unary_union(src_albers.geometry).bounds
    minx, miny, maxx, maxy = src_bounds

    # Resolution heuristic warning
    cell_area = resolution * resolution
    min_zone_area = tgt_albers.geometry.area.min()
    if min_zone_area / cell_area < 50:  # noqa: PLR2004
        logger.warning(
            "Smallest zone (%.0f m²) < 50x cell area (%d m²). Consider a finer resolution.",
            min_zone_area,
            cell_area,
        )

    w = int(np.ceil((maxx - minx) / resolution))
    h = int(np.ceil((maxy - miny) / resolution))
    transform = from_bounds(
        minx,
        miny,
        minx + w * resolution,
        miny + h * resolution,
        w,
        h,
    )
    shape = (h, w)

    # Rasterize
    pop_arr = _rasterize_weights(wgt_albers, weight_col, transform, shape)
    src_arr, int_to_src = _rasterize_categorical(
        src_albers,
        "source_id",
        transform,
        shape,
    )

    # Cross-tabulate via exactextract
    df = _cross_tabulate(pop_arr, src_arr, int_to_src, transform, tgt_albers)

    # Compute total rasterized population per source zone (including
    # population outside all target zones) so allocation_weight reflects
    # the true fraction, not an inflated share.
    flat_pop = pop_arr.ravel()
    flat_src = src_arr.ravel()
    src_mask = flat_src > 0
    src_totals = np.bincount(
        flat_src[src_mask],
        weights=flat_pop[src_mask],
        minlength=max(int_to_src) + 1,
    )
    source_pop_rows = [
        {"source_id": sid, "source_total_pop": float(src_totals[sint])}
        for sint, sid in int_to_src.items()
        if src_totals[sint] > 0
    ]
    source_pop_df = pl.DataFrame(source_pop_rows).with_columns(
        pl.col("source_id").cast(pl.Utf8),
    )

    # Conservation check
    raster_pop = df["population"].sum()
    pct = abs(raster_pop - total_weight) / max(total_weight, 1) * 100  # pyright: ignore[reportOperatorIssue]
    if pct > 2:  # noqa: PLR2004
        logger.warning(
            "Weight conservation: %.0f vs %.0f (%.1f%% diff)",
            raster_pop,
            total_weight,
            pct,
        )

    # Normalise to allocation weights per source zone using total
    # source population (not just the in-target-zone portion)
    df = df.join(source_pop_df, on="source_id", how="left")
    df = df.with_columns(
        (pl.col("population") / pl.col("source_total_pop")).alias("allocation_weight"),
    ).drop("source_total_pop")

    # Drop sliver allocations below threshold and redistribute their
    # weight proportionally among the remaining rows per source zone.
    # Also absorb small out-of-region remainders below the threshold.
    if min_allocation > 0:
        n_before = len(df)
        df = df.filter(pl.col("allocation_weight") >= min_allocation)
        n_dropped = n_before - len(df)

        # Re-normalise so remaining weights sum to 1.0 per source zone.
        # This absorbs both dropped target-zone slivers and small
        # out-of-region remainders in one step.
        df = df.with_columns(
            (
                pl.col("allocation_weight") / pl.col("allocation_weight").sum().over("source_id")
            ).alias("allocation_weight"),
        )
        if n_dropped:
            logger.info(
                "Dropped %d sliver rows with allocation_weight < %.4f",
                n_dropped,
                min_allocation,
            )

    logger.info(
        "Crosswalk: %d rows, %d source zones -> %d target zones",
        len(df),
        df["source_id"].n_unique(),
        df["target_id"].n_unique(),
    )
    return df


# ---------------------------------------------------------------------------
# Rasterization helpers
# ---------------------------------------------------------------------------
def _rasterize_weights(
    gdf: gpd.GeoDataFrame,
    weight_col: str,
    transform: rasterio.transform.Affine,
    shape: tuple[int, int],
) -> np.ndarray:
    """Burn polygon weights into a float32 array using centroid accumulation.

    Each polygon's full weight is placed in the raster cell containing its
    centroid.  This avoids losing population from small polygons (e.g.
    dense urban Census blocks) that are smaller than the raster cell size.
    """
    centroids = gdf.geometry.centroid
    rows, cols = rasterio.transform.rowcol(
        transform,
        centroids.x.values,
        centroids.y.values,
    )
    rows = np.asarray(rows, dtype=np.intp)
    cols = np.asarray(cols, dtype=np.intp)
    weights = gdf[weight_col].values.astype(np.float64)

    valid = (rows >= 0) & (rows < shape[0]) & (cols >= 0) & (cols < shape[1])
    n_outside = int((~valid).sum())
    if n_outside:
        logger.warning(
            "Weight raster: %d features had centroids outside grid bounds",
            n_outside,
        )
    weight_raster = np.zeros(shape, dtype=np.float64)
    np.add.at(weight_raster, (rows[valid], cols[valid]), weights[valid])  # pyright: ignore[reportArgumentType]
    weight_raster = weight_raster.astype(np.float32)

    raster_total = float(weight_raster.sum())
    input_total = float(weights.sum())  # pyright: ignore[reportAttributeAccessIssue]
    if input_total > 0:
        loss_pct = abs(raster_total - input_total) / input_total * 100
        if loss_pct > 1:
            logger.warning(
                "Weight raster conservation: %.0f vs %.0f (%.1f%% loss)",
                raster_total,
                input_total,
                loss_pct,
            )

    logger.info(
        "Weight raster (%s): %dx%d, total=%.0f",
        weight_col,
        shape[1],
        shape[0],
        weight_raster.sum(),
    )
    return weight_raster


def _rasterize_categorical(
    gdf: gpd.GeoDataFrame,
    id_col: str,
    transform: rasterio.transform.Affine,
    shape: tuple[int, int],
) -> tuple[np.ndarray, dict[int, str]]:
    """Burn polygon zones into an int32 array with an ID lookup dict."""
    gdf = gdf.copy()
    unique_ids = sorted(gdf[id_col].unique())
    id_to_int = {v: i + 1 for i, v in enumerate(unique_ids)}
    gdf["_zint"] = gdf[id_col].map(id_to_int).astype(np.int32)

    raster = np.asarray(
        rasterio.features.rasterize(
            list(zip(gdf.geometry, gdf["_zint"], strict=True)),
            out_shape=shape,
            transform=transform,
            fill=0,
            dtype="int32",
        ),
        dtype=np.int32,
    )

    int_to_id = {v: k for k, v in id_to_int.items()}
    logger.info("%s raster: %d zones, %d cells", id_col, len(unique_ids), int((raster > 0).sum()))
    return raster, int_to_id


def _cross_tabulate(
    weight_arr: np.ndarray,
    source_arr: np.ndarray,
    int_to_source: dict[int, str],
    transform: rasterio.transform.Affine,
    target_gdf: gpd.GeoDataFrame,
) -> pl.DataFrame:
    """Cross-tabulate weights by source zone x target zone via exactextract.

    Both arrays are written as bands in a single GeoTIFF so that
    ``exactextract`` returns aligned per-cell arrays.  Per-feature
    accumulation uses ``np.bincount`` for a single vectorized pass.

    Returns columns ``[source_id, target_id, population]``.
    """
    with _temp_tif(weight_arr, source_arr, transform=transform, dtypes=["float32", "int32"]) as tif:
        ee_features: list[dict] = exact_extract(
            tif,
            target_gdf,
            ["values", "coverage"],
            include_cols=["target_id"],
        )  # pyright: ignore[reportAssignmentType]

    # band_1 = weight values / coverage, band_2 = source-zone integer labels
    n_src = max(int_to_source) + 1
    rows: list[dict[str, object]] = []
    for feat in ee_features:
        props = feat["properties"]
        wt_vals = np.asarray(props["band_1_values"], dtype=np.float64)
        src_vals = np.asarray(props["band_2_values"], dtype=np.int32)
        coverage = np.asarray(props["band_1_coverage"], dtype=np.float64)

        mask = src_vals > 0
        if not mask.any():
            continue
        totals = np.bincount(
            src_vals[mask],
            weights=(wt_vals * coverage)[mask],
            minlength=n_src,
        )

        tid = str(props["target_id"])
        for src_int, src_id in int_to_source.items():
            if totals[src_int] > 0:
                rows.append(
                    {
                        "source_id": src_id,
                        "target_id": tid,
                        "population": float(totals[src_int]),
                    }
                )

    if not rows:
        msg = "Cross-tabulation produced no results. Check source/target/weight overlap."
        raise ValueError(msg)

    return pl.DataFrame(rows).with_columns(
        pl.col("source_id").cast(pl.Utf8),
        pl.col("target_id").cast(pl.Utf8),
        pl.col("population").cast(pl.Float64),
    )


@contextlib.contextmanager
def _temp_tif(
    *arrays: np.ndarray,
    transform: rasterio.transform.Affine,
    dtypes: list[str] | None = None,
) -> Generator[str, None, None]:
    """Context manager: write bands to an in-memory GeoTIFF (GDAL /vsimem/).

    A single multi-band file ensures ``exactextract`` returns aligned
    per-cell arrays for every band.  Using ``rasterio.MemoryFile``
    avoids transient Windows temp-file read errors.
    """
    count = len(arrays)
    if dtypes is None:
        dtypes = ["float32"] * count
    memfile = MemoryFile()
    try:
        with memfile.open(
            driver="GTiff",
            height=arrays[0].shape[0],
            width=arrays[0].shape[1],
            count=count,
            dtype=dtypes[0],
            crs=_ALBERS_CRS,
            transform=transform,
        ) as dst:
            for i, (arr, dt) in enumerate(zip(arrays, dtypes, strict=True), start=1):
                dst.write(arr.astype(dt), i)
        yield memfile.name
    finally:
        memfile.close()
