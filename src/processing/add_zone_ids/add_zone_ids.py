"""Add zone IDs to households, persons, and linked trips based on geographic locations."""

import logging

import geopandas as gpd
import polars as pl

from data_canon.core.dataclass import CanonicalData
from pipeline.decoration import step

logger = logging.getLogger(__name__)


def prepare_zones(shp: gpd.GeoDataFrame, zone_id_field: str) -> gpd.GeoDataFrame:
    """Zone polygons indexed by zone ID, as a string so nulls survive pandas."""
    zones = shp.loc[:, [zone_id_field, "geometry"]].copy()
    zones[zone_id_field] = zones[zone_id_field].astype(str)
    return zones.set_index(zone_id_field)


def prepare_snap_zones(
    shp: gpd.GeoDataFrame, zone_id_field: str, projected_crs: str
) -> gpd.GeoDataFrame:
    """Zone polygons for the nearest-zone fallback: projected and repaired.

    make_valid() repairs self-intersecting rings that cause GEOSException in
    shapely's query_nearest. Reprojecting and repairing a large zone system
    takes seconds, so callers zoning several tables against the same shapefile
    should build this once and pass it to ``add_zone_to_dataframe``.
    """
    zones = prepare_zones(shp, zone_id_field).to_crs(projected_crs)
    zones["geometry"] = zones.geometry.make_valid()
    return zones


def zone_points(df: pl.DataFrame, df_index: str, lon_col: str, lat_col: str) -> gpd.GeoDataFrame:
    """WGS-84 points for ``df``'s coordinates, indexed by ``df_index``."""
    # Keep just index to avoid corrupting original polars DataFrame with pandas nonsense
    points = gpd.GeoDataFrame(
        index=df[df_index].to_numpy(),
        geometry=gpd.points_from_xy(df[lon_col].to_numpy(), df[lat_col].to_numpy()),
        crs="EPSG:4326",
    )
    points.index.name = df_index
    return points


# Helper function to add zone ID to a dataframe based on lon/lat
def add_zone_to_dataframe(
    df: pl.DataFrame,
    shp: gpd.GeoDataFrame,
    df_index: str,
    lon_col: str,
    lat_col: str,
    zone_col_name: str,
    zone_id_field: str,
    projected_crs: str | None = None,
    max_snap_distance: float | None = None,
    zones: gpd.GeoDataFrame | None = None,
    snap_zones: gpd.GeoDataFrame | None = None,
    points: gpd.GeoDataFrame | None = None,
) -> pl.DataFrame:
    """Add a zone ID column to a Polars DataFrame via point-in-polygon spatial join.

    Each row's lon/lat coordinates are matched against the provided zone shapefile.
    If a point falls within exactly one zone polygon it receives that zone's ID.

    Nearest-neighbour fallback
    --------------------------
    If ``projected_crs`` is provided, any unmatched point is snapped to the
    *nearest* zone polygon (by boundary distance) instead of being left null.
    Supply a metric CRS such as ``"EPSG:26910"`` (UTM Zone 10N for the Bay
    Area); operating in degrees produces incorrect distances and triggers a
    GeoPandas warning.  When ``projected_crs`` is ``None`` no snapping is
    performed and unmatched points are left null with a WARNING logged.

    Use ``max_snap_distance`` (in ``projected_crs`` units, i.e. metres for
    UTM) to distinguish GPS-drift edge cases from genuinely out-of-region
    points: a point is snapped only when the distance to the nearest zone
    boundary is ≤ ``max_snap_distance``; points farther away are left null.
    ``max_snap_distance=None`` snaps all unmatched points regardless of
    distance.

    Args:
        df: Input Polars DataFrame.
        shp: Zone boundary GeoDataFrame (any CRS; will be reprojected as needed).
        df_index: Name of the unique-ID column in ``df`` used for joining back.
        lon_col: Name of the longitude column in ``df`` (WGS-84 assumed).
        lat_col: Name of the latitude column in ``df`` (WGS-84 assumed).
        zone_col_name: Name for the output zone-ID column added to ``df``.
        zone_id_field: Field name in ``shp`` that holds the zone ID values.
        projected_crs: EPSG string for the metric CRS used during the
            nearest-neighbour fallback (e.g. ``"EPSG:26910"``).  If ``None``,
            no fallback is performed and unmatched points remain null.
        canonical_data: Canonical data container. When present, each generated
            ``{prefix}_{zone_name}`` column is registered on it so the writer
            knows the column is ours rather than vendor passthrough.
        max_snap_distance: Maximum distance (in ``projected_crs`` units, i.e.
            metres for UTM) within which an unmatched point is snapped to the
            nearest zone polygon.  Points farther than this are left null
            (assumed to be genuinely out-of-region rather than near a zone
            edge).  ``None`` means no distance limit — all unmatched points
            are snapped.  Only used when ``projected_crs`` is provided.
        zones: ``shp`` as built by ``prepare_zones`` for the same
            ``zone_id_field``. Built here when not given; passing it lets
            repeated calls reuse its spatial index.
        snap_zones: ``shp`` as built by ``prepare_snap_zones`` for the same
            ``zone_id_field`` and ``projected_crs``. Built here when not given.
        points: ``df``'s coordinates as built by ``zone_points``. Built here
            when not given; passing it lets several zone systems reuse them.

    Returns:
        ``df`` with a new ``zone_col_name`` column appended.  The column dtype
        is ``Int64`` when all non-null values are numeric, otherwise ``Utf8``.
    """
    gdf = points if points is not None else zone_points(df, df_index, lon_col, lat_col)

    # Prepare shapefile for spatial join and ensure zone ID is string to handle nulls in pandas land
    shp_prepared = zones if zones is not None else prepare_zones(shp, zone_id_field)

    # Spatial join to find zone containing each point
    gdf_joined = gpd.sjoin(gdf, shp_prepared, how="left", predicate="within")
    gdf_joined = gdf_joined.rename(columns={zone_id_field: zone_col_name})

    # Fallback: for points that didn't fall within any zone, snap to the nearest zone
    # polygon by boundary distance. Only performed when projected_crs is provided.
    # max_snap_distance filters out genuinely out-of-region points: only snap if the
    # point is within that distance of the nearest zone boundary.
    null_mask = gdf_joined[zone_col_name].isna()
    if null_mask.any():
        n_null = null_mask.sum()
        if projected_crs:
            # Project to metric CRS before nearest-neighbour join
            # to avoid incorrect results from operating in geographic CRS (degrees).
            shp_for_nearest = (
                snap_zones
                if snap_zones is not None
                else prepare_snap_zones(shp, zone_id_field, projected_crs)
            )
            points_projected = gdf[null_mask].to_crs(projected_crs)
            # Exclude points with null/NaN coordinates (e.g. persons with no work location);
            # those have no real location and should remain null regardless.
            valid_geom_mask = (
                points_projected.geometry.is_valid & ~points_projected.geometry.is_empty
            )
            points_for_nearest = points_projected[valid_geom_mask]
            if not points_for_nearest.empty:
                nearest = gpd.sjoin_nearest(
                    points_for_nearest,
                    shp_for_nearest,
                    how="left",
                    max_distance=max_snap_distance,
                ).rename(columns={zone_id_field: zone_col_name})
                # Drop duplicate rows that arise when multiple zones are equidistant
                nearest = nearest[~nearest.index.duplicated(keep="first")]
                n_snapped = nearest[zone_col_name].notna().sum()
                n_beyond = len(points_for_nearest) - n_snapped
                if n_snapped > 0:
                    logger.warning(
                        "%d point(s) did not fall within any %s zone; snapped to nearest zone.",
                        n_snapped,
                        zone_col_name,
                    )
                if n_beyond > 0:
                    logger.warning(
                        "%d point(s) did not fall within any %s zone and exceeded the snap"
                        " distance of %.0f m; leaving as null.",
                        n_beyond,
                        zone_col_name,
                        max_snap_distance,
                    )
                gdf_joined.loc[points_for_nearest.index, zone_col_name] = nearest[
                    zone_col_name
                ].values
        else:
            logger.warning(
                "%d point(s) did not fall within any %s zone; leaving as null.",
                n_null,
                zone_col_name,
            )

    gdf_joined = gdf_joined.drop(columns="geometry")

    # If all zone IDs are integers, convert to Int64 to allow nulls
    # else keep as string
    casttype = pl.Utf8
    if gdf_joined[zone_col_name].dropna().str.isdigit().all():
        casttype = pl.Int64

    # Join back to original polars DataFrame on index
    df_joined = df.join(
        pl.from_pandas(gdf_joined.reset_index()),
        on=df_index,
        how="left",
    ).with_columns(pl.col(zone_col_name).cast(casttype))

    return df_joined


@step(
    requires={
        "households": {"hh_id", "home_lon", "home_lat"},
        "persons": {"person_id", "work_lon", "work_lat", "school_lon", "school_lat"},
        "unlinked_trips": {"unlinked_trip_id", "o_lon", "o_lat", "d_lon", "d_lat"},
        "linked_trips": {"linked_trip_id", "o_lon", "o_lat", "d_lon", "d_lat"},
        "tours": {"tour_id", "o_lon", "o_lat", "d_lon", "d_lat"},
    },
)
def add_zone_ids(
    zone_geographies: list[dict],
    households: pl.DataFrame | None = None,
    persons: pl.DataFrame | None = None,
    unlinked_trips: pl.DataFrame | None = None,
    linked_trips: pl.DataFrame | None = None,
    tours: pl.DataFrame | None = None,
    joint_trips: pl.DataFrame | None = None,
    projected_crs: str | None = None,
    max_snap_distance: float | None = None,
    canonical_data: CanonicalData | None = None,
) -> dict:
    """Add zone IDs for multiple geographic levels based on locations.

    Automatically applies each zone geography to standard locations:

    - households: home_lon/lat → home_{zone_name}
    - persons: work_lon/lat → work_{zone_name},
                school_lon/lat → school_{zone_name}
    - linked_trips: o_lon/lat → o_{zone_name}, d_lon/lat → d_{zone_name}

    Args:
        households: Households dataframe
        persons: Persons dataframe
        unlinked_trips: Unlinked trips dataframe
        linked_trips: Linked trips dataframe
        tours: Tours dataframe
        joint_trips: Joint trips dataframe
        canonical_data: Canonical data container. When present, each generated
            ``{prefix}_{zone_name}`` column is registered on it, so the writer
            can tell a column the pipeline created from vendor passthrough.
        zone_geographies: List of dicts, each containing:
            - shapefile: Path to shapefile with zone boundaries (str)
            - zone_id_field: Field name in shapefile for zone ID
            - zone_name: Short name for zone type (e.g., 'taz', 'maz', 'county')
        projected_crs: Optional EPSG string (e.g. 'EPSG:26910') for the metric
            CRS to use when snapping unmatched points to the nearest zone.
            If None, no nearest-zone fallback is performed.
        max_snap_distance: Maximum snap distance in ``projected_crs`` units
            (metres for UTM).  Unmatched points farther than this from all
            zone boundaries are left null (assumed out-of-region).  None
            means no distance limit.  Only used when ``projected_crs`` is set.

    Returns:
        Dictionary with updated dataframes
    """
    # Initialize results dictionary in outer scope to update in loop, allow accumulation of zone IDs
    results = {
        "households": households,
        "persons": persons,
        "unlinked_trips": unlinked_trips,
        "linked_trips": linked_trips,
        "tours": tours,
        "joint_trips": joint_trips,
    }

    # Pre-load the shapefiles to avoid re-loading for each table
    shapefiles_cache = {}
    # Coordinates don't change between zone systems, so their points are built once
    points_cache = {}

    # Process each zone geography
    for zone_config in zone_geographies:
        shapefile_path = zone_config["shapefile"]
        zone_id_field = zone_config["zone_id_field"]
        zone_name = zone_config["zone_name"]

        # Cache the shapefile if not already loaded, avoid re-loading
        if shapefile_path not in shapefiles_cache:
            shapefiles_cache[shapefile_path] = gpd.read_file(shapefile_path)

        # Load the shapefile
        shapefile = shapefiles_cache[shapefile_path]
        # Built once per geography: rebuilding them per table cost most of the step
        zones = prepare_zones(shapefile, zone_id_field)
        snap_zones = (
            prepare_snap_zones(shapefile, zone_id_field, projected_crs) if projected_crs else None
        )

        # Standard location mappings: (table, table_index, lon_col, lat_col, location_prefix)
        standard_locations = [
            ("households", "hh_id", "home_lon", "home_lat", "home"),
            ("persons", "person_id", "work_lon", "work_lat", "work"),
            ("persons", "person_id", "school_lon", "school_lat", "school"),
            ("unlinked_trips", "unlinked_trip_id", "o_lon", "o_lat", "o"),
            ("unlinked_trips", "unlinked_trip_id", "d_lon", "d_lat", "d"),
            ("linked_trips", "linked_trip_id", "o_lon", "o_lat", "o"),
            ("linked_trips", "linked_trip_id", "d_lon", "d_lat", "d"),
            ("tours", "tour_id", "o_lon", "o_lat", "o"),
            ("tours", "tour_id", "d_lon", "d_lat", "d"),
            # joint_trips holds no coordinates of its own: it is a linking table,
            # and its members are zoned above.
        ]

        # Apply this zone geography to all standard locations
        for table, idx, lon_col, lat_col, location_prefix in standard_locations:
            output_col = f"{location_prefix}_{zone_name}"

            df = results.get(table)

            if df is None:
                # Make sure its not in results
                results.pop(table, None)
                continue  # Skip if no table specified

            logger.info(
                "Adding %s IDs on table %s using field '%s' from %s",
                zone_name.upper(),
                table,
                zone_id_field,
                shapefile_path,
            )

            if output_col in df.columns:
                logger.warning(
                    "Column %s already exists in %s; replacing it.",
                    output_col,
                    table,
                )
                df = df.drop(output_col)

            points_key = (table, lon_col, lat_col)
            if points_key not in points_cache:
                points_cache[points_key] = zone_points(df, idx, lon_col, lat_col)

            results[table] = add_zone_to_dataframe(
                df,
                shapefile,
                df_index=idx,
                lon_col=lon_col,
                lat_col=lat_col,
                zone_col_name=output_col,
                zone_id_field=zone_id_field,
                projected_crs=projected_crs,
                max_snap_distance=max_snap_distance,
                zones=zones,
                snap_zones=snap_zones,
                points=points_cache[points_key],
            )

            # The name comes from config, so no model can declare it. Say what
            # was created rather than leaving the writer to pattern-match it.
            if canonical_data is not None:
                canonical_data.register_generated_columns(table, [output_col])

            if table == "linked_trips":
                _log_joint_members_split_across_zones(results[table], output_col, zone_name)

    return results


def _log_joint_members_split_across_zones(
    linked_trips: pl.DataFrame, zone_col: str, zone_name: str
) -> None:
    """Report joint trips whose members did not all land in the same zone.

    Members of a joint trip were in the same place, so a few metres of survey
    and geocoding noise is all that separates them -- but near a boundary that
    is enough to put them in different zones. The reading is genuine, not a
    fault to repair: they really were either side of the line.

    It is reported because anything that has to name one zone for the group must
    choose, and the choice is invisible in the result. Reported per zone system,
    since a boundary that splits a group in one need not exist in another.
    """
    if "joint_trip_id" not in linked_trips.columns or zone_col not in linked_trips.columns:
        return

    members = linked_trips.filter(pl.col("joint_trip_id").is_not_null())
    if members.is_empty():
        return

    per_group = members.group_by("joint_trip_id").agg(pl.col(zone_col).n_unique().alias("_n_zones"))
    split = per_group.filter(pl.col("_n_zones") > 1).height
    if split:
        logger.warning(
            "%d of %d joint trips (%.2f%%) have members in more than one %s zone at %s; "
            "any single zone reported for the group is a choice between them.",
            split,
            per_group.height,
            100 * split / per_group.height,
            zone_name.upper(),
            zone_col,
        )
