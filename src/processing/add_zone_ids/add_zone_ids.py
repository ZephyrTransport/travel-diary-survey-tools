"""Add zone IDs to households, persons, and linked trips based on geographic locations."""

import logging

import geopandas as gpd
import polars as pl

from pipeline.decoration import step

logger = logging.getLogger(__name__)


# Helper function to add zone ID to a dataframe based on lon/lat
def add_zone_to_dataframe(
    df: pl.DataFrame,
    shp: gpd.GeoDataFrame,
    df_index: str,
    lon_col: str,
    lat_col: str,
    zone_col_name: str,
    zone_id_field: str,
) -> pl.DataFrame:
    """Add zone ID to dataframe based on lon/lat coordinates."""
    # Convert to GeoDataFrame
    # Keep just index to avoid corrupting original polars DataFrame with pandas nonsense
    gdf = gpd.GeoDataFrame(
        index=df[df_index].to_list(),
        geometry=gpd.points_from_xy(df[lon_col].to_list(), df[lat_col].to_list()),
        crs="EPSG:4326",
    )
    gdf.index.name = df_index

    # Prepare shapefile for spatial join and ensure zone ID is string to handle nulls in pandas land
    shp_prepared = shp.loc[:, [zone_id_field, "geometry"]].copy()
    shp_prepared[zone_id_field] = shp_prepared[zone_id_field].astype(str)
    shp_prepared = shp_prepared.set_index(zone_id_field)

    # Spatial join to find zone containing each point
    gdf_joined = gpd.sjoin(gdf, shp_prepared, how="left", predicate="within")
    gdf_joined = gdf_joined.rename(columns={zone_id_field: zone_col_name})
    gdf_joined = gdf_joined.drop(columns="geometry")

    # If all zone IDs are integers, convert to Int64 to allow nulls
    # else keep as string
    casttype = pl.Utf8
    if gdf_joined[zone_col_name].dropna().apply(lambda x: x.isdigit()).all():
        casttype = pl.Int64

    # Join back to original polars DataFrame on index
    df_joined = df.join(
        pl.from_pandas(gdf_joined.reset_index()),
        on=df_index,
        how="left",
    ).with_columns(pl.col(zone_col_name).cast(casttype))

    return df_joined


@step()
def add_zone_ids(
    zone_geographies: list[dict],
    households: pl.DataFrame | None = None,
    persons: pl.DataFrame | None = None,
    unlinked_trips: pl.DataFrame | None = None,
    linked_trips: pl.DataFrame | None = None,
    tours: pl.DataFrame | None = None,
    joint_trips: pl.DataFrame | None = None,
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
        zone_geographies: List of dicts, each containing:
            - shapefile: Path to shapefile with zone boundaries (str)
            - zone_id_field: Field name in shapefile for zone ID
            - zone_name: Short name for zone type (e.g., 'taz', 'maz', 'county')

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
    # Process each zone geography
    for zone_config in zone_geographies:
        shapefile_path = zone_config["shapefile"]
        zone_id_field = zone_config["zone_id_field"]
        zone_name = zone_config["zone_name"]

        # Load the shapefile
        shapefile = gpd.read_file(shapefile_path)

        # Standard location mappings: (table, table_index, lon_col, lat_col, location_prefix)
        standard_locations = [
            ("households", "hh_id", "home_lon", "home_lat", "home"),
            ("persons", "person_id", "work_lon", "work_lat", "work"),
            ("persons", "person_id", "school_lon", "school_lat", "school"),
            ("unlinked_trips", "trip_id", "o_lon", "o_lat", "o"),
            ("unlinked_trips", "trip_id", "d_lon", "d_lat", "d"),
            ("linked_trips", "linked_trip_id", "o_lon", "o_lat", "o"),
            ("linked_trips", "linked_trip_id", "d_lon", "d_lat", "d"),
            ("tours", "tour_id", "o_lon", "o_lat", "o"),
            ("tours", "tour_id", "d_lon", "d_lat", "d"),
            ("joint_trips", "joint_trip_id", "o_lon_mean", "o_lat_mean", "o"),
            ("joint_trips", "joint_trip_id", "d_lon_mean", "d_lat_mean", "d"),
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

            results[table] = add_zone_to_dataframe(
                df,
                shapefile,
                df_index=idx,
                lon_col=lon_col,
                lat_col=lat_col,
                zone_col_name=output_col,
                zone_id_field=zone_id_field,
            )

    return results
