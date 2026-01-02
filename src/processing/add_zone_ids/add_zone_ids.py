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
    lon_col: str,
    lat_col: str,
    zone_col_name: str,
    zone_id_field: str,
) -> pl.DataFrame:
    """Add zone ID to dataframe based on lon/lat coordinates."""
    gdf = gpd.GeoDataFrame(
        df.to_pandas(),
        geometry=gpd.points_from_xy(df[lon_col].to_list(), df[lat_col].to_list()),
        crs="EPSG:4326",
    )

    # Prepare shapefile for spatial join and ensure zone ID is string
    shp_prepared = shp.loc[:, [zone_id_field, "geometry"]].copy()
    shp_prepared[zone_id_field] = shp_prepared[zone_id_field].astype(str)
    shp_prepared = shp_prepared.set_index(zone_id_field)

    # Spatial join to find zone containing each point
    gdf_joined = gpd.sjoin(gdf, shp_prepared, how="left", predicate="within")
    gdf_joined = gdf_joined.rename(columns={zone_id_field: zone_col_name})
    gdf_joined = gdf_joined.drop(columns="geometry")

    return pl.from_pandas(gdf_joined)


@step()
def add_zone_ids(
    households: pl.DataFrame,
    persons: pl.DataFrame,
    linked_trips: pl.DataFrame,
    zone_geographies: list[dict],
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
        linked_trips: Linked trips dataframe
        zone_geographies: List of dicts, each containing:
            - shapefile: Path to shapefile with zone boundaries (str)
            - zone_id_field: Field name in shapefile for zone ID
            - zone_name: Short name for zone type (e.g., 'taz', 'maz', 'county')

    Returns:
        Dictionary with updated dataframes
    """
    results = {
        "households": households,
        "persons": persons,
        "linked_trips": linked_trips,
    }

    # Process each zone geography
    for zone_config in zone_geographies:
        shapefile_path = zone_config["shapefile"]
        zone_id_field = zone_config["zone_id_field"]
        zone_name = zone_config["zone_name"]

        logger.info(
            "Adding %s IDs using field '%s' from %s",
            zone_name.upper(),
            zone_id_field,
            shapefile_path,
        )

        # Load the shapefile
        shapefile = gpd.read_file(shapefile_path)

        # Standard location mappings: (table, lon_col, lat_col, location_prefix)
        standard_locations = [
            ("households", "home_lon", "home_lat", "home"),
            ("persons", "work_lon", "work_lat", "work"),
            ("persons", "school_lon", "school_lat", "school"),
            ("linked_trips", "o_lon", "o_lat", "o"),
            ("linked_trips", "d_lon", "d_lat", "d"),
        ]

        # Apply this zone geography to all standard locations
        for table, lon_col, lat_col, location_prefix in standard_locations:
            output_col = f"{location_prefix}_{zone_name}"

            if output_col in results[table].columns:
                logger.warning(
                    "Column %s already exists in %s; replacing it.",
                    output_col,
                    table,
                )
                results[table] = results[table].drop(output_col)

            results[table] = add_zone_to_dataframe(
                results[table],
                shapefile,
                lon_col=lon_col,
                lat_col=lat_col,
                zone_col_name=output_col,
                zone_id_field=zone_id_field,
            )

    return results
