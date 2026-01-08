"""Tests for add_zone_ids module."""

import geopandas as gpd
import polars as pl
import pytest
from shapely.geometry import Polygon

from processing.add_zone_ids.add_zone_ids import add_zone_ids, add_zone_to_dataframe


class TestAddZoneToDataframe:
    """Test add_zone_to_dataframe helper function."""

    def test_add_zone_to_points_within_polygon(self):
        """Test adding zone IDs to points within polygons."""
        # Create test dataframe with points
        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "lon": [0.5, 1.5, 2.5],
                "lat": [0.5, 1.5, 2.5],
            }
        )

        # Create zones (square polygons)
        zones_gdf = gpd.GeoDataFrame(
            {"zone_id": ["Z1", "Z2", "Z3"]},
            geometry=[
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),  # Zone 1: 0-1
                Polygon([(1, 1), (2, 1), (2, 2), (1, 2)]),  # Zone 2: 1-2
                Polygon([(2, 2), (3, 2), (3, 3), (2, 3)]),  # Zone 3: 2-3
            ],
            crs="EPSG:4326",
        )

        result = add_zone_to_dataframe(
            df=df,
            df_index="id",
            shp=zones_gdf,
            lon_col="lon",
            lat_col="lat",
            zone_col_name="zone",
            zone_id_field="zone_id",
        )

        assert "zone" in result.columns
        assert result["zone"][0] == "Z1"
        assert result["zone"][1] == "Z2"
        assert result["zone"][2] == "Z3"

    def test_add_zone_to_points_outside_polygons(self):
        """Test handling points outside all zones."""
        df = pl.DataFrame(
            {
                "id": [1, 2],
                "lon": [0.5, 10.0],  # Second point way outside
                "lat": [0.5, 10.0],
            }
        )

        zones_gdf = gpd.GeoDataFrame(
            {"zone_id": ["Z1"]},
            geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
            crs="EPSG:4326",
        )

        result = add_zone_to_dataframe(
            df=df,
            df_index="id",
            shp=zones_gdf,
            lon_col="lon",
            lat_col="lat",
            zone_col_name="zone",
            zone_id_field="zone_id",
        )

        assert result["zone"][0] == "Z1"
        assert result["zone"][1] is None  # Outside zone

    def test_preserves_original_columns(self):
        """Test that original dataframe columns are preserved."""
        df = pl.DataFrame(
            {
                "id": [1, 2],
                "name": ["Alice", "Bob"],
                "lon": [0.5, 1.5],
                "lat": [0.5, 1.5],
            }
        )

        zones_gdf = gpd.GeoDataFrame(
            {"zone_id": ["Z1"]},
            geometry=[Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])],
            crs="EPSG:4326",
        )

        result = add_zone_to_dataframe(
            df=df,
            df_index="id",
            shp=zones_gdf,
            lon_col="lon",
            lat_col="lat",
            zone_col_name="zone",
            zone_id_field="zone_id",
        )

        assert "id" in result.columns
        assert "name" in result.columns
        assert "zone" in result.columns
        assert result["name"][0] == "Alice"

    def test_zone_id_converted_to_string(self):
        """Test that zone IDs are converted to strings."""
        df = pl.DataFrame(
            {
                "id": [1],
                "lon": [0.5],
                "lat": [0.5],
            }
        )

        # Create zones with integer IDs
        zones_gdf = gpd.GeoDataFrame(
            {"zone_id": [100]},  # Integer zone ID
            geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
            crs="EPSG:4326",
        )

        result = add_zone_to_dataframe(
            df=df,
            df_index="id",
            shp=zones_gdf,
            lon_col="lon",
            lat_col="lat",
            zone_col_name="zone",
            zone_id_field="zone_id",
        )

        # Should be integer
        assert result["zone"][0] == 100


class TestAddZoneIds:
    """Test add_zone_ids function."""

    @pytest.fixture
    def sample_households(self):
        """Sample households data."""
        return pl.DataFrame(
            {
                "hh_id": [1, 2],
                "home_lon": [0.5, 1.5],
                "home_lat": [0.5, 1.5],
            }
        )

    @pytest.fixture
    def sample_persons(self):
        """Sample persons data."""
        return pl.DataFrame(
            {
                "person_id": [1, 2, 3],
                "hh_id": [1, 1, 2],
                "work_lon": [0.5, 1.5, 2.5],
                "work_lat": [0.5, 1.5, 2.5],
                "school_lon": [0.5, 1.5, 2.5],
                "school_lat": [0.5, 1.5, 2.5],
            }
        )

    @pytest.fixture
    def sample_trips(self):
        """Sample linked trips data."""
        return pl.DataFrame(
            {
                "trip_id": [1, 2],
                "o_lon": [0.5, 1.5],
                "o_lat": [0.5, 1.5],
                "d_lon": [1.5, 2.5],
                "d_lat": [1.5, 2.5],
            }
        )

    @pytest.fixture
    def zone_shapefile(self, tmp_path):
        """Create a test zone shapefile."""
        zones_gdf = gpd.GeoDataFrame(
            {"taz_id": ["TAZ1", "TAZ2", "TAZ3"]},
            geometry=[
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                Polygon([(1, 1), (2, 1), (2, 2), (1, 2)]),
                Polygon([(2, 2), (3, 2), (3, 3), (2, 3)]),
            ],
            crs="EPSG:4326",
        )

        shp_path = tmp_path / "zones.shp"
        zones_gdf.to_file(shp_path)
        return str(shp_path)

    def test_add_single_zone_geography(
        self, sample_households, sample_persons, sample_trips, zone_shapefile
    ):
        """Test adding a single zone geography."""
        zone_geographies = [
            {
                "shapefile": zone_shapefile,
                "zone_id_field": "taz_id",
                "zone_name": "taz",
            }
        ]

        result = add_zone_ids(
            households=sample_households,
            persons=sample_persons,
            unlinked_trips=sample_trips,
            zone_geographies=zone_geographies,
        )

        # Check households has home_taz
        assert "home_taz" in result["households"].columns
        assert result["households"]["home_taz"][0] == "TAZ1"
        assert result["households"]["home_taz"][1] == "TAZ2"

        # Check persons has work_taz and school_taz
        assert "work_taz" in result["persons"].columns
        assert "school_taz" in result["persons"].columns
        assert result["persons"]["work_taz"][0] == "TAZ1"
        assert result["persons"]["school_taz"][2] == "TAZ3"

        # Check trips has o_taz and d_taz
        assert "o_taz" in result["unlinked_trips"].columns
        assert "d_taz" in result["unlinked_trips"].columns
        assert result["unlinked_trips"]["o_taz"][0] == "TAZ1"
        assert result["unlinked_trips"]["d_taz"][1] == "TAZ3"

    def test_add_multiple_zone_geographies(
        self, sample_households, sample_persons, sample_trips, tmp_path
    ):
        """Test adding multiple zone geographies."""
        # Create two different zone shapefiles
        taz_gdf = gpd.GeoDataFrame(
            {"taz_id": ["T1", "T2"]},
            geometry=[
                Polygon([(0, 0), (1, 0), (1, 2), (0, 2)]),
                Polygon([(1, 0), (3, 0), (3, 2), (1, 2)]),
            ],
            crs="EPSG:4326",
        )
        taz_path = tmp_path / "taz.shp"
        taz_gdf.to_file(taz_path)

        county_gdf = gpd.GeoDataFrame(
            {"county_id": ["C1"]},
            geometry=[Polygon([(0, 0), (3, 0), (3, 3), (0, 3)])],
            crs="EPSG:4326",
        )
        county_path = tmp_path / "county.shp"
        county_gdf.to_file(county_path)

        zone_geographies = [
            {"shapefile": str(taz_path), "zone_id_field": "taz_id", "zone_name": "taz"},
            {"shapefile": str(county_path), "zone_id_field": "county_id", "zone_name": "county"},
        ]

        result = add_zone_ids(
            households=sample_households,
            persons=sample_persons,
            unlinked_trips=sample_trips,
            zone_geographies=zone_geographies,
        )

        # Check both zone types were added
        assert "home_taz" in result["households"].columns
        assert "home_county" in result["households"].columns
        assert "work_taz" in result["persons"].columns
        assert "work_county" in result["persons"].columns

    def test_replaces_existing_zone_column(
        self, sample_households, sample_persons, sample_trips, zone_shapefile
    ):
        """Test that existing zone columns are replaced with warning."""
        # Add existing zone column
        sample_households = sample_households.with_columns(pl.lit("OLD_VALUE").alias("home_taz"))

        zone_geographies = [
            {
                "shapefile": zone_shapefile,
                "zone_id_field": "taz_id",
                "zone_name": "taz",
            }
        ]

        result = add_zone_ids(
            households=sample_households,
            persons=sample_persons,
            unlinked_trips=sample_trips,
            zone_geographies=zone_geographies,
        )

        # Should have replaced the old value
        assert result["households"]["home_taz"][0] == "TAZ1"
        assert result["households"]["home_taz"][0] != "OLD_VALUE"

    def test_preserves_original_columns(
        self, sample_households, sample_persons, sample_trips, zone_shapefile
    ):
        """Test that original columns are preserved."""
        zone_geographies = [
            {
                "shapefile": zone_shapefile,
                "zone_id_field": "taz_id",
                "zone_name": "taz",
            }
        ]

        result = add_zone_ids(
            households=sample_households,
            persons=sample_persons,
            unlinked_trips=sample_trips,
            zone_geographies=zone_geographies,
        )

        # Original columns should still exist
        assert "hh_id" in result["households"].columns
        assert "person_id" in result["persons"].columns
        assert "trip_id" in result["unlinked_trips"].columns
        assert "home_lon" in result["households"].columns

    def test_returns_all_three_tables(
        self, sample_households, sample_persons, sample_trips, zone_shapefile
    ):
        """Test that all three tables are returned."""
        zone_geographies = [
            {
                "shapefile": zone_shapefile,
                "zone_id_field": "taz_id",
                "zone_name": "taz",
            }
        ]

        result = add_zone_ids(
            households=sample_households,
            persons=sample_persons,
            unlinked_trips=sample_trips,
            zone_geographies=zone_geographies,
        )

        assert "households" in result
        assert "persons" in result
        assert "unlinked_trips" in result
        assert isinstance(result["households"], pl.DataFrame)
        assert isinstance(result["persons"], pl.DataFrame)
        assert isinstance(result["unlinked_trips"], pl.DataFrame)
