"""Tests for add_zone_ids module."""

import geopandas as gpd
import polars as pl
import pytest
from shapely.geometry import Polygon

from processing.add_zone_ids.add_zone_ids import add_zone_ids, add_zone_to_dataframe

# Three unit squares on the diagonal: (0-1, 0-1), (1-2, 1-2), (2-3, 2-3).
SQUARES = [
    Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
    Polygon([(1, 1), (2, 1), (2, 2), (1, 2)]),
    Polygon([(2, 2), (3, 2), (3, 3), (2, 3)]),
]


def _zones(field: str, ids: list, polygons: list[Polygon] = SQUARES) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame({field: ids}, geometry=polygons[: len(ids)], crs="EPSG:4326")


def _shapefile(path, field: str, ids: list, polygons: list[Polygon] = SQUARES) -> str:
    _zones(field, ids, polygons).to_file(path)
    return str(path)


def _geography(shapefile: str, field: str, name: str = "taz") -> dict:
    return {"shapefile": shapefile, "zone_id_field": field, "zone_name": name}


def _zone_points(df: pl.DataFrame, zones: gpd.GeoDataFrame) -> pl.DataFrame:
    return add_zone_to_dataframe(
        df=df,
        df_index="id",
        shp=zones,
        lon_col="lon",
        lat_col="lat",
        zone_col_name="zone",
        zone_id_field="zone_id",
    )


class TestAddZoneToDataframe:
    """Test add_zone_to_dataframe helper function."""

    def test_points_get_the_zone_they_fall_in(self):
        """Each point gets its polygon's id; a point outside every polygon gets null."""
        df = pl.DataFrame(
            {"id": [1, 2, 3, 4], "lon": [0.5, 1.5, 2.5, 10.0], "lat": [0.5, 1.5, 2.5, 10.0]}
        )

        result = _zone_points(df, _zones("zone_id", ["Z1", "Z2", "Z3"]))

        assert result["zone"].to_list() == ["Z1", "Z2", "Z3", None]

    def test_all_numeric_zone_ids_become_int64(self):
        """Zone ids that are all digits are cast to Int64, which admits nulls."""
        df = pl.DataFrame({"id": [1], "lon": [0.5], "lat": [0.5]})

        result = _zone_points(df, _zones("zone_id", [100]))

        assert result["zone"].dtype == pl.Int64
        assert result["zone"][0] == 100


class TestAddZoneIds:
    """Test add_zone_ids function."""

    @pytest.fixture
    def tables(self) -> dict[str, pl.DataFrame]:
        """Two households, three persons and two unlinked trips."""
        return {
            "households": pl.DataFrame(
                {"hh_id": [1, 2], "home_lon": [0.5, 1.5], "home_lat": [0.5, 1.5]}
            ),
            "persons": pl.DataFrame(
                {
                    "person_id": [1, 2, 3],
                    "hh_id": [1, 1, 2],
                    "work_lon": [0.5, 1.5, 2.5],
                    "work_lat": [0.5, 1.5, 2.5],
                    "school_lon": [0.5, 1.5, 2.5],
                    "school_lat": [0.5, 1.5, 2.5],
                }
            ),
            "unlinked_trips": pl.DataFrame(
                {
                    "unlinked_trip_id": [1, 2],
                    "o_lon": [0.5, 1.5],
                    "o_lat": [0.5, 1.5],
                    "d_lon": [1.5, 2.5],
                    "d_lat": [1.5, 2.5],
                }
            ),
        }

    @pytest.fixture(scope="class")
    def zone_shapefile(self, tmp_path_factory) -> str:
        """A three-zone shapefile with ids 1-3, written once for the whole class."""
        return _shapefile(tmp_path_factory.mktemp("zones") / "zones.shp", "taz_id", [1, 2, 3])

    def test_add_single_zone_geography(self, tables, zone_shapefile):
        """Homes, workplaces, schools and trip ends each get a ``<prefix>_taz`` column."""
        result = add_zone_ids(**tables, zone_geographies=[_geography(zone_shapefile, "taz_id")])

        assert result["households"]["home_taz"].to_list() == [1, 2]
        assert result["persons"]["work_taz"][0] == 1
        assert result["persons"]["school_taz"][2] == 3
        assert result["unlinked_trips"]["o_taz"][0] == 1
        assert result["unlinked_trips"]["d_taz"][1] == 3

    def test_habitual_locations_get_their_zone(self, zone_shapefile):
        """Bare ``lat``/``lon`` coordinates get a zone column named by the bare zone name."""
        habitual_locations = pl.DataFrame(
            {"habitual_location_id": [1, 2], "lon": [0.5, 2.5], "lat": [0.5, 2.5]}
        )

        result = add_zone_ids(
            habitual_locations=habitual_locations,
            zone_geographies=[_geography(zone_shapefile, "taz_id")],
        )

        assert result["habitual_locations"]["taz"].to_list() == [1, 3]

    def test_add_multiple_zone_geographies(self, tables, tmp_path):
        """Each geography adds its own column, named by its zone name."""
        taz = [Polygon([(0, 0), (1, 0), (1, 2), (0, 2)]), Polygon([(1, 0), (3, 0), (3, 2), (1, 2)])]
        county = [Polygon([(0, 0), (3, 0), (3, 3), (0, 3)])]
        zone_geographies = [
            _geography(_shapefile(tmp_path / "taz.shp", "taz_id", ["T1", "T2"], taz), "taz_id"),
            _geography(
                _shapefile(tmp_path / "county.shp", "county_id", ["C1"], county),
                "county_id",
                "county",
            ),
        ]

        result = add_zone_ids(**tables, zone_geographies=zone_geographies)

        # The third workplace is north of the TAZ layer, so it has no TAZ.
        assert result["households"]["home_taz"].to_list() == ["T1", "T2"]
        assert result["persons"]["work_taz"].to_list() == ["T1", "T2", None]
        assert result["households"]["home_county"].to_list() == ["C1", "C1"]
        assert result["persons"]["work_county"].to_list() == ["C1", "C1", "C1"]

    def test_string_zone_ids(self, tables, tmp_path):
        """Zone ids that are not numeric stay text."""
        shapefile = _shapefile(tmp_path / "zones.shp", "zone_id", ["TAZ_A", "TAZ_B", "TAZ_C"])

        result = add_zone_ids(**tables, zone_geographies=[_geography(shapefile, "zone_id")])

        assert result["households"]["home_taz"].to_list() == ["TAZ_A", "TAZ_B"]
        assert result["persons"]["work_taz"][2] == "TAZ_C"

    def test_replaces_existing_zone_column(self, tables, zone_shapefile):
        """A zone column already on the table is replaced."""
        tables["households"] = tables["households"].with_columns(pl.lit(999).alias("home_taz"))

        result = add_zone_ids(**tables, zone_geographies=[_geography(zone_shapefile, "taz_id")])

        assert result["households"]["home_taz"][0] == 1
