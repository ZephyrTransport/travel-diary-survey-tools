"""Tests for read_write module."""

import geopandas as gpd
import polars as pl
import pytest
from shapely.geometry import Point

from data_canon.core.dataclass import CanonicalData
from processing.read_write.read_write import load_data, write_data


class TestLoadData:
    """Test load_data function."""

    def test_load_csv(self, tmp_path):
        """Test loading CSV files."""
        # Create test CSV
        csv_path = tmp_path / "test.csv"
        df = pl.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        df.write_csv(csv_path)

        input_paths = {"test_table": str(csv_path)}
        result = load_data(input_paths=input_paths)

        assert "test_table" in result
        assert isinstance(result["test_table"], pl.DataFrame)
        assert len(result["test_table"]) == 3
        assert result["test_table"]["id"].to_list() == [1, 2, 3]

    def test_load_parquet(self, tmp_path):
        """Test loading Parquet files."""
        # Create test Parquet
        parquet_path = tmp_path / "test.parquet"
        df = pl.DataFrame({"id": [1, 2, 3], "value": [10.5, 20.5, 30.5]})
        df.write_parquet(parquet_path)

        input_paths = {"test_table": str(parquet_path)}
        result = load_data(input_paths=input_paths)

        assert "test_table" in result
        assert isinstance(result["test_table"], pl.DataFrame)
        assert len(result["test_table"]) == 3

    def test_load_shapefile(self, tmp_path):
        """Test loading shapefiles."""
        # Create test shapefile
        shp_path = tmp_path / "test.shp"
        gdf = gpd.GeoDataFrame(
            {"id": [1, 2], "name": ["A", "B"]},
            geometry=[Point(0, 0), Point(1, 1)],
            crs="EPSG:4326",
        )
        gdf.to_file(shp_path)

        input_paths = {"zones": str(shp_path)}
        result = load_data(input_paths=input_paths)

        assert "zones" in result
        assert isinstance(result["zones"], gpd.GeoDataFrame)
        assert len(result["zones"]) == 2

    def test_load_geojson(self, tmp_path):
        """Test loading GeoJSON files."""
        # Create test GeoJSON
        geojson_path = tmp_path / "test.geojson"
        gdf = gpd.GeoDataFrame(
            {"id": [1, 2], "name": ["A", "B"]},
            geometry=[Point(0, 0), Point(1, 1)],
            crs="EPSG:4326",
        )
        gdf.to_file(geojson_path, driver="GeoJSON")

        input_paths = {"zones": str(geojson_path)}
        result = load_data(input_paths=input_paths)

        assert "zones" in result
        assert isinstance(result["zones"], gpd.GeoDataFrame)

    def test_load_multiple_files(self, tmp_path):
        """Test loading multiple files at once."""
        # Create multiple files
        csv_path = tmp_path / "data.csv"
        parquet_path = tmp_path / "data.parquet"

        pl.DataFrame({"id": [1, 2]}).write_csv(csv_path)
        pl.DataFrame({"id": [3, 4]}).write_parquet(parquet_path)

        input_paths = {"csv_data": str(csv_path), "parquet_data": str(parquet_path)}
        result = load_data(input_paths=input_paths)

        assert len(result) == 2
        assert "csv_data" in result
        assert "parquet_data" in result

    def test_load_nonexistent_file_raises_error(self, tmp_path):
        """Test that loading non-existent file raises FileNotFoundError."""
        input_paths = {"test": str(tmp_path / "nonexistent.csv")}

        with pytest.raises(FileNotFoundError, match="does not exist"):
            load_data(input_paths=input_paths)

    def test_load_unsupported_format_raises_error(self, tmp_path):
        """Test that unsupported file format raises ValueError."""
        # Create a file with unsupported extension
        unsupported_path = tmp_path / "test.xlsx"
        unsupported_path.write_text("dummy content")

        input_paths = {"test": str(unsupported_path)}

        with pytest.raises(ValueError, match="Unsupported file format"):
            load_data(input_paths=input_paths)

    def test_load_with_broken_path_trace(self, tmp_path):
        """Test error message includes broken path information."""
        # Use a deeply nested nonexistent path
        broken_path = tmp_path / "nonexistent_dir" / "subdir" / "file.csv"

        input_paths = {"test": str(broken_path)}

        with pytest.raises(FileNotFoundError, match="Possibly broken at"):
            load_data(input_paths=input_paths)


class TestWriteData:
    """Test write_data function."""

    def test_write_csv(self, tmp_path):
        """Test writing CSV files."""
        output_path = tmp_path / "output.csv"
        canonical_data = CanonicalData()
        canonical_data.households = pl.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

        output_paths = {"households": str(output_path)}
        write_data(
            output_paths=output_paths,
            canonical_data=canonical_data,
            validate_input=False,
        )

        assert output_path.exists()
        # Verify content
        loaded = pl.read_csv(output_path)
        assert len(loaded) == 3
        assert loaded["id"].to_list() == [1, 2, 3]

    def test_write_parquet(self, tmp_path):
        """Test writing Parquet files."""
        output_path = tmp_path / "output.parquet"
        canonical_data = CanonicalData()
        canonical_data.persons = pl.DataFrame({"id": [1, 2], "age": [25, 30]})

        output_paths = {"persons": str(output_path)}
        write_data(
            output_paths=output_paths,
            canonical_data=canonical_data,
            validate_input=False,
        )

        assert output_path.exists()
        loaded = pl.read_parquet(output_path)
        assert len(loaded) == 2

    def test_write_creates_directories(self, tmp_path):
        """Test that write_data creates parent directories."""
        output_path = tmp_path / "subdir1" / "subdir2" / "output.csv"
        canonical_data = CanonicalData()
        canonical_data.trips = pl.DataFrame({"id": [1]})  # pyright: ignore[reportAttributeAccessIssue]

        output_paths = {"trips": str(output_path)}
        write_data(
            output_paths=output_paths,
            canonical_data=canonical_data,
            validate_input=False,
            create_dirs=True,
        )

        assert output_path.exists()
        assert output_path.parent.exists()

    def test_write_without_creating_directories_fails(self, tmp_path):
        """Test that write fails if directories don't exist and create_dirs=False."""
        output_path = tmp_path / "nonexistent" / "output.csv"
        canonical_data = CanonicalData()
        canonical_data.households = pl.DataFrame({"id": [1]})

        output_paths = {"households": str(output_path)}

        # Will raise OS error for missing directory
        with pytest.raises(
            OSError, match=r"No such file or directory|cannot find the path specified"
        ):
            write_data(
                output_paths=output_paths,
                canonical_data=canonical_data,
                validate_input=False,
                create_dirs=False,
            )

    def test_write_multiple_tables(self, tmp_path):
        """Test writing multiple tables."""
        canonical_data = CanonicalData()
        canonical_data.households = pl.DataFrame({"id": [1, 2]})
        canonical_data.persons = pl.DataFrame({"id": [10, 20]})

        output_paths = {
            "households": str(tmp_path / "households.csv"),
            "persons": str(tmp_path / "persons.parquet"),
        }
        write_data(
            output_paths=output_paths,
            canonical_data=canonical_data,
            validate_input=False,
        )

        assert (tmp_path / "households.csv").exists()
        assert (tmp_path / "persons.parquet").exists()

    def test_write_unsupported_format_raises_error(self, tmp_path):
        """Test that unsupported output format raises ValueError."""
        output_path = tmp_path / "output.xlsx"
        canonical_data = CanonicalData()
        canonical_data.households = pl.DataFrame({"id": [1]})

        output_paths = {"households": str(output_path)}

        with pytest.raises(ValueError, match="Unsupported file format"):
            write_data(
                output_paths=output_paths,
                canonical_data=canonical_data,
                validate_input=False,
            )

    def test_write_geodataframe(self, tmp_path):
        """Test writing GeoDataFrame to shapefile."""
        output_path = tmp_path / "output.shp"
        canonical_data = CanonicalData()

        # Create a GeoDataFrame
        gdf = gpd.GeoDataFrame(
            {"id": [1, 2]},
            geometry=[Point(0, 0), Point(1, 1)],
            crs="EPSG:4326",
        )
        canonical_data.zones = gdf  # pyright: ignore[reportAttributeAccessIssue]

        output_paths = {"zones": str(output_path)}
        write_data(
            output_paths=output_paths,
            canonical_data=canonical_data,
            validate_input=False,
        )

        assert output_path.exists()
        # Verify content
        loaded = gpd.read_file(output_path)
        assert len(loaded) == 2

    def test_write_text_file(self, tmp_path):
        """Test writing text files."""
        output_path = tmp_path / "output.txt"
        canonical_data = CanonicalData()
        canonical_data.summary = "Test summary content"  # pyright: ignore[reportAttributeAccessIssue]

        output_paths = {"summary": str(output_path)}
        write_data(
            output_paths=output_paths,
            canonical_data=canonical_data,
            validate_input=False,
        )

        assert output_path.exists()
        assert output_path.read_text() == "Test summary content"
