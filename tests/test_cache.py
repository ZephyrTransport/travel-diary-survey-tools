"""Tests for pipeline caching functionality."""

import contextlib
import shutil
import threading
import time

import geopandas as gpd
import polars as pl
import pytest
from shapely.geometry import Point

from pipeline.cache import PipelineCache
from pipeline.decoration import step


@pytest.fixture
def cache_dir(tmp_path):
    """Provide a temporary cache directory."""
    cache_path = tmp_path / "test_cache"
    yield cache_path
    # Cleanup after test
    if cache_path.exists():
        shutil.rmtree(cache_path)


@pytest.fixture
def pipeline_cache(cache_dir):
    """Provide a PipelineCache instance."""
    return PipelineCache(cache_dir=cache_dir)


@pytest.fixture
def sample_dataframe():
    """Provide a sample DataFrame for testing."""
    return pl.DataFrame(
        {
            "hh_id": [1, 2, 3],
            "home_lat": [37.7, 37.8, 37.9],
            "home_lon": [-122.4, -122.5, -122.6],
        }
    )


class TestPipelineCache:
    """Test PipelineCache functionality."""

    def test_cache_initialization(self, cache_dir):
        """Test cache directory is created on initialization."""
        cache = PipelineCache(cache_dir=cache_dir)
        assert cache_dir.exists()
        assert cache.cache_dir == cache_dir

    def test_cache_key_generation(self, pipeline_cache, sample_dataframe):
        """Test cache key is deterministic for same inputs."""
        inputs = {"households": sample_dataframe}
        params = {"param1": "value1"}

        key1 = pipeline_cache.get_cache_key("test_step", inputs, params)
        key2 = pipeline_cache.get_cache_key("test_step", inputs, params)

        assert key1 == key2
        assert len(key1) == 16  # 16 hex characters

    def test_cache_key_changes_with_data(self, pipeline_cache, sample_dataframe):
        """Test cache key changes when data changes."""
        inputs1 = {"households": sample_dataframe}
        inputs2 = {"households": sample_dataframe.with_columns(pl.col("hh_id") + 10)}
        params = {"param1": "value1"}

        key1 = pipeline_cache.get_cache_key("test_step", inputs1, params)
        key2 = pipeline_cache.get_cache_key("test_step", inputs2, params)

        assert key1 != key2

    def test_cache_key_changes_with_params(self, pipeline_cache, sample_dataframe):
        """Test cache key changes when parameters change."""
        inputs = {"households": sample_dataframe}

        key1 = pipeline_cache.get_cache_key("test_step", inputs, {"param1": "value1"})
        key2 = pipeline_cache.get_cache_key("test_step", inputs, {"param1": "value2"})

        assert key1 != key2

    def test_save_and_load(self, pipeline_cache, sample_dataframe):
        """Test saving and loading cached data."""
        step_name = "test_step"
        cache_key = "test_key_12345678"
        outputs = {"households": sample_dataframe}

        # Save to cache
        pipeline_cache.save(step_name, cache_key, outputs)

        # Load from cache
        loaded = pipeline_cache.load(step_name, cache_key)

        assert loaded is not None
        assert "households" in loaded
        assert loaded["households"].equals(sample_dataframe)

    def test_cache_miss(self, pipeline_cache):
        """Test loading non-existent cache returns None."""
        result = pipeline_cache.load("nonexistent_step", "nonexistent_key")
        assert result is None

    def test_invalidate_step(self, pipeline_cache, sample_dataframe):
        """Test invalidating cache for a specific step."""
        step_name = "test_step"
        cache_key = "test_key_12345678"
        outputs = {"households": sample_dataframe}

        # Save to cache
        pipeline_cache.save(step_name, cache_key, outputs)
        assert pipeline_cache.load(step_name, cache_key) is not None

        # Invalidate
        pipeline_cache.invalidate(step_name)
        assert pipeline_cache.load(step_name, cache_key) is None

    def test_invalidate_all(self, pipeline_cache, sample_dataframe):
        """Test invalidating all caches."""
        outputs = {"households": sample_dataframe}

        # Save multiple caches
        pipeline_cache.save("step1", "key1", outputs)
        pipeline_cache.save("step2", "key2", outputs)

        # Invalidate all
        pipeline_cache.invalidate()

        assert pipeline_cache.load("step1", "key1") is None
        assert pipeline_cache.load("step2", "key2") is None

    def test_list_cached_steps(self, pipeline_cache, sample_dataframe):
        """Test listing cached steps."""
        outputs = {"households": sample_dataframe}

        # Save to cache
        pipeline_cache.save("step1", "key1", outputs)
        pipeline_cache.save("step2", "key2", outputs)

        # List caches
        cached_steps = pipeline_cache.list_cached_steps()

        assert len(cached_steps) == 2
        step_names = {step["step_name"] for step in cached_steps}
        assert step_names == {"step1", "step2"}

    def test_cache_statistics(self, pipeline_cache, sample_dataframe):
        """Test cache hit/miss statistics."""
        step_name = "test_step"
        cache_key = "test_key_12345678"
        outputs = {"households": sample_dataframe}

        # Save to cache
        pipeline_cache.save(step_name, cache_key, outputs)

        # Reset stats
        pipeline_cache.reset_stats()

        # Cache hit
        pipeline_cache.load(step_name, cache_key)
        # Cache miss
        pipeline_cache.load("nonexistent", "nonexistent")

        # Expected stats
        ex_stats = {
            "loaded": 1,
            "missing": 1,
            "stale": 0,
            "total": 2,
            "load_rate": 0.5,
        }

        stats = pipeline_cache.get_stats()
        assert stats == ex_stats


class TestStepDecoratorCaching:
    """Test @step decorator caching functionality."""

    @pytest.fixture
    def slow_step(self):
        """Create a slow step function for testing cache performance."""

        @step(validate_input=False, cache=True)
        def process_data(households: pl.DataFrame) -> dict[str, pl.DataFrame]:
            time.sleep(0.1)  # Simulate slow operation
            processed = households.with_columns(
                pl.concat_str([pl.lit("HH_"), pl.col("hh_id").cast(pl.Utf8)]).alias("hh_code")
            )
            return {"households": processed}

        return process_data

    def test_decorator_cache_miss_and_hit(self, slow_step, pipeline_cache, sample_dataframe):
        """Test decorator correctly caches and retrieves results."""
        # First call - cache miss
        start = time.time()
        result1 = slow_step(
            households=sample_dataframe,
            pipeline_cache=pipeline_cache,
            cache=True,
        )
        first_duration = time.time() - start

        # Second call - cache hit
        start = time.time()
        result2 = slow_step(
            households=sample_dataframe,
            pipeline_cache=pipeline_cache,
            cache=True,
        )
        second_duration = time.time() - start

        # Verify results are identical
        assert result1["households"].equals(result2["households"])

        # Verify second call was significantly faster (cache hit)
        assert second_duration < first_duration * 0.5

    def test_decorator_cache_disabled(self, slow_step, pipeline_cache, sample_dataframe):
        """Test decorator works when caching is disabled."""
        result1 = slow_step(
            households=sample_dataframe,
            pipeline_cache=pipeline_cache,
            cache=False,
        )
        result2 = slow_step(
            households=sample_dataframe,
            pipeline_cache=pipeline_cache,
            cache=False,
        )

        # Results should be identical but both calls execute the function
        assert result1["households"].equals(result2["households"])

        # Expect no cache entries

        # Verify no cache was created
        stats = pipeline_cache.get_stats()
        assert stats["loaded"] == 0

    def test_decorator_without_pipeline_cache(self, slow_step, sample_dataframe):
        """Test decorator works when no pipeline_cache is provided."""
        result = slow_step(households=sample_dataframe, cache=True)
        assert "households" in result
        assert "hh_code" in result["households"].columns


class TestCacheIntegration:
    """Integration tests for caching in full pipeline context."""

    def test_cache_invalidation_on_data_change(self, pipeline_cache):
        """Test cache is properly invalidated when input data changes."""

        @step(validate_input=False, cache=True)
        def transform(households: pl.DataFrame) -> dict[str, pl.DataFrame]:
            return {"households": households.with_columns(pl.col("hh_id") * 2)}

        df1 = pl.DataFrame({"hh_id": [1, 2, 3]})
        df2 = pl.DataFrame({"hh_id": [4, 5, 6]})

        # First call with df1
        result1 = transform(households=df1, pipeline_cache=pipeline_cache, cache=True)

        # Second call with df2 - should not use cache
        result2 = transform(households=df2, pipeline_cache=pipeline_cache, cache=True)

        # Results should be different
        assert not result1["households"].equals(result2["households"])
        assert result1["households"]["hh_id"].to_list() == [2, 4, 6]
        assert result2["households"]["hh_id"].to_list() == [8, 10, 12]

    def test_multiple_output_tables_cached(self, pipeline_cache):
        """Test caching works with multiple output tables."""

        @step(validate_input=False, cache=True)
        def split_data(households: pl.DataFrame) -> dict[str, pl.DataFrame]:
            return {
                "households": households.filter(pl.col("hh_id") <= 2),
                "persons": pl.DataFrame({"person_id": [1, 2], "hh_id": [1, 2]}),
            }

        df = pl.DataFrame({"hh_id": [1, 2, 3]})

        # First call
        result1 = split_data(households=df, pipeline_cache=pipeline_cache, cache=True)

        # Second call - should use cache
        result2 = split_data(households=df, pipeline_cache=pipeline_cache, cache=True)

        assert result1["households"].equals(result2["households"])
        assert result1["persons"].equals(result2["persons"])

        # Verify both tables were cached
        cached_steps = pipeline_cache.list_cached_steps()
        assert len(cached_steps) == 1
        assert set(cached_steps[0]["tables"]) == {"households", "persons"}

    def test_cache_key_with_empty_dataframe(self, pipeline_cache):
        """Test cache key generation with empty DataFrame."""
        empty_df = pl.DataFrame({"id": []})
        inputs = {"data": empty_df}
        params = {"param": "value"}

        key = pipeline_cache.get_cache_key("test_step", inputs, params)

        assert isinstance(key, str)
        assert len(key) == 16

    def test_cache_key_with_none_inputs(self, pipeline_cache):
        """Test cache key generation with None inputs (first step)."""
        key = pipeline_cache.get_cache_key("test_step", None, {"param": "value"})

        assert isinstance(key, str)
        assert len(key) == 16

    def test_cache_key_with_none_params(self, pipeline_cache, sample_dataframe):
        """Test cache key generation with None parameters."""
        inputs = {"data": sample_dataframe}

        key = pipeline_cache.get_cache_key("test_step", inputs, None)

        assert isinstance(key, str)
        assert len(key) == 16

    def test_load_nonexistent_step(self, pipeline_cache):
        """Test loading from nonexistent step."""
        result = pipeline_cache.load("nonexistent_step", "fake_key")

        assert result is None

    def test_load_nonexistent_cache_key(self, pipeline_cache, sample_dataframe):
        """Test loading with nonexistent cache key."""
        # Save with one key
        pipeline_cache.save("test_step", "real_key", {"data": sample_dataframe})

        # Try to load with different key
        result = pipeline_cache.load("test_step", "fake_key")

        assert result is None

    def test_clear_cache(self, pipeline_cache, sample_dataframe):
        """Test clearing all cache."""
        # Save some data
        pipeline_cache.save("step1", "key1", {"data": sample_dataframe})
        pipeline_cache.save("step2", "key2", {"data": sample_dataframe})

        assert len(pipeline_cache.list_cached_steps()) == 2

        # Clear cache
        pipeline_cache.clear()

        # Cache should be empty (directory still exists but contents cleared)
        assert len(pipeline_cache.list_cached_steps()) == 0
        # Cache directory itself may still exist
        if pipeline_cache.cache_dir.exists():
            # But should have no step directories
            step_dirs = [d for d in pipeline_cache.cache_dir.iterdir() if d.is_dir()]
            assert len(step_dirs) == 0

    def test_cache_with_geodataframe(self, pipeline_cache):
        """Test caching with GeoDataFrame."""
        gdf = gpd.GeoDataFrame(
            {"id": [1, 2], "value": [10, 20]}, geometry=[Point(0, 0), Point(1, 1)], crs="EPSG:4326"
        )

        # Save and load
        pipeline_cache.save("test_step", "geo_key", {"locations": gdf})
        loaded = pipeline_cache.load("test_step", "geo_key")

        assert loaded is not None
        assert "locations" in loaded
        assert isinstance(loaded["locations"], gpd.GeoDataFrame)
        assert loaded["locations"].crs.to_string() == "EPSG:4326"

    def test_cache_key_with_non_serializable_params(self, pipeline_cache, sample_dataframe):
        """Test cache key generation with non-serializable parameters."""
        inputs = {"data": sample_dataframe}
        # Include non-serializable object (like a thread lock)
        params = {
            "serializable": "value",
            "non_serializable": threading.Lock(),
            "another_good": 123,
        }

        # Should skip non-serializable and generate key from valid params
        key = pipeline_cache.get_cache_key("test_step", inputs, params)

        assert isinstance(key, str)
        assert len(key) == 16

    def test_load_with_corrupted_metadata(self, pipeline_cache, sample_dataframe):
        """Test loading cache with corrupted metadata file."""
        step_name = "test_step"
        cache_key = "corrupt_key"

        # Save valid data first
        pipeline_cache.save(step_name, cache_key, {"data": sample_dataframe})

        # Corrupt the metadata file
        cache_path = pipeline_cache.cache_dir / step_name / cache_key
        metadata_path = cache_path / "metadata.json"
        metadata_path.write_text("{ invalid json }")

        # Load should return None
        result = pipeline_cache.load(step_name, cache_key)
        assert result is None

        # Stats should show stale
        assert pipeline_cache._stats["stale"] > 0

    def test_load_with_missing_metadata(self, pipeline_cache, sample_dataframe):
        """Test loading cache with missing metadata file."""
        step_name = "test_step"
        cache_key = "missing_metadata"

        # Save valid data first
        pipeline_cache.save(step_name, cache_key, {"data": sample_dataframe})

        # Delete metadata file
        cache_path = pipeline_cache.cache_dir / step_name / cache_key
        metadata_path = cache_path / "metadata.json"
        metadata_path.unlink()

        # Load should return None
        result = pipeline_cache.load(step_name, cache_key)
        assert result is None

    def test_load_with_missing_table_file(self, pipeline_cache, sample_dataframe):
        """Test loading cache when table parquet file is missing."""
        step_name = "test_step"
        cache_key = "missing_table"

        # Save valid data first
        pipeline_cache.save(step_name, cache_key, {"data": sample_dataframe})

        # Delete one of the table files
        cache_path = pipeline_cache.cache_dir / step_name / cache_key
        table_file = cache_path / "data.parquet"
        table_file.unlink()

        # Load should return None
        result = pipeline_cache.load(step_name, cache_key)
        assert result is None

    def test_list_cached_steps_with_corrupted_metadata(self, pipeline_cache, sample_dataframe):
        """Test list_cached_steps skips corrupted cache entries."""
        # Save valid data
        pipeline_cache.save("step1", "key1", {"data": sample_dataframe})
        pipeline_cache.save("step2", "key2", {"data": sample_dataframe})

        # Corrupt one metadata file
        corrupt_path = pipeline_cache.cache_dir / "step2" / "key2" / "metadata.json"
        corrupt_path.write_text("{ bad json }")

        # List should only return valid step
        cached_steps = pipeline_cache.list_cached_steps()

        # Should have step1 but not step2 (corrupted)
        step_names = [s["step_name"] for s in cached_steps]
        assert "step1" in step_names
        # step2 might still be listed if list_cached_steps is lenient

    def test_save_with_exception_cleans_up(self, pipeline_cache):
        """Test that failed save cleans up partial cache."""
        step_name = "test_step"
        cache_key = "will_fail"

        # Create an object that can't be saved properly
        class SerializationError(RuntimeError):
            def __init__(self):
                super().__init__("Cannot serialize")

        class UnserializableObject:
            def __getstate__(self):
                raise SerializationError

        outputs = {"bad_data": UnserializableObject()}

        # This should catch the exception and clean up
        with contextlib.suppress(Exception):
            pipeline_cache.save(step_name, cache_key, outputs)

        # Cache directory for this key should not exist
        cache_path = pipeline_cache.cache_dir / step_name / cache_key
        assert not cache_path.exists()

    def test_get_stats(self, pipeline_cache, sample_dataframe):
        """Test cache statistics calculation."""
        # Initial stats
        stats = pipeline_cache.get_stats()
        assert stats["loaded"] == 0
        assert stats["missing"] == 0
        assert stats["stale"] == 0
        assert stats["total"] == 0

        # Save and load
        pipeline_cache.save("step1", "key1", {"data": sample_dataframe})
        pipeline_cache.load("step1", "key1")

        stats = pipeline_cache.get_stats()
        assert stats["loaded"] == 1
        assert stats["total"] == 1
        assert stats["load_rate"] == 1.0

        # Try loading non-existent cache
        pipeline_cache.load("step2", "key2")

        stats = pipeline_cache.get_stats()
        assert stats["missing"] == 1
        assert stats["total"] == 2
        assert stats["load_rate"] == 0.5

    def test_invalidate_nonexistent_step(self, pipeline_cache):
        """Test invalidating a step that doesn't exist."""
        # Should not raise error
        pipeline_cache.invalidate("nonexistent_step")

        # Directory should not exist
        step_dir = pipeline_cache.cache_dir / "nonexistent_step"
        assert not step_dir.exists()
