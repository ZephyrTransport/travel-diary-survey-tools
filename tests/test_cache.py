"""Tests for pipeline caching functionality."""

import shutil
import time

import polars as pl
import pytest

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

    def test_cache_key_changes_with_data(
        self, pipeline_cache, sample_dataframe
    ):
        """Test cache key changes when data changes."""
        inputs1 = {"households": sample_dataframe}
        inputs2 = {
            "households": sample_dataframe.with_columns(pl.col("hh_id") + 10)
        }
        params = {"param1": "value1"}

        key1 = pipeline_cache.get_cache_key("test_step", inputs1, params)
        key2 = pipeline_cache.get_cache_key("test_step", inputs2, params)

        assert key1 != key2

    def test_cache_key_changes_with_params(
        self, pipeline_cache, sample_dataframe
    ):
        """Test cache key changes when parameters change."""
        inputs = {"households": sample_dataframe}

        key1 = pipeline_cache.get_cache_key(
            "test_step", inputs, {"param1": "value1"}
        )
        key2 = pipeline_cache.get_cache_key(
            "test_step", inputs, {"param1": "value2"}
        )

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
                pl.concat_str(
                    [pl.lit("HH_"), pl.col("hh_id").cast(pl.Utf8)]
                ).alias("hh_code")
            )
            return {"households": processed}

        return process_data

    def test_decorator_cache_miss_and_hit(
        self, slow_step, pipeline_cache, sample_dataframe
    ):
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

    def test_decorator_cache_disabled(
        self, slow_step, pipeline_cache, sample_dataframe
    ):
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

    def test_decorator_without_pipeline_cache(
        self, slow_step, sample_dataframe
    ):
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
        result1 = transform(
            households=df1, pipeline_cache=pipeline_cache, cache=True
        )

        # Second call with df2 - should not use cache
        result2 = transform(
            households=df2, pipeline_cache=pipeline_cache, cache=True
        )

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
        result1 = split_data(
            households=df, pipeline_cache=pipeline_cache, cache=True
        )

        # Second call - should use cache
        result2 = split_data(
            households=df, pipeline_cache=pipeline_cache, cache=True
        )

        assert result1["households"].equals(result2["households"])
        assert result1["persons"].equals(result2["persons"])

        # Verify both tables were cached
        cached_steps = pipeline_cache.list_cached_steps()
        assert len(cached_steps) == 1
        assert set(cached_steps[0]["tables"]) == {"households", "persons"}
