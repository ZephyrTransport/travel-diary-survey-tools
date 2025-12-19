"""Tests for pipeline status reporting and get_data functionality."""

import json
import logging
import os
import time

import polars as pl
import pytest
import yaml

from pipeline.pipeline import Pipeline


@pytest.fixture
def temp_cache_dir(tmp_path):
    """Create a temporary cache directory with mock cached data."""
    cache_dir = tmp_path / ".cache"

    # Create mock cache for step1
    step1_cache = cache_dir / "step1" / "abc123"
    step1_cache.mkdir(parents=True)

    # Create mock DataFrames
    df1 = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    df2 = pl.DataFrame({"id": [1, 2], "value": [10, 20]})

    # Save as parquet
    df1.write_parquet(step1_cache / "households.parquet")
    df2.write_parquet(step1_cache / "persons.parquet")

    # Save metadata
    metadata = {
        "step_name": "step1",
        "cache_key": "abc123",
        "tables": ["households", "persons"],
        "table_types": {"households": "polars", "persons": "polars"},
        "row_counts": {"households": 3, "persons": 2},
    }
    with (step1_cache / "metadata.json").open("w") as f:
        json.dump(metadata, f)

    # Create mock cache for step2
    step2_cache = cache_dir / "step2" / "def456"
    step2_cache.mkdir(parents=True)

    df3 = pl.DataFrame({"id": [1, 2, 3, 4], "trip": ["A", "B", "C", "D"]})
    df3.write_parquet(step2_cache / "linked_trips.parquet")

    metadata2 = {
        "step_name": "step2",
        "cache_key": "def456",
        "tables": ["linked_trips"],
        "table_types": {"linked_trips": "polars"},
        "row_counts": {"linked_trips": 4},
    }
    with (step2_cache / "metadata.json").open("w") as f:
        json.dump(metadata2, f)

    return cache_dir


@pytest.fixture
def temp_config(tmp_path):
    """Create a temporary config file."""
    config = {
        "steps": [
            {"name": "step1", "cache": True},
            {"name": "step2", "cache": True},
            {"name": "step3", "cache": False},
        ]
    }

    config_path = tmp_path / "config.yaml"

    with config_path.open("w") as f:
        yaml.dump(config, f)

    return config_path


def test_pipeline_status_with_cache(temp_config, temp_cache_dir, caplog):
    """Test that pipeline correctly reports status of cached steps."""
    caplog.set_level(logging.INFO)

    pipeline = Pipeline(
        config_path=str(temp_config),
        steps=[],
        caching=temp_cache_dir,
    )

    # Check internal status tracking
    assert pipeline._step_status["step1"]["has_cache"] is True
    assert pipeline._step_status["step1"]["cache_key"] == "abc123"
    assert set(pipeline._step_status["step1"]["tables"]) == {
        "households",
        "persons",
    }

    assert pipeline._step_status["step2"]["has_cache"] is True
    assert pipeline._step_status["step2"]["cache_key"] == "def456"
    assert pipeline._step_status["step2"]["tables"] == ["linked_trips"]

    assert pipeline._step_status["step3"]["has_cache"] is False
    assert pipeline._step_status["step3"]["cache_enabled"] is False

    # Check that status was logged
    assert "Pipeline Status" in caplog.text
    assert "✓ CACHED" in caplog.text
    assert "∅ NO CACHE (disabled)" in caplog.text


def test_pipeline_status_no_cache(temp_config, tmp_path, caplog):
    """Test status when no cache exists."""
    caplog.set_level(logging.INFO)

    # Use empty cache directory
    cache_dir = tmp_path / "empty_cache"
    cache_dir.mkdir()

    pipeline = Pipeline(
        config_path=str(temp_config),
        steps=[],
        caching=cache_dir,
    )

    # All steps should show no cache
    assert pipeline._step_status["step1"]["has_cache"] is False
    assert pipeline._step_status["step2"]["has_cache"] is False

    assert "✗ NO CACHE" in caplog.text


def test_get_data_from_latest_step(temp_config, temp_cache_dir):
    """Test fetching data from latest step that has the table."""
    pipeline = Pipeline(
        config_path=str(temp_config),
        steps=[],
        caching=temp_cache_dir,
    )

    # Fetch households - should come from step1
    households = pipeline.get_data("households")
    assert isinstance(households, pl.DataFrame)
    assert len(households) == 3
    assert "id" in households.columns
    assert "name" in households.columns

    # Check it was added to pipeline.data
    assert pipeline.data.households is not None
    assert len(pipeline.data.households) == 3


def test_get_data_from_specific_step(temp_config, temp_cache_dir):
    """Test fetching data from a specific step."""
    pipeline = Pipeline(
        config_path=str(temp_config),
        steps=[],
        caching=temp_cache_dir,
    )

    # Fetch from specific step
    persons = pipeline.get_data("persons", step="step1")
    assert isinstance(persons, pl.DataFrame)
    assert len(persons) == 2
    assert "value" in persons.columns


def test_get_data_missing_table(temp_config, temp_cache_dir):
    """Test error handling when table doesn't exist."""
    pipeline = Pipeline(
        config_path=str(temp_config),
        steps=[],
        caching=temp_cache_dir,
    )

    with pytest.raises(
        ValueError, match="not found in any cached step"
    ) as exc_info:
        pipeline.get_data("nonexistent_table")

    assert "not found in any cached step" in str(exc_info.value)
    assert "Available tables:" in str(exc_info.value)


def test_get_data_missing_step(temp_config, temp_cache_dir):
    """Test error when specified step has no cache."""
    pipeline = Pipeline(
        config_path=str(temp_config),
        steps=[],
        caching=temp_cache_dir,
    )

    with pytest.raises(ValueError, match="has no cached data") as exc_info:
        pipeline.get_data("households", step="step3")

    assert "has no cached data" in str(exc_info.value)


def test_get_data_table_not_in_step(temp_config, temp_cache_dir):
    """Test error when table not in specified step."""
    pipeline = Pipeline(
        config_path=str(temp_config),
        steps=[],
        caching=temp_cache_dir,
    )

    with pytest.raises(ValueError, match="not found in step") as exc_info:
        pipeline.get_data("linked_trips", step="step1")

    assert "not found in step" in str(exc_info.value)
    assert "Available tables:" in str(exc_info.value)


def test_get_data_no_caching_enabled(temp_config):
    """Test error when caching is disabled."""
    pipeline = Pipeline(
        config_path=str(temp_config),
        steps=[],
        caching=False,
    )

    with pytest.raises(ValueError, match="Caching is disabled") as exc_info:
        pipeline.get_data("households")

    assert "Caching is disabled" in str(exc_info.value)


def test_multiple_cache_keys_uses_newest(temp_config, temp_cache_dir):
    """Test that when multiple cache keys exist, newest is used."""
    # Create an older cache for step1

    old_cache = temp_cache_dir / "step1" / "old999"
    old_cache.mkdir(parents=True)

    df_old = pl.DataFrame({"id": [99], "name": ["old"]})
    df_old.write_parquet(old_cache / "households.parquet")

    metadata_old = {
        "step_name": "step1",
        "cache_key": "old999",
        "tables": ["households"],
        "table_types": {"households": "polars"},
        "row_counts": {"households": 1},
    }
    with (old_cache / "metadata.json").open("w") as f:
        json.dump(metadata_old, f)

    # Make the old cache older by modification time
    old_time = time.time() - 3600  # 1 hour ago

    os.utime(old_cache, (old_time, old_time))

    # Create pipeline - should use newer cache
    pipeline = Pipeline(
        config_path=str(temp_config),
        steps=[],
        caching=temp_cache_dir,
    )

    # Should use abc123 (newer), not old999
    assert pipeline._step_status["step1"]["cache_key"] == "abc123"

    # Verify by loading data
    households = pipeline.get_data("households")
    assert len(households) == 3  # From abc123, not 1 from old999


def test_scan_cache_after_run(temp_config, temp_cache_dir):
    """Test that cache status is refreshed after pipeline run."""
    # Note: This test would need actual step functions to fully test
    # For now, just verify the method exists and can be called
    pipeline = Pipeline(
        config_path=str(temp_config),
        steps=[],
        caching=temp_cache_dir,
    )

    # Should be able to re-scan without error
    pipeline._scan_cache()

    # Status should remain consistent
    assert pipeline._step_status["step1"]["has_cache"] is True
