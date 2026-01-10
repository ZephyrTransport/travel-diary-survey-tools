"""Tests for pipeline status reporting and get_data functionality."""

import json
import logging
import os
import time
from pathlib import Path

import polars as pl
import pytest
import yaml

from pipeline.decoration import step
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

    with pytest.raises(ValueError, match="not found in any cached step") as exc_info:
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

    with pytest.raises(
        ValueError, match=r"Table 'households' not found in canonical data."
    ) as exc_info:
        pipeline.get_data("households")

    assert "Table 'households' not found in canonical data." in str(exc_info.value)


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


def test_pipeline_with_caching_path_string(temp_config, tmp_path):
    """Test Pipeline initialization with caching as string path."""
    cache_dir = tmp_path / "custom_cache"

    pipeline = Pipeline(
        config_path=str(temp_config),
        steps=[],
        caching=str(cache_dir),  # Pass as string
    )

    assert pipeline.cache is not None
    assert pipeline.cache.cache_dir == cache_dir


def test_pipeline_with_caching_path_object(temp_config, tmp_path):
    """Test Pipeline initialization with caching as Path object."""
    cache_dir = tmp_path / "custom_cache"

    pipeline = Pipeline(
        config_path=str(temp_config),
        steps=[],
        caching=cache_dir,  # Pass as Path
    )

    assert pipeline.cache is not None
    assert pipeline.cache.cache_dir == cache_dir


def test_pipeline_with_caching_true(temp_config):
    """Test Pipeline initialization with caching=True uses default."""
    pipeline = Pipeline(
        config_path=str(temp_config),
        steps=[],
        caching=True,
    )

    assert pipeline.cache is not None
    assert pipeline.cache.cache_dir == Path(".cache")


def test_report_status_with_no_steps(tmp_path):
    """Test status report with empty config."""
    config = {"steps": []}
    config_path = tmp_path / "empty_config.yaml"

    with config_path.open("w") as f:
        yaml.dump(config, f)

    pipeline = Pipeline(
        config_path=str(config_path),
        steps=[],
        caching=False,
    )

    # Should not crash with empty steps
    pipeline.report_status()


def test_get_available_tables(temp_config, temp_cache_dir):
    """Test _get_available_tables method."""
    pipeline = Pipeline(
        config_path=str(temp_config),
        steps=[],
        caching=temp_cache_dir,
    )

    tables = pipeline._get_available_tables()

    # Should have all cached tables
    assert "households" in tables
    assert "persons" in tables
    assert "linked_trips" in tables

    # Should map to correct steps
    assert "step1" in tables["households"]
    assert "step1" in tables["persons"]
    assert "step2" in tables["linked_trips"]


def test_find_step_with_table(temp_config, temp_cache_dir):
    """Test _find_step_with_table finds latest step."""
    pipeline = Pipeline(
        config_path=str(temp_config),
        steps=[],
        caching=temp_cache_dir,
    )

    # Find households - should be in step1
    step = pipeline._find_step_with_table("households")
    assert step == "step1"

    # Find linked_trips - should be in step2
    step = pipeline._find_step_with_table("linked_trips")
    assert step == "step2"

    # Find nonexistent table
    step = pipeline._find_step_with_table("nonexistent")
    assert step is None


def test_pipeline_caching_disabled_operations(temp_config):
    """Test that pipeline operations work with caching disabled."""
    pipeline = Pipeline(
        config_path=str(temp_config),
        steps=[],
        caching=False,
    )

    # _get_available_tables should return empty dict
    tables = pipeline._get_available_tables()
    assert tables == {}

    # _find_step_with_table should return None
    step = pipeline._find_step_with_table("households")
    assert step is None


def test_scan_cache_with_corrupted_metadata(temp_config, temp_cache_dir):
    """Test _scan_cache handles corrupted metadata gracefully."""
    # Create cache with invalid metadata
    step_cache = temp_cache_dir / "step1" / "corrupt123"
    step_cache.mkdir(parents=True)

    # Write corrupted JSON
    metadata_path = step_cache / "metadata.json"
    metadata_path.write_text("{ invalid json }")

    # Should not crash
    pipeline = Pipeline(
        config_path=str(temp_config),
        steps=[],
        caching=temp_cache_dir,
    )

    # Should have scanned successfully (skipping corrupted cache)
    assert "step1" in pipeline._step_status


def test_step_status_cache_enabled_field(temp_config, temp_cache_dir):
    """Test that cache_enabled field is set correctly in step status."""
    pipeline = Pipeline(
        config_path=str(temp_config),
        steps=[],
        caching=temp_cache_dir,
    )

    # step1 and step2 have cache enabled
    assert pipeline._step_status["step1"]["cache_enabled"] is True
    assert pipeline._step_status["step2"]["cache_enabled"] is True

    # step3 has cache disabled
    assert pipeline._step_status["step3"]["cache_enabled"] is False


def test_pipeline_config_variable_substitution(tmp_path):
    """Test that config template variables are substituted."""
    config = {
        "base_dir": "/data/surveys",
        "survey_name": "bats_2023",
        "input_file": "{{ base_dir }}/{{ survey_name }}/input.csv",
        "steps": [],
    }
    config_path = tmp_path / "config.yaml"

    with config_path.open("w") as f:
        yaml.dump(config, f)

    pipeline = Pipeline(
        config_path=str(config_path),
        steps=[],
        caching=False,
    )

    # Variables should be substituted
    assert pipeline.config["input_file"] == "/data/surveys/bats_2023/input.csv"


def test_parse_step_args_with_canonical_data(temp_config):
    """Test parse_step_args passes canonical_data parameter."""

    def step_func(canonical_data):
        pass

    pipeline = Pipeline(
        config_path=str(temp_config),
        steps=[step_func],
        caching=False,
    )

    kwargs = pipeline.parse_step_args("step1", step_func)

    assert "canonical_data" in kwargs
    assert kwargs["canonical_data"] is pipeline.data


def test_parse_step_args_with_table_names(temp_config):
    """Test parse_step_args extracts table data from canonical data."""

    def step_func(households, persons):
        pass

    pipeline = Pipeline(
        config_path=str(temp_config),
        steps=[step_func],
        caching=False,
    )

    pipeline.data.households = pl.DataFrame({"id": [1, 2]})
    pipeline.data.persons = pl.DataFrame({"id": [1, 2]})

    kwargs = pipeline.parse_step_args("step_func", step_func)

    assert "households" in kwargs
    assert "persons" in kwargs
    assert kwargs["households"] is pipeline.data.households
    assert kwargs["persons"] is pipeline.data.persons


def test_parse_step_args_missing_required_parameter(tmp_path):
    """Test parse_step_args raises error for missing required params."""
    config = {
        "steps": [
            {"name": "test_step", "params": {}}  # Missing required param
        ]
    }
    config_path = tmp_path / "config.yaml"
    with config_path.open("w") as f:
        yaml.dump(config, f)

    def step_func(required_param):  # No default value
        pass

    pipeline = Pipeline(
        config_path=str(config_path),
        steps=[step_func],
        caching=False,
    )

    with pytest.raises(ValueError, match="Missing required parameter 'required_param'"):
        pipeline.parse_step_args("test_step", step_func)


def test_parse_step_args_with_config_params(tmp_path):
    """Test parse_step_args extracts parameters from config."""
    config = {"steps": [{"name": "test_step", "params": {"threshold": 0.5, "mode": "strict"}}]}
    config_path = tmp_path / "config.yaml"
    with config_path.open("w") as f:
        yaml.dump(config, f)

    def step_func(threshold, mode):
        pass

    pipeline = Pipeline(
        config_path=str(config_path),
        steps=[step_func],
        caching=False,
    )

    kwargs = pipeline.parse_step_args("test_step", step_func)

    assert kwargs["threshold"] == 0.5
    assert kwargs["mode"] == "strict"


def test_parse_step_args_with_defaults(tmp_path):
    """Test parse_step_args handles parameters with default values."""
    config = {
        "steps": [
            {"name": "test_step", "params": {}}  # No params provided
        ]
    }
    config_path = tmp_path / "config.yaml"
    with config_path.open("w") as f:
        yaml.dump(config, f)

    def step_func(optional_param="default_value"):
        pass

    pipeline = Pipeline(
        config_path=str(config_path),
        steps=[step_func],
        caching=False,
    )

    kwargs = pipeline.parse_step_args("test_step", step_func)

    # Should not include param since it has default and not in config
    assert "optional_param" not in kwargs


def test_load_from_step_with_invalid_step(temp_config, temp_cache_dir):
    """Test _load_from_step with step that has no cache."""
    pipeline = Pipeline(
        config_path=str(temp_config),
        steps=[],
        caching=temp_cache_dir,
    )

    with pytest.raises(ValueError, match="has no cached data"):
        pipeline._load_from_step("households", "step3")  # step3 has no cache


def test_load_from_step_missing_table_in_step(temp_config, temp_cache_dir):
    """Test _load_from_step when table not in specified step."""
    pipeline = Pipeline(
        config_path=str(temp_config),
        steps=[],
        caching=temp_cache_dir,
    )

    with pytest.raises(ValueError, match="not found in step"):
        pipeline._load_from_step("nonexistent_table", "step1")


def test_pipeline_run_with_simple_step(tmp_path):
    """Test pipeline.run() executes steps correctly."""
    config = {"steps": [{"name": "simple_step", "cache": False}]}
    config_path = tmp_path / "config.yaml"
    with config_path.open("w") as f:
        yaml.dump(config, f)

    # Create a simple step function
    step_executed = []

    def simple_step(canonical_data, **kwargs):  # noqa: ARG001
        step_executed.append(True)
        canonical_data.households = pl.DataFrame({"id": [1, 2, 3]})

    pipeline = Pipeline(
        config_path=str(config_path),
        steps=[simple_step],
        caching=False,
    )

    result = pipeline.run()

    assert len(step_executed) == 1
    assert result.households is not None
    assert len(result.households) == 3


def test_pipeline_run_missing_step(tmp_path):
    """Test pipeline.run() raises error for missing step."""
    config = {"steps": [{"name": "nonexistent_step"}]}
    config_path = tmp_path / "config.yaml"
    with config_path.open("w") as f:
        yaml.dump(config, f)

    pipeline = Pipeline(
        config_path=str(config_path),
        steps=[],  # No steps provided
        caching=False,
    )

    with pytest.raises(ValueError, match="not found in pipeline steps"):
        pipeline.run()


def test_pipeline_run_with_log_file(tmp_path):
    """Test pipeline with log file configuration."""
    config = {
        "log_file": "pipeline.log",  # Relative path
        "steps": [{"name": "test_step", "cache": False}],
    }
    config_path = tmp_path / "config.yaml"
    with config_path.open("w") as f:
        yaml.dump(config, f)

    def test_step(canonical_data, **kwargs):  # noqa: ARG001
        canonical_data.households = pl.DataFrame({"id": [1]})

    pipeline = Pipeline(
        config_path=str(config_path),
        steps=[test_step],
        caching=False,
    )

    pipeline.run()

    # Log file should be in .cache directory
    log_file = Path(".cache") / "pipeline.log"
    assert log_file.exists()


def test_pipeline_run_with_absolute_log_file(tmp_path):
    """Test pipeline with absolute log file path."""
    log_file = tmp_path / "test.log"
    config = {
        "log_file": str(log_file),  # Absolute path
        "steps": [{"name": "test_step", "cache": False}],
    }
    config_path = tmp_path / "config.yaml"
    with config_path.open("w") as f:
        yaml.dump(config, f)

    def test_step(canonical_data, **kwargs):  # noqa: ARG001
        canonical_data.households = pl.DataFrame({"id": [1]})

    pipeline = Pipeline(
        config_path=str(config_path),
        steps=[test_step],
        caching=False,
    )

    pipeline.run()

    assert log_file.exists()


def test_pipeline_run_with_caching_statistics(tmp_path):
    """Test pipeline run logs cache statistics."""
    cache_dir = tmp_path / "cache"
    config = {"steps": [{"name": "cached_step", "cache": True}]}
    config_path = tmp_path / "config.yaml"
    with config_path.open("w") as f:
        yaml.dump(config, f)

    @step()
    def cached_step(canonical_data):
        canonical_data.households = pl.DataFrame({"id": [1, 2]})

    pipeline = Pipeline(
        config_path=str(config_path),
        steps=[cached_step],
        caching=cache_dir,
    )

    # First run - should cache
    result1 = pipeline.run()
    assert result1.households is not None

    # Second run - should use cache
    pipeline2 = Pipeline(
        config_path=str(config_path),
        steps=[cached_step],
        caching=cache_dir,
    )
    result2 = pipeline2.run()
    assert result2.households is not None
