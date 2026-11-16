"""Tests for the Pipeline runner: step args and run()."""

from pathlib import Path

import polars as pl
import pytest
import yaml
from pydantic import BaseModel, ConfigDict

from data_canon.core.exceptions import DataValidationError
from pipeline.decoration import step
from pipeline.pipeline import Pipeline


def _write_config(tmp_path, config: dict) -> str:
    config_path = tmp_path / "config.yaml"
    with config_path.open("w") as f:
        yaml.dump(config, f)
    return str(config_path)


def _pipeline(tmp_path, step_func, **step_cfg) -> Pipeline:
    """A pipeline running ``step_func`` alone, configured under its own name."""
    config = {"steps": [{"name": step_func.__name__, **step_cfg}]}
    return Pipeline(config_path=_write_config(tmp_path, config), steps=[step_func])


def _parse(tmp_path, params, step_func) -> dict:
    """The keyword arguments the pipeline assembles for ``step_func`` under ``params``."""
    return _pipeline(tmp_path, step_func, params=params).parse_step_args(
        step_func.__name__, step_func
    )


class TestParseStepArgs:
    """Assembling a step's keyword arguments from the config and canonical data."""

    def test_parse_step_args_with_canonical_data(self, tmp_path):
        """A ``canonical_data`` argument gets the pipeline's data object."""

        def step_func(canonical_data):
            pass

        pipeline = _pipeline(tmp_path, step_func)

        assert pipeline.parse_step_args("step_func", step_func)["canonical_data"] is pipeline.data

    def test_parse_step_args_with_table_names(self, tmp_path):
        """The tables a step's contract declares come from the canonical data."""

        @step(requires={"households": set(), "persons": set()})
        def step_func(tables):
            pass

        pipeline = _pipeline(tmp_path, step_func)
        pipeline.data.households = pl.DataFrame({"id": [1, 2]})
        pipeline.data.persons = pl.DataFrame({"id": [1, 2]})
        kwargs = pipeline.parse_step_args("step_func", step_func)

        assert kwargs["households"] is pipeline.data.households
        assert kwargs["persons"] is pipeline.data.persons

    def test_parse_step_args_missing_required_parameter(self, tmp_path):
        """A parameter with no default that the config does not set raises."""

        def step_func(required_param):
            pass

        with pytest.raises(ValueError, match="Missing required parameter 'required_param'"):
            _parse(tmp_path, {}, step_func)

    def test_parse_step_args_with_config_params(self, tmp_path):
        """Parameters come from the step's ``params`` in the config."""

        def step_func(threshold, mode):
            pass

        kwargs = _parse(tmp_path, {"threshold": 0.5, "mode": "strict"}, step_func)

        assert kwargs == {"threshold": 0.5, "mode": "strict"}

    def test_parse_step_args_with_defaults(self, tmp_path):
        """A parameter the config is silent about is left to the function's default."""

        def step_func(optional_param="default_value"):
            pass

        assert "optional_param" not in _parse(tmp_path, {}, step_func)

    def test_parse_step_args_rejects_unknown_key(self, tmp_path):
        """A config key the step does not take raises instead of being dropped."""

        def step_func(method="buffer"):
            pass

        with pytest.raises(ValueError, match=r"sets \['config'\], which the step does not take"):
            _parse(tmp_path, {"config": {"method": "x"}}, step_func)

    def test_parse_step_args_rejects_table_name_key(self, tmp_path):
        """A config key naming a table raises: the table would win and the value be lost."""

        def step_func(households):
            pass

        with pytest.raises(ValueError, match="which name canonical tables"):
            _parse(tmp_path, {"households": {"buffer": 1}}, step_func)

    def test_parse_step_args_accepts_empty_params(self, tmp_path):
        """An empty ``params:`` block reads as no settings."""

        def step_func(optional_param="default_value"):
            pass

        assert _parse(tmp_path, None, step_func) == {}


class _MatchSettings(BaseModel):
    """A settings model as a step would take it: one required field, one defaulted."""

    model_config = ConfigDict(extra="forbid")

    buffer_meters: float
    min_dwell_minutes: float = 90.0


class _LooseSettings(BaseModel):
    """A settings model that would let a misspelt key through."""

    buffer_meters: float


class TestSettingsModels:
    """A step argument annotated with a model is built from its keys, flat or as a block."""

    def test_flat_keys_are_routed_to_their_model(self, tmp_path):
        """A flat key goes to the model owning its name; the defaulted field fills in."""

        def step_func(matching: _MatchSettings, include_homes: bool):
            pass

        kwargs = _parse(tmp_path, {"buffer_meters": 300.0, "include_homes": True}, step_func)

        assert kwargs["matching"] == _MatchSettings(buffer_meters=300.0, min_dwell_minutes=90.0)
        assert kwargs["include_homes"] is True

    def test_a_block_is_built_into_the_model(self, tmp_path):
        """The same settings may sit in a block named after the argument."""

        def step_func(matching: _MatchSettings):
            pass

        kwargs = _parse(tmp_path, {"matching": {"buffer_meters": 300.0}}, step_func)

        assert kwargs["matching"] == _MatchSettings(buffer_meters=300.0)

    def test_flat_and_block_together_raise(self, tmp_path):
        """One model's settings are written one way, so there is one place to read."""

        def step_func(matching: _MatchSettings):
            pass

        with pytest.raises(ValueError, match="write those settings one way"):
            _parse(
                tmp_path,
                {"matching": {"buffer_meters": 300.0}, "min_dwell_minutes": 45.0},
                step_func,
            )

    @pytest.mark.parametrize(
        "params",
        [
            pytest.param({"min_dwell_minutes": 45.0}, id="flat"),
            pytest.param({"matching": {"min_dwell_minutes": 45.0}}, id="block"),
            pytest.param({}, id="nothing"),
        ],
    )
    def test_a_field_without_a_default_is_required(self, tmp_path, params):
        """The model's own rule decides, whichever way the settings are written."""

        def step_func(matching: _MatchSettings):
            pass

        with pytest.raises(ValueError, match="_MatchSettings settings") as excinfo:
            _parse(tmp_path, params, step_func)
        assert "buffer_meters" in str(excinfo.value)

    def test_a_misspelt_flat_key_raises(self, tmp_path):
        """A flat key no argument or field owns is refused, not dropped."""

        def step_func(matching: _MatchSettings):
            pass

        with pytest.raises(ValueError, match=r"sets \['bufer_meters'\]"):
            _parse(tmp_path, {"bufer_meters": 300.0}, step_func)

    def test_a_misspelt_key_in_a_block_raises(self, tmp_path):
        """Inside a block, the model's extra="forbid" refuses it."""

        def step_func(matching: _MatchSettings):
            pass

        with pytest.raises(ValueError, match="bufer_meters"):
            _parse(tmp_path, {"matching": {"bufer_meters": 300.0}}, step_func)

    def test_a_block_that_is_not_a_mapping_raises(self, tmp_path):
        """A scalar where a block belongs is a config error, not a type error later."""

        def step_func(matching: _MatchSettings):
            pass

        with pytest.raises(ValueError, match="must be a block of _MatchSettings"):
            _parse(tmp_path, {"matching": 300.0}, step_func)

    def test_a_model_allowing_extra_fields_is_refused(self, tmp_path):
        """Without extra="forbid" a misspelt key in a block would pass unread."""

        def step_func(matching: _LooseSettings):
            pass

        with pytest.raises(TypeError, match='extra="forbid"'):
            _parse(tmp_path, {"buffer_meters": 300.0}, step_func)

    def test_a_field_named_like_an_argument_fails_at_definition(self):
        """A flat key must have one owner, so the clash is refused when the step is defined."""
        with pytest.raises(TypeError, match="two owners"):

            @step()
            def test_step(matching: _MatchSettings, buffer_meters: float):
                pass


class TestRun:
    """Executing the configured steps."""

    def test_pipeline_run_with_simple_step(self, tmp_path):
        """``run()`` executes each configured step once and returns the data."""
        step_executed = []

        def simple_step(canonical_data, **kwargs):  # noqa: ARG001
            step_executed.append(True)
            canonical_data.households = pl.DataFrame({"id": [1, 2, 3]})

        result = _pipeline(tmp_path, simple_step).run()

        assert len(step_executed) == 1
        assert result.households["id"].to_list() == [1, 2, 3]

    @pytest.mark.parametrize(
        ("step_cfg", "raises"),
        [
            pytest.param({}, True, id="config_silent_decorator_stands"),
            pytest.param({"validate_output": False}, False, id="config_overrides"),
        ],
    )
    def test_decorator_flags_stand_unless_config_sets_them(self, tmp_path, step_cfg, raises):
        """``@step(validate_output=True)`` validates unless the config turns it off."""

        @step(validate_input=False, validate_output=True)
        def checked() -> dict[str, pl.DataFrame]:
            return {"households": pl.DataFrame({"hh_id": [0]})}  # invalid: hh_id >= 1

        pipeline = _pipeline(tmp_path, checked, **step_cfg)
        if raises:
            with pytest.raises(DataValidationError):
                pipeline.run()
        else:
            pipeline.run()

    def test_a_step_taking_a_flag_gets_the_value_in_force(self, tmp_path):
        """A step with a ``validate_input`` argument gets it when the config says nothing."""
        seen = []

        @step()
        def takes_flag(validate_input: bool) -> None:
            seen.append(validate_input)

        _pipeline(tmp_path, takes_flag).run()
        assert seen == [True]

    def test_pipeline_run_missing_step_reports_all_and_runs_nothing(self, tmp_path):
        """Unregistered steps are all listed, with the registered ones, before any step runs."""
        config_path = _write_config(
            tmp_path,
            {"steps": [{"name": "good_step"}, {"name": "missing_one"}, {"name": "missing_two"}]},
        )

        executed = []

        def good_step(canonical_data, **kwargs):  # noqa: ARG001
            executed.append("good_step")

        pipeline = Pipeline(config_path=config_path, steps=[good_step])

        with pytest.raises(ValueError, match="not found in pipeline steps") as exc_info:
            pipeline.run()

        message = str(exc_info.value)
        assert "missing_one" in message
        assert "missing_two" in message
        assert "good_step" in message
        assert executed == []

    @pytest.mark.parametrize("absolute", [False, True], ids=["relative", "absolute"])
    def test_pipeline_run_with_log_file(self, tmp_path, monkeypatch, absolute: bool):
        """A log path is taken as given: an absolute one as is, a relative one from the cwd."""
        monkeypatch.chdir(tmp_path)
        log_file = tmp_path / "test.log" if absolute else Path("pipeline.log")
        configured = str(log_file) if absolute else "pipeline.log"

        config_path = _write_config(
            tmp_path,
            {"log_file": configured, "steps": [{"name": "test_step"}]},
        )

        def test_step(canonical_data, **kwargs):  # noqa: ARG001
            canonical_data.households = pl.DataFrame({"id": [1]})

        Pipeline(config_path=config_path, steps=[test_step]).run()

        assert log_file.exists()

    def test_a_step_with_an_unknown_key_stops_before_running(self, tmp_path):
        """A key the pipeline does not read, such as the removed ``cache``, is not ignored."""
        ran: list[bool] = []

        def noop(canonical_data, **kwargs):  # noqa: ARG001
            ran.append(True)

        with pytest.raises(ValueError, match=r"unknown keys \['cache'\]"):
            _pipeline(tmp_path, noop, cache=True).run()
        assert ran == []
