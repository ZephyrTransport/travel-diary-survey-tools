"""Tests for config shorthand resolution.

A config shorthand may refer to another shorthand, which is how projects hang
every output off one root:

    survey_dir: "M:/.../WeightedDataset"
    output_dir: "{{ survey_dir }}/pipeline_analysis"

Substituting in one pass makes that work only when the referenced shorthand is
declared *after* the one referring to it, so the same config resolves or emits a
literal ``{{ survey_dir }}`` directory depending on the order its keys happen to
be written in. These tests pin the order-independence.
"""

import pytest
import yaml

from pipeline.pipeline import MAX_TEMPLATE_PASSES, Pipeline, _resolve_variables


def _load(tmp_path, config: dict) -> dict:
    """Write a config and return it as Pipeline loads it."""
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return Pipeline(config_path=str(path), steps=[]).config


def test_shorthand_referring_to_an_earlier_shorthand_resolves(tmp_path):
    """The regression: the target is declared *before* the reference."""
    config = _load(
        tmp_path,
        {
            "survey_dir": "M:/survey",
            "output_dir": "{{ survey_dir }}/pipeline_analysis",
            "steps": [{"name": "load_data", "params": {"out": "{{ output_dir }}/x.csv"}}],
        },
    )

    assert config["output_dir"] == "M:/survey/pipeline_analysis"
    assert config["steps"][0]["params"]["out"] == "M:/survey/pipeline_analysis/x.csv"


def test_resolution_does_not_depend_on_declaration_order(tmp_path):
    """Declaring the target after the reference gives the identical result."""
    reversed_order = _load(
        tmp_path,
        {
            "output_dir": "{{ survey_dir }}/pipeline_analysis",
            "survey_dir": "M:/survey",
            "steps": [{"name": "load_data", "params": {"out": "{{ output_dir }}/x.csv"}}],
        },
    )

    assert reversed_order["output_dir"] == "M:/survey/pipeline_analysis"
    assert reversed_order["steps"][0]["params"]["out"] == "M:/survey/pipeline_analysis/x.csv"


def test_chained_shorthands_resolve():
    """A reference several links deep still bottoms out."""
    resolved = _resolve_variables(
        {
            "root": "M:/data",
            "survey_dir": "{{ root }}/survey",
            "output_dir": "{{ survey_dir }}/analysis",
            "log_file": "{{ output_dir }}/run.log",
        }
    )

    assert resolved["log_file"] == "M:/data/survey/analysis/run.log"


def test_no_template_leaks_into_a_resolved_path(tmp_path):
    """No resolved value keeps an unexpanded marker, which becomes a real path."""
    config = _load(
        tmp_path,
        {
            "survey_dir": "M:/survey",
            "output_dir": "{{ survey_dir }}/out",
            "log_file": "{{ output_dir }}/run.log",
            "steps": [],
        },
    )

    leaked = [key for key, value in config.items() if isinstance(value, str) and "{{" in value]
    assert not leaked, f"unexpanded template left in {leaked}"


def test_a_cycle_is_reported_rather_than_spun_on():
    """Shorthands referring to each other raise instead of looping forever."""
    with pytest.raises(ValueError, match="cycle"):
        _resolve_variables({"a": "{{ b }}/x", "b": "{{ a }}/y"})


def test_unknown_reference_is_left_alone():
    """A reference to something undeclared is not an error here.

    Nothing at this layer knows which strings are meant to be paths, so an
    unrecognised marker is passed through untouched rather than guessed at.
    """
    resolved = _resolve_variables({"a": "{{ nowhere }}/x"})

    assert resolved["a"] == "{{ nowhere }}/x"
    assert MAX_TEMPLATE_PASSES > 1
