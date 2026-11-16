"""Each project's ``config.yaml`` agrees with its ``run.py`` and with the steps it names.

The runners are parsed with :mod:`ast`, not imported: importing ``run.py`` maps
network drives and reads ``.env``.
"""

import ast
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

import processing
from data_canon.core.dataclass import CanonicalData
from pipeline.config import resolve_templates
from pipeline.pipeline import Pipeline

PROJECTS_DIR = Path(__file__).parent.parent / "projects"

# Name of the list each runner passes to ``Pipeline(steps=...)``.
STEP_LIST_NAME = "processing_steps"

# Every consumer of a usability profile, and the key it names them under.
_PROFILE_CONSUMERS = (
    ("compute_weights", "weight_profiles"),
    ("add_existing_weights", "weight_profiles"),
    ("format_ctramp", "usability_profile"),
    ("format_daysim", "usability_profile"),
)

# The cascade stamps this on every run, whatever profiles a project declares.
_ALWAYS_STAMPED = frozenset({"survey_complete"})


def _project_dirs() -> list[Path]:
    """Every project directory holding both a config and a runner."""
    if not PROJECTS_DIR.is_dir():
        return []
    return sorted(
        d
        for d in PROJECTS_DIR.iterdir()
        if (d / "config.yaml").is_file() and (d / "run.py").is_file()
    )


def _steps(config_path: Path) -> list[dict]:
    with config_path.open() as f:
        return (yaml.safe_load(f) or {}).get("steps", [])


def _config_step_names(config_path: Path) -> list[str]:
    return [step["name"] for step in _steps(config_path)]


def _runner_step_names(run_path: Path) -> set[str]:
    """Names in the runner's ``processing_steps`` list, which are the ``__name__`` keys."""
    tree = ast.parse(run_path.read_text(encoding="utf-8"), filename=str(run_path))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.List):
            continue
        if any(
            isinstance(target, ast.Name) and target.id == STEP_LIST_NAME for target in node.targets
        ):
            return {elt.id for elt in node.value.elts if isinstance(elt, ast.Name)}

    pytest.fail(f"No `{STEP_LIST_NAME} = [...]` assignment found in {run_path}")


@pytest.mark.parametrize("project_dir", _project_dirs(), ids=lambda p: p.name)
def test_config_steps_are_registered_in_runner(project_dir: Path):
    """Every step a config names is passed to the Pipeline (the runner may pass more)."""
    config_steps = _config_step_names(project_dir / "config.yaml")
    registered = _runner_step_names(project_dir / "run.py")

    unregistered = [name for name in config_steps if name not in registered]

    assert not unregistered, (
        f"{project_dir.name}/config.yaml names step(s) "
        f"{', '.join(repr(n) for n in unregistered)} that {project_dir.name}/run.py "
        f"never passes to Pipeline(steps=[...]). The run aborts on the first one. "
        f"Import the function and add it to `{STEP_LIST_NAME}`."
    )


@pytest.mark.parametrize("project_dir", _project_dirs(), ids=lambda p: p.name)
def test_config_step_names_are_unique(project_dir: Path):
    """A config does not name the same step twice."""
    config_steps = _config_step_names(project_dir / "config.yaml")
    duplicates = {name for name in config_steps if config_steps.count(name) > 1}

    assert not duplicates, (
        f"{project_dir.name}/config.yaml declares duplicate step(s): "
        f"{', '.join(sorted(duplicates))}."
    )


def _step_params(config_path: Path, step_name: str) -> dict:
    """Params of the named step, or an empty dict if the config omits it."""
    for step in _steps(config_path):
        if step.get("name") == step_name:
            return step.get("params") or {}
    return {}


@pytest.mark.parametrize("project_dir", _project_dirs(), ids=lambda p: p.name)
def test_named_profiles_are_stamped_by_the_cascade(project_dir: Path):
    """Every profile a step names is one ``cascade_usability`` stamps; a typo would not fail."""
    config_path = project_dir / "config.yaml"
    declared = set(_step_params(config_path, "cascade_usability").get("usability_profiles") or {})
    if not declared:
        pytest.skip("project declares no usability profiles")
    legal = declared | _ALWAYS_STAMPED

    for step_name, key in _PROFILE_CONSUMERS:
        params = _step_params(config_path, step_name)
        value = params.get(key) or []
        named = value if isinstance(value, list) else [value]
        for profile in named:
            assert profile in legal, (
                f"{project_dir.name}: step {step_name!r} names profile {profile!r}, which "
                f"cascade_usability does not stamp. Declared: {sorted(legal)}"
            )


@pytest.mark.parametrize("project_dir", _project_dirs(), ids=lambda p: p.name)
def test_config_keys_are_taken_by_their_steps(project_dir: Path):
    """Every key a config sets is taken by its ``processing`` step, and none it needs is missing."""
    with (project_dir / "config.yaml").open() as f:
        config = resolve_templates(yaml.safe_load(f))
    fake = SimpleNamespace(config=config, data=CanonicalData())
    for step_cfg in config["steps"]:
        step_func = getattr(processing, step_cfg["name"], None)
        if step_func is None:
            continue
        Pipeline.parse_step_args(fake, step_cfg["name"], step_func)


def test_projects_are_discovered():
    """At least one project is found, so the tests above do not pass on zero cases."""
    assert _project_dirs(), f"No project (config.yaml + run.py) found under {PROJECTS_DIR}"
