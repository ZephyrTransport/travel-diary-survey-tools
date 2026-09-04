"""Contract tests binding each project's pipeline config to its runner.

``Pipeline`` resolves the step names in ``config.yaml`` against the functions
handed to ``Pipeline(steps=[...])``. Nothing links the two: a step can be added
to a config, and its implementation merged, while the runner is never updated to
pass the function. The pipeline then dies part-way through a long run on a step
that was never registered. These tests close that loop statically, so the
mismatch fails in CI instead of on a live dataset.

The runner modules are parsed with :mod:`ast` rather than imported. Importing one
has side effects that must not happen during a test run -- ``run.py`` maps
network drives and reads ``.env`` at module level.
"""

import ast
from pathlib import Path

import pytest
import yaml

PROJECTS_DIR = Path(__file__).parent.parent / "projects"

# Name of the list each runner passes to ``Pipeline(steps=...)``.
STEP_LIST_NAME = "processing_steps"


def _project_dirs() -> list[Path]:
    """Every project directory holding both a config and a runner."""
    if not PROJECTS_DIR.is_dir():
        return []
    return sorted(
        d
        for d in PROJECTS_DIR.iterdir()
        if (d / "config.yaml").is_file() and (d / "run.py").is_file()
    )


def _config_step_names(config_path: Path) -> list[str]:
    """Step names declared in a project config, in execution order."""
    with config_path.open() as f:
        config = yaml.safe_load(f)
    return [step["name"] for step in (config or {}).get("steps", [])]


def _runner_step_names(run_path: Path) -> set[str]:
    """Names in the runner's ``processing_steps`` list, without importing it.

    The identifiers in that list are the registry keys: ``Pipeline`` keys by
    ``func.__name__``, and the ``@step`` decorator preserves it via
    ``functools.wraps``, so an imported or locally defined function registers
    under the same name it is referenced by here.
    """
    tree = ast.parse(run_path.read_text(encoding="utf-8"), filename=str(run_path))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.List):
            continue
        if any(
            isinstance(target, ast.Name) and target.id == STEP_LIST_NAME for target in node.targets
        ):
            return {elt.id for elt in node.value.elts if isinstance(elt, ast.Name)}

    pytest.fail(f"No `{STEP_LIST_NAME} = [...]` assignment found in {run_path}")
    return set()  # unreachable; keeps the return type honest


@pytest.mark.parametrize("project_dir", _project_dirs(), ids=lambda p: p.name)
def test_config_steps_are_registered_in_runner(project_dir: Path):
    """Every step named in a project config is passed to the Pipeline.

    The runner may register extra functions the config does not use (steps can
    be commented out of a config), so this is a subset check, not equality.
    """
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
    """A config must not name the same step twice.

    Steps are cached and looked up by name, so a duplicate silently runs the same
    function again and overwrites the first result's cache entry.
    """
    config_steps = _config_step_names(project_dir / "config.yaml")
    duplicates = {name for name in config_steps if config_steps.count(name) > 1}

    assert not duplicates, (
        f"{project_dir.name}/config.yaml declares duplicate step(s): "
        f"{', '.join(sorted(duplicates))}."
    )


def _step_params(config_path: Path, step_name: str) -> dict:
    """Params of the named step, or an empty dict if the config omits it."""
    with config_path.open() as f:
        config = yaml.safe_load(f)
    for step in (config or {}).get("steps", []):
        if step.get("name") == step_name:
            return step.get("params") or {}
    return {}


# Every consumer of a usability profile, and the key it names one under.
_PROFILE_CONSUMERS = (
    ("compute_weights", "usability_flag_col"),
    ("add_existing_weights", "usability_flag_col"),
    ("format_ctramp", "usability_flag_col"),
    ("format_daysim", "usability_flag_col"),
)

# Always available: the cascade stamps it on every run, whatever profiles a
# project declares.
_ALWAYS_STAMPED = frozenset({"complete"})


@pytest.mark.parametrize("project_dir", _project_dirs(), ids=lambda p: p.name)
def test_named_profiles_are_stamped_by_the_cascade(project_dir: Path):
    """Every profile a step names must be one ``cascade_completeness`` declares.

    The weight columns are suffixed with the profile name verbatim, so a typo
    here does not fail: it writes a column set no consumer can name, gated on a
    verdict no step stamps. Cheaper to catch statically than after a balancing
    run per profile.
    """
    config_path = project_dir / "config.yaml"
    declared = set(
        _step_params(config_path, "cascade_completeness").get("usability_profiles") or {}
    )
    if not declared:
        pytest.skip("project declares no usability profiles")
    legal = declared | _ALWAYS_STAMPED

    for step_name, key in _PROFILE_CONSUMERS:
        params = _step_params(config_path, step_name)
        named = [
            *([params[key]] if params.get(key) else []),
            *(params.get("weight_profiles") or []),
        ]
        for profile in named:
            assert profile in legal, (
                f"{project_dir.name}: step {step_name!r} names profile {profile!r}, which "
                f"cascade_completeness does not stamp. Declared: {sorted(legal)}"
            )


@pytest.mark.parametrize("project_dir", _project_dirs(), ids=lambda p: p.name)
def test_weighting_names_one_universe(project_dir: Path):
    """A weighting step names a single profile or a profile list, never both."""
    config_path = project_dir / "config.yaml"
    for step_name in ("compute_weights", "add_existing_weights"):
        params = _step_params(config_path, step_name)
        if not params:
            continue
        assert not (params.get("usability_flag_col") and params.get("weight_profiles")), (
            f"{project_dir.name}: step {step_name!r} names both usability_flag_col and "
            "weight_profiles; they mean different things about what gets fitted."
        )


def test_projects_are_discovered():
    """Guard the discovery itself.

    If the layout changes and no project is found, the parametrized tests above
    silently collect zero cases and pass, giving false assurance.
    """
    assert _project_dirs(), f"No project (config.yaml + run.py) found under {PROJECTS_DIR}"
