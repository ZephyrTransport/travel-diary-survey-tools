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


def test_projects_are_discovered():
    """Guard the discovery itself.

    If the layout changes and no project is found, the parametrized tests above
    silently collect zero cases and pass, giving false assurance.
    """
    assert _project_dirs(), f"No project (config.yaml + run.py) found under {PROJECTS_DIR}"
