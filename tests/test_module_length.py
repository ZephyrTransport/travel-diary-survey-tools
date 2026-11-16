"""The module-length check: the repo passes it in CI, and each of its rules holds."""

import importlib.util
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).parent.parent / "scripts" / "check_module_length.py"
_spec = importlib.util.spec_from_file_location("check_module_length", _SCRIPT)
check_module_length = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check_module_length)


def test_the_repo_passes():
    """No module is over the limit except the grandfathered ones, none of which grew."""
    assert check_module_length.problems() == []


def _tree(tmp_path: Path, modules: dict[str, int], baseline: dict[str, int]) -> Path:
    """A fake repo: modules of the given line counts and a limit of 10."""
    for rel, lines in modules.items():
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x = 1\n" * lines, encoding="utf-8")
    listed = "\n".join(f'"{rel}" = {n}' for rel, n in baseline.items())
    (tmp_path / "pyproject.toml").write_text(
        f"[tool.module-length]\nmax-lines = 10\n\n[tool.module-length.baseline]\n{listed}\n",
        encoding="utf-8",
    )
    return tmp_path


@pytest.mark.parametrize(
    ("modules", "baseline", "message"),
    [
        pytest.param({"src/a.py": 11}, {}, "over the 10-line limit", id="new_module_over"),
        pytest.param({"src/a.py": 21}, {"src/a.py": 20}, "up from 20", id="listed_module_grew"),
        pytest.param({"src/a.py": 9}, {"src/a.py": 20}, "within the limit", id="listed_now_fits"),
        pytest.param({}, {"src/a.py": 20}, "no longer exists", id="listed_deleted"),
    ],
)
def test_a_rule_is_reported(tmp_path, modules, baseline, message):
    """Each way a module breaks the check names the module and what to do."""
    (problem,) = check_module_length.problems(_tree(tmp_path, modules, baseline))
    assert problem.startswith("src/a.py")
    assert message in problem


@pytest.mark.parametrize(
    ("modules", "baseline"),
    [
        pytest.param({"src/a.py": 10}, {}, id="at_the_limit"),
        pytest.param({"src/a.py": 15}, {"src/a.py": 20}, id="listed_module_shrank"),
        pytest.param({"src/a.py": 20}, {"src/a.py": 20}, id="listed_module_unchanged"),
    ],
)
def test_allowed_cases_pass(tmp_path, modules, baseline):
    """At the limit, or a listed module no longer than it was, passes."""
    assert check_module_length.problems(_tree(tmp_path, modules, baseline)) == []
