"""Fail when a module under ``src/`` is longer than the line limit.

The limit and the baseline live in ``pyproject.toml`` under ``[tool.module-length]``.
Modules already over the limit when the check was added are listed in the baseline
at their length then: each may shrink but not grow, and comes off the list once
it is within the limit. Any other module over the limit fails outright.

Lines are raw lines, as an editor or ``wc -l`` counts them.

Run by pre-commit and by ``tests/test_module_length.py``; exits 1 on any problem.
"""

import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SECTION = "[tool.module-length.baseline]"


def problems(root: Path = ROOT) -> list[str]:
    """Every module that breaks the limit or its baseline, and every stale entry."""
    settings = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    config = settings["tool"]["module-length"]
    limit: int = config["max-lines"]
    baseline: dict[str, int] = config.get("baseline", {})

    found: list[str] = []
    seen: set[str] = set()
    for path in sorted((root / "src").rglob("*.py")):
        rel = path.relative_to(root).as_posix()
        seen.add(rel)
        lines = len(path.read_text(encoding="utf-8").splitlines())
        allowed = baseline.get(rel)
        if allowed is None:
            if lines > limit:
                found.append(f"{rel}: {lines} lines, over the {limit}-line limit. Split it.")
        elif lines > allowed:
            found.append(
                f"{rel}: {lines} lines, up from {allowed} in {SECTION}. "
                "A listed module may shrink but not grow."
            )
        elif lines <= limit:
            found.append(f"{rel}: {lines} lines, now within the limit. Remove it from {SECTION}.")

    found.extend(
        f"{rel}: listed in {SECTION} but no longer exists. Remove it."
        for rel in sorted(set(baseline) - seen)
    )
    return found


def main() -> int:
    """Print every problem and return the exit code."""
    found = problems()
    for line in found:
        print(line)
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
