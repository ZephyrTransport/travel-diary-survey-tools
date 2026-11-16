"""Generate the Developer Reference: one page per package under ``src/``.

Run by the mkdocs-gen-files plugin during the docs build; the pages and the
menu (``reference/SUMMARY.md``, read by mkdocs-literate-nav) exist only in the
build, so a new or moved module appears without editing the docs.

A package's page holds its own description, then one section per module in it.
A package with sub-packages is a menu section whose first entry is that page.
"""

from pathlib import Path

import mkdocs_gen_files

SRC = Path(__file__).parent.parent / "src"

# Packages whose classes a user page already documents: their page points there instead,
# so each class has one entry for links to resolve to. Paths are from the reference page.
DOCUMENTED_ELSEWHERE = {
    "data_canon/models": "The data models are documented on the "
    "[Data Models](../../models/index.md) "
    "pages: the survey tables, and the DaySim and CT-RAMP output formats.",
}

# Per-block options: public names only, and no module-level loggers.
OPTIONS = """    options:
      members: null
      filters: ["!^_", "!^logger$"]
"""


def page(package: Path) -> str:
    """Markdown for one package: its description, then each module."""
    dotted = ".".join(package.relative_to(SRC).parts)
    lines = [
        f"# {dotted}",
        "",
        f"::: {dotted}",
        "    options:",
        "      show_root_heading: false",
        "      members: false",
        "",
    ]
    rel = package.relative_to(SRC).as_posix()
    if rel in DOCUMENTED_ELSEWHERE:
        return "\n".join([*lines, DOCUMENTED_ELSEWHERE[rel], ""])
    for module in sorted(package.glob("*.py")):
        if module.name.startswith("_"):
            continue
        lines += [f"::: {dotted}.{module.stem}", OPTIONS]
    return "\n".join(lines)


def packages() -> list[Path]:
    """Every package under src/, parents before children."""
    return sorted(p.parent for p in SRC.rglob("__init__.py") if "__pycache__" not in p.parts)


def doc_path(package: Path, all_packages: list[Path]) -> Path:
    """index.md for a package with sub-packages, else <name>.md beside its parent's."""
    rel = package.relative_to(SRC)
    has_children = any(p.parent == package for p in all_packages)
    return rel / "index.md" if has_children else rel.with_suffix(".md")


def summary(all_packages: list[Path]) -> str:
    """The literate-nav menu, nested like src/."""
    lines = []
    for package in all_packages:
        rel = package.relative_to(SRC)
        indent = "    " * (len(rel.parts) - 1)
        path = doc_path(package, all_packages).as_posix()
        if path.endswith("index.md"):
            lines.append(f"{indent}* {rel.name}")
            lines.append(f"{indent}    * [Overview]({path})")
        else:
            lines.append(f"{indent}* [{rel.name}]({path})")
    return "\n".join(lines) + "\n"


all_packages = packages()
for package in all_packages:
    target = Path("reference") / doc_path(package, all_packages)
    with mkdocs_gen_files.open(target, "w") as f:
        f.write(page(package))
    mkdocs_gen_files.set_edit_path(target, package.relative_to(SRC.parent) / "__init__.py")

with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as f:
    f.write(summary(all_packages))
