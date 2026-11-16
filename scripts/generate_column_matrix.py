"""Generate column requirement matrix from data models.

This script reads the step metadata from the step registry and generates
a documentation matrix showing which columns are required in which
pipeline steps.
"""

import inspect
import sys
import types
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import yaml
from pydantic import BaseModel
from pydantic.fields import FieldInfo

import data_canon.codebook.days as days_module
import data_canon.codebook.households as households_module
import data_canon.codebook.persons as persons_module
import data_canon.codebook.tours as tours_module
import data_canon.codebook.trips as trips_module
import data_canon.codebook.vehicles as vehicles_module

# Import processing package to trigger step registration via @step() decorators
import processing  # noqa: F401
from data_canon.core.dataclass import CanonicalData
from data_canon.core.labeled_enum import LabeledEnum
from pipeline.step_registry import STEP_REGISTRY, is_config_named


def get_field_type_description(field_info: FieldInfo) -> str:
    """Extract human-readable type description from field.

    Args:
        field_info: Pydantic FieldInfo object

    Returns:
        String describing the field type
    """
    annotation = field_info.annotation

    # Resolve the core type, stripping Optional/Union wrappers
    def _short_name(t: type) -> str:
        return getattr(t, "__name__", str(t).rsplit(".", 1)[-1])

    # Handle Union types (X | None, Optional[X], Union[X, Y])
    if isinstance(annotation, types.UnionType) or (
        hasattr(annotation, "__origin__") and str(annotation.__origin__) == "typing.Union"
    ):
        args = annotation.__args__
        non_none = [arg for arg in args if arg is not type(None)]
        names = [_short_name(a) for a in non_none]
        result = " or ".join(names)
        if type(None) in args:
            return f"{result} or None"
        return result

    # Simple type
    return _short_name(annotation)


def get_field_constraints(field_info: FieldInfo) -> str:
    """Extract validation constraints from field.

    Args:
        field_info: Pydantic FieldInfo object

    Returns:
        String describing constraints
    """
    constraints = []

    # Get constraints from metadata
    if hasattr(field_info, "metadata"):
        for item in field_info.metadata:
            if hasattr(item, "ge") and item.ge is not None:
                constraints.append(f"≥ {item.ge}")
            if hasattr(item, "le") and item.le is not None:
                constraints.append(f"≤ {item.le}")
            if hasattr(item, "gt") and item.gt is not None:
                constraints.append(f"> {item.gt}")
            if hasattr(item, "lt") and item.lt is not None:
                constraints.append(f"< {item.lt}")

    # Get constraints from json_schema_extra
    extra = field_info.json_schema_extra or {}

    if extra.get("unique", False):
        constraints.append("UNIQUE")

    if fk_to := extra.get("fk_to"):
        constraints.append(f"FK → `{fk_to}`")

    if extra.get("required_child", False):
        constraints.append("REQ_CHILD")

    return ", ".join(constraints) if constraints else ""


# Reference config: its step order sets the matrix columns.
REFERENCE_CONFIG = Path(__file__).parent.parent / "projects" / "bats_2023" / "config.yaml"


def ordered_steps(config_path: Path) -> list[str]:
    """Every step with a contract: the reference config's order, then the rest by name.

    A step the reference config does not run still has a contract worth showing,
    so it is listed after the ones it does run rather than left out.
    """
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    in_config = list(dict.fromkeys(s["name"] for s in config.get("steps", [])))
    declared = {name for name, contract in STEP_REGISTRY.items() if contract.tables}
    return [s for s in in_config if s in declared] + sorted(declared - set(in_config))


def _mark(step: str, table: str, field_name: str, required: str) -> str:
    """The cell for one step and field: *required* if read, ``+`` if produced."""
    tc = STEP_REGISTRY[step].tables.get(table)
    if tc is None:
        return ""
    return (required if field_name in tc.requires else "") + (
        "+" if field_name in tc.produces else ""
    )


def table_rows(
    table: str, model: type[BaseModel], steps: list[str], required: str
) -> list[list[str]]:
    """``[field, type, constraints, *marks]`` for each model field, then each pattern.

    A pattern such as ``usable_<profile>`` is a column the config names, so no
    model declares it; it gets its own row after the declared fields.

    Raises:
        ValueError: A contract names a column that is neither a model field nor
            a pattern -- a typo, or a field the model is missing.
    """
    contracts = [STEP_REGISTRY[s].tables[table] for s in steps if table in STEP_REGISTRY[s].tables]
    named = set().union(*(tc.requires | tc.produces for tc in contracts)) if contracts else set()
    unknown = sorted(f for f in named if f not in model.model_fields and not is_config_named(f))
    if unknown:
        msg = f"Step contracts name {table} columns no model declares: {unknown}"
        raise ValueError(msg)

    rows = [
        [
            name,
            get_field_type_description(info),
            get_field_constraints(info),
            *(_mark(s, table, name, required) for s in steps),
        ]
        for name, info in model.model_fields.items()
    ]
    rows += [
        [name, "named by config", "", *(_mark(s, table, name, required) for s in steps)]
        for name in sorted(f for f in named if is_config_named(f))
    ]
    return rows


def _steps_for(table: str, steps: list[str]) -> list[str]:
    """The steps whose contract mentions *table*."""
    return [s for s in steps if table in STEP_REGISTRY[s].tables]


def generate_matrix_markdown(models: dict[str, type[BaseModel]]) -> str:
    """Generate tabbed markdown showing column requirements per table.

    Each canonical table gets its own tab with a compact matrix that only
    includes pipeline steps relevant to that table.

    Args:
        models: Dictionary mapping table names to model classes

    Returns:
        Markdown formatted string with pymdownx content tabs
    """
    sorted_steps = ordered_steps(REFERENCE_CONFIG)

    lines: list[str] = []
    # YAML frontmatter for MkDocs page metadata
    lines.append("---")
    lines.append("body_class: col-matrix")
    lines.append("hide:")
    lines.append("  - toc")
    lines.append("---")
    lines.append("")
    lines.append("# Column Requirement Matrix")
    lines.append("")
    lines.append("Generated automatically by `scripts/generate_column_matrix.py`.")
    lines.append("")
    lines.append("***Do not edit this markdown file directly.***")
    lines.append("")
    lines.append(
        "Each tab shows the fields for one canonical table. "
        "Only steps that reference the table are shown. Rows such as "
        "`usable_<profile>` are columns the config names, one per profile or zone."
    )
    lines.append("")
    lines.append("| Symbol | Meaning |")
    lines.append("| :----: | ------- |")
    lines.append("| ✓ | Required as input |")
    lines.append("| + | Created / produced |")
    lines.append("| ✓+ | Required, then updated |")
    lines.append("")

    for table_name, model in models.items():
        table_steps = _steps_for(table_name, sorted_steps)

        # Tab header (pymdownx tabbed alternate style)
        lines.append(f'=== "{table_name}"')
        lines.append("")

        if table_steps:
            header = ["Field", "Type", "Constraints", *table_steps]
            lines.append("    | " + " | ".join(header) + " |")
            lines.append("    | " + " | ".join(["---"] * len(header)) + " |")
            for name, *rest in table_rows(table_name, model, table_steps, "✓"):
                lines.append("    | " + " | ".join([f"`{name}`", *rest]) + " |")
        else:
            lines.append("    No step-specific field requirements registered.")

        lines.append("")

    return "\n".join(lines)


def generate_matrix_csv(models: dict[str, type[BaseModel]]) -> str:
    """Generate CSV showing column requirements per step.

    Args:
        models: Dictionary mapping table names to model classes

    Returns:
        CSV formatted string
    """
    sorted_steps = ordered_steps(REFERENCE_CONFIG)

    lines = [",".join(["Table", "Field", "Type", "Constraints", *sorted_steps])]
    for table_name, model in models.items():
        lines.extend(
            ",".join([table_name, *row]) for row in table_rows(table_name, model, sorted_steps, "x")
        )
    return "\n".join(lines)


def collect_labeled_enums() -> dict[str, type]:
    """Collect all LabeledEnum classes from codebook modules.

    Returns:
        Dictionary mapping enum class names to enum classes
    """
    modules = [
        days_module,
        households_module,
        persons_module,
        tours_module,
        trips_module,
        vehicles_module,
    ]

    return {
        name: obj
        for module in modules
        for name, obj in inspect.getmembers(module)
        # Check if it's a class, subclass of LabeledEnum,
        # and not LabeledEnum itself
        if (
            inspect.isclass(obj)
            and issubclass(obj, LabeledEnum)
            and obj is not LabeledEnum
            and not name.endswith("Map")  # Skip mapping classes
        )
    }


def generate_enum_codebook_markdown(enums: dict[str, type]) -> str:
    """Generate markdown table showing enum values and labels.

    Args:
        enums: Dictionary mapping enum class names to enum classes

    Returns:
        Markdown formatted table string
    """
    lines = []
    lines.append("# Enums")
    lines.append("")
    lines.append("Quick lookup of categorical values and labels for codebook enum fields.")
    lines.append("")

    # Sort enums by name for consistent ordering
    sorted_enum_names = sorted(enums.keys())

    for enum_name in sorted_enum_names:
        enum_class = enums[enum_name]

        # Get field name and description if available
        field_name = enum_class.get_field_name()
        description = enum_class.get_description()

        # Create section header
        lines.append(f"## {enum_name}")
        lines.append("")

        if field_name:
            lines.append(f"**Field name:** `{field_name}`")
            lines.append("")

        if description:
            lines.append(f"**Description:** {description}")
            lines.append("")

        # Create table header
        lines.append("| Value | Label |")
        lines.append("| --- | --- |")

        # Add enum members
        lines.extend(
            [f"| {member.value} | {member.label} |" for member in enum_class.__members__.values()]
        )

        lines.append("")

    return "\n".join(lines)


def main() -> None:
    """Generate and save column requirement matrices."""
    models = CanonicalData().models

    # Collect enum classes
    enums = collect_labeled_enums()

    # Generate column matrix page (wide, no TOC)
    markdown = generate_matrix_markdown(models)

    output_dir = Path(__file__).parent.parent / "docs"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "COLUMN_REQUIREMENTS.md"
    markdown_lines = [line.rstrip() for line in markdown.splitlines()]
    markdown_formatted = "\n".join(markdown_lines) + "\n"
    output_path.write_text(markdown_formatted, encoding="utf-8")
    print(f"Generated: {output_path}")

    # Generate enum codebook page (separate, with TOC)
    enum_markdown = generate_enum_codebook_markdown(enums)
    enum_path = output_dir / "codebook_enums.md"
    enum_lines = [line.rstrip() for line in enum_markdown.splitlines()]
    enum_formatted = "\n".join(enum_lines) + "\n"
    enum_path.write_text(enum_formatted, encoding="utf-8")
    print(f"Generated: {enum_path}")

    # Generate CSV in scripts folder
    csv = generate_matrix_csv(models)
    csv_path = output_dir / "column_requirements.csv"
    # Remove trailing whitespace and ensure single trailing newline
    csv_lines = [line.rstrip() for line in csv.splitlines()]
    csv_formatted = "\n".join(csv_lines) + "\n"
    csv_path.write_text(csv_formatted, encoding="utf-8")
    print(f"Generated: {csv_path}")


if __name__ == "__main__":
    main()
