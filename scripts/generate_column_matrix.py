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
import data_canon.codebook.trips as trips_module
import data_canon.codebook.vehicles as vehicles_module

# Import processing package to trigger step registration via @step() decorators
import processing  # noqa: F401
from data_canon.core.labeled_enum import LabeledEnum
from data_canon.models.survey import (
    HouseholdModel,
    LinkedTripModel,
    PersonDayModel,
    PersonModel,
    TourModel,
    UnlinkedTripModel,
)
from pipeline.step_registry import get_step_validation_summary


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


def get_field_creation_info(model: type[BaseModel]) -> dict[str, str]:
    """Get information about which step creates each field.

    Args:
        model: Pydantic model class

    Returns:
        Dictionary mapping field names to the step that creates them
    """
    creation_info = {}
    for field_name, field_info in model.model_fields.items():
        extra = field_info.json_schema_extra or {}
        created_in_step = extra.get("created_in_step")
        if created_in_step:
            creation_info[field_name] = created_in_step
    return creation_info


def check_steps_and_order(steps: set[str], config_path: Path) -> list[str]:
    """Check steps against config and return ordered list."""
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))

    # Extract step order from config if available
    cfg_steps = [s["name"] for s in config.get("steps", [])]

    # Ensure we don't have a step not in the preferred order
    missing = steps - set(cfg_steps)
    if missing:
        msg = (
            f"Required model step(s) {missing} missing from config steps. "
            "Check the models and config file."
        )
        raise ValueError(msg)

    # Remove repeat config steps while preserving order
    seen = set()
    ordered_steps = []
    for s in cfg_steps:
        if s not in seen:
            ordered_steps.append(s)
        seen.add(s)

    return ordered_steps


def generate_matrix_markdown(models: dict[str, type]) -> str:  # noqa: PLR0915
    """Generate tabbed markdown showing column requirements per table.

    Each canonical table gets its own tab with a compact matrix that only
    includes pipeline steps relevant to that table.

    Args:
        models: Dictionary mapping table names to model classes

    Returns:
        Markdown formatted string with pymdownx content tabs
    """
    # Get registry data: {step_name: {table_name: [fields]}}
    registry_summary = get_step_validation_summary()
    required_steps = set(registry_summary.keys())

    creation_info = {}
    for table_name, model in models.items():
        creation_info[table_name] = get_field_creation_info(model)

    # Read projects/config.yaml for preferred order if available
    example_path = Path(__file__).parent.parent / "projects" / "bats_2023" / "config.yaml"
    sorted_steps = check_steps_and_order(required_steps, example_path)

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
        "Only steps that reference the table are shown."
    )
    lines.append("")
    lines.append("| Symbol | Meaning |")
    lines.append("| :----: | ------- |")
    lines.append("| ✓ | Required as input |")
    lines.append("| + | Created / produced |")
    lines.append("")

    for table_name, model in models.items():
        # Determine which steps are relevant to this table
        table_steps = [s for s in sorted_steps if table_name in registry_summary.get(s, {})]

        field_creation = creation_info[table_name]
        all_fields = list(model.model_fields.keys())

        # Tab header (pymdownx tabbed alternate style)
        lines.append(f'=== "{table_name}"')
        lines.append("")

        if table_steps:
            # Build compact header
            header = ["Field", "Type", "Constraints", *table_steps]
            lines.append("    | " + " | ".join(header) + " |")
            lines.append("    | " + " | ".join(["---"] * len(header)) + " |")

            for field_name in all_fields:
                field_info = model.model_fields[field_name]
                field_type = get_field_type_description(field_info)
                constraints = get_field_constraints(field_info)
                row = [f"`{field_name}`", field_type, constraints]

                for step in table_steps:
                    step_fields = registry_summary.get(step, {}).get(table_name, [])
                    created_in = field_creation.get(field_name)

                    if created_in == step:
                        row.append("+")
                    elif field_name in step_fields:
                        row.append("✓")
                    else:
                        row.append("")

                lines.append("    | " + " | ".join(row) + " |")
        else:
            lines.append("    No step-specific field requirements registered.")

        lines.append("")

    return "\n".join(lines)


def generate_matrix_csv(models: dict[str, type]) -> str:
    """Generate CSV showing column requirements per step.

    Args:
        models: Dictionary mapping table names to model classes

    Returns:
        CSV formatted string
    """
    # Get registry data: {step_name: {table_name: [fields]}}
    registry_summary = get_step_validation_summary()
    required_steps = set(registry_summary.keys())

    creation_info = {}
    for table_name, model in models.items():
        creation_info[table_name] = get_field_creation_info(model)

    # Sort steps for consistent ordering
    example_path = Path(__file__).parent.parent / "projects" / "bats_2023" / "config.yaml"
    sorted_steps = check_steps_and_order(required_steps, example_path)

    # Build CSV
    lines = []
    header = ["Table", "Field", "Type", "Constraints", *sorted_steps]
    lines.append(",".join(header))

    for table_name, model in models.items():
        field_creation = creation_info[table_name]

        # Get all fields from model
        all_fields = list(model.model_fields.keys())

        for field_name in all_fields:
            field_info = model.model_fields[field_name]
            field_type = get_field_type_description(field_info)
            constraints = get_field_constraints(field_info)

            row = [table_name, field_name, field_type, constraints]

            # Check each step
            for step in sorted_steps:
                step_tables = registry_summary.get(step, {})
                step_fields = step_tables.get(table_name, [])
                created_in = field_creation.get(field_name)

                if created_in == step:
                    row.append("+")
                elif field_name in step_fields:
                    row.append("x")
                else:
                    row.append("")

            lines.append(",".join(row))

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
    models = {
        "households": HouseholdModel,
        "persons": PersonModel,
        "days": PersonDayModel,
        "unlinked_trips": UnlinkedTripModel,
        "linked_trips": LinkedTripModel,
        "tours": TourModel,
    }

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
