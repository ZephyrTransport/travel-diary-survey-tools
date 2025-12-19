"""Generate column requirement matrix from data models.

This script reads the step metadata from Pydantic models and generates
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

import data_canon.codebook.days as days_module
import data_canon.codebook.households as households_module
import data_canon.codebook.persons as persons_module
import data_canon.codebook.trips as trips_module
import data_canon.codebook.vehicles as vehicles_module
from data_canon.core.labeled_enum import LabeledEnum
from data_canon.models.survey import (
    HouseholdModel,
    LinkedTripModel,
    PersonDayModel,
    PersonModel,
    TourModel,
    UnlinkedTripModel,
)
from data_canon.validation.row import get_step_validation_summary


def get_field_type_description(field_info: object) -> str:
    """Extract human-readable type description from field.

    Args:
        field_info: Pydantic FieldInfo object

    Returns:
        String describing the field type
    """
    annotation = field_info.annotation

    # Handle Optional types (Union with None)
    if hasattr(annotation, "__origin__"):
        origin = annotation.__origin__
        # Check for Union type (includes | syntax)
        if origin is types.UnionType or str(origin) == "typing.Union":
            args = annotation.__args__
            non_none = [arg for arg in args if arg is not type(None)]
            if non_none and len(non_none) == 1:
                return non_none[0].__name__
            # Multiple non-None types
            type_names = [arg.__name__ for arg in non_none]
            return " or ".join(type_names)

    # Simple type
    if hasattr(annotation, "__name__"):
        return annotation.__name__

    # Convert to string and replace pipe with "or" for markdown compatibility
    # Replace " | " with " or " to avoid breaking markdown table delimiters
    return str(annotation).replace(" | ", " or ")


def get_field_constraints(field_info: object) -> str:
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


def generate_matrix_markdown(models: dict[str, type]) -> str:  # noqa: C901, PLR0912, PLR0915
    """Generate markdown table showing column requirements per step.

    Args:
        models: Dictionary mapping table names to model classes

    Returns:
        Markdown formatted table string
    """
    # Collect all unique steps across all models
    required_steps = set()
    model_summaries = {}
    creation_info = {}

    for table_name, model in models.items():
        summary = get_step_validation_summary(model)
        model_summaries[table_name] = summary
        creation_info[table_name] = get_field_creation_info(model)
        for step in summary:
            if step != "ALL":
                required_steps.add(step)

    # Read projects/config.yaml for preferred order if available
    example_path = (
        Path(__file__).parent.parent / "projects" / "bats_2023" / "config.yaml"
    )

    sorted_steps = check_steps_and_order(required_steps, example_path)

    # Build markdown table
    lines = []
    lines.append("# Column Requirement Matrix")
    lines.append(
        "Generated automatically by `scripts/generate_column_matrix.py`."
    )
    lines.append("")
    lines.append("***Do not edit this markdown file directly.***")
    lines.append("")
    lines.append(
        "This matrix shows which columns are required in which pipeline steps. "
    )
    lines.append("- ✓ = required in step")
    lines.append("- \\+ = created in step")
    lines.append("")
    lines.append("## Constraint Legend")
    lines.append("")
    lines.append(
        "- **UNIQUE**: Field must have unique values across all records"
    )
    lines.append(
        "- **FK → `table.column`**: Foreign key reference to parent table"
    )
    lines.append(
        "- **REQ_CHILD**: Parent record must have at least one child record"
    )
    lines.append("- **≥ / ≤ / > / <**: Numeric range constraints")
    lines.append("")

    # Create table header
    header = ["Table", "Field", "Type", "Constraints", *sorted_steps]
    lines.append("| " + " | ".join(header) + " |")
    lines.append("| " + " | ".join(["---"] * len(header)) + " |")

    for table_name, model in models.items():
        summary = model_summaries[table_name]
        all_steps_fields = set(summary.get("ALL", []))
        field_creation = creation_info[table_name]

        # Get all fields from model
        all_fields = list(model.model_fields.keys())

        # Create rows for each field
        for i, field_name in enumerate(all_fields):
            field_info = model.model_fields[field_name]
            field_type = get_field_type_description(field_info)
            constraints = get_field_constraints(field_info)

            # Add table name only in first row for this table
            if i == 0:
                row = [
                    f"**{table_name}**",
                    f"`{field_name}`",
                    field_type,
                    constraints,
                ]
            else:
                # Other rows - leave table name blank
                row = ["", f"`{field_name}`", field_type, constraints]

            # Check if field is required in ALL steps
            if field_name in all_steps_fields:
                # Check if created in any step
                for step in sorted_steps:
                    created_in = field_creation.get(field_name)
                    if created_in == step:
                        row.append("+")
                    else:
                        row.append("✓")
            else:
                # Check each step
                for step in sorted_steps:
                    step_fields = summary.get(step, [])
                    created_in = field_creation.get(field_name)

                    if created_in == step:
                        # Field is created in this step
                        row.append("+")
                    elif field_name in step_fields:
                        # Field is required in this step
                        row.append("✓")
                    else:
                        row.append("")

            lines.append("| " + " | ".join(row) + " |")

    lines.append("")

    return "\n".join(lines)


def generate_matrix_csv(models: dict[str, type]) -> str:  # noqa: C901, PLR0912
    """Generate CSV showing column requirements per step.

    Args:
        models: Dictionary mapping table names to model classes

    Returns:
        CSV formatted string
    """
    # Collect all unique steps across all models
    required_steps = set()
    model_summaries = {}
    creation_info = {}

    for table_name, model in models.items():
        summary = get_step_validation_summary(model)
        model_summaries[table_name] = summary
        creation_info[table_name] = get_field_creation_info(model)
        for step in summary:
            if step != "ALL":
                required_steps.add(step)

    # Sort steps for consistent ordering
    example_path = (
        Path(__file__).parent.parent / "projects" / "bats_2023" / "config.yaml"
    )
    sorted_steps = check_steps_and_order(required_steps, example_path)

    # Build CSV
    lines = []
    header = ["Table", "Field", "Type", "Constraints", *sorted_steps]
    lines.append(",".join(header))

    for table_name, model in models.items():
        summary = model_summaries[table_name]
        all_steps_fields = set(summary.get("ALL", []))
        field_creation = creation_info[table_name]

        # Get all fields from model
        all_fields = list(model.model_fields.keys())

        for field_name in all_fields:
            field_info = model.model_fields[field_name]
            field_type = get_field_type_description(field_info)
            constraints = get_field_constraints(field_info)

            row = [table_name, field_name, field_type, constraints]

            # Check if field is required in ALL steps
            if field_name in all_steps_fields:
                # Check if any are creation steps
                for step in sorted_steps:
                    created_in = field_creation.get(field_name)
                    if created_in == step:
                        row.append("+")
                    else:
                        row.append("x")
            else:
                # Check each step
                for step in sorted_steps:
                    step_fields = summary.get(step, [])
                    created_in = field_creation.get(field_name)

                    if created_in == step:
                        # Field is created in this step
                        row.append("+")
                    elif field_name in step_fields:
                        # Field is required in this step
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
    lines.append("# Codebook Enum Values")
    lines.append("")
    lines.append(
        "This section shows the categorical values and labels "
        "for custom enum fields."
    )
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
            [f"| {member.value} | {member.label} |" for member in enum_class]
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

    # Generate markdown in repo root
    markdown = generate_matrix_markdown(models)

    # Add enum codebook section
    enum_markdown = generate_enum_codebook_markdown(enums)
    markdown += "\n\n" + enum_markdown

    output_dir = Path(__file__).parent.parent / "docs"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / "COLUMN_REQUIREMENTS.md"
    output_path.write_text(markdown, encoding="utf-8")
    print(f"Generated: {output_path}")

    # Generate CSV in scripts folder
    csv = generate_matrix_csv(models)
    csv_path = output_dir / "column_requirements.csv"
    csv_path.write_text(csv, encoding="utf-8")
    print(f"Generated: {csv_path}")


if __name__ == "__main__":
    main()
