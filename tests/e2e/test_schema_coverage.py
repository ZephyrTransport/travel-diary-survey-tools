"""Every column the pipeline computes must be declared on its canonical model.

The canonical models are a *floor*, not an exhaustive output spec: row
validation filters each row to the model's fields, so undeclared columns pass
through silently. That is deliberate for vendor data -- the raw survey extract
is carried through untouched and its hundreds of columns are not ours to
document. It is not acceptable for columns the pipeline itself derives: those
are a contract we own, and an undeclared one reaches the delivered CSV with no
schema, no validation, and no codebook entry (``_collect_enums_for_tables``
reads enums off model annotations, so an undeclared enum column ships as bare
integers nobody can resolve).

This test draws that line mechanically. A column must be one of:

* declared on the table's model,
* present in the raw input (vendor passthrough), or
* an instance of a documented generated family (see ``_GENERATED_SUFFIXES``).

Anything else fails here, at the moment it is added, rather than being found
later by reading parquet schemas.
"""

import polars as pl
import pytest
import yaml

from data_canon.core.dataclass import CanonicalData

pytestmark = [pytest.mark.e2e, pytest.mark.slow]

# Column families generated from configuration rather than fixed in a model.
# ``stash_preimputed_columns`` emits ``{column}_preimputed`` for whatever columns
# a project names, so the set differs per project and cannot be a static field.
_GENERATED_SUFFIXES = ("_preimputed",)

# Prefixes ``add_zone_ids`` attaches a zone name to, e.g. ``o_taz``, ``home_maz``.
_ZONE_PREFIXES = ("o", "d", "home", "work", "school")


def _raw_columns(input_dir) -> set[str]:
    """Every column across the raw survey extract -- the vendor surface."""
    cols: set[str] = set()
    for path in (input_dir / "survey").glob("*.parquet"):
        cols |= set(pl.read_parquet_schema(path).keys())
    return cols


def _zone_columns(output_dir) -> set[str]:
    """Zone columns this run's config asks for.

    ``add_zone_ids`` names its output ``{prefix}_{zone_name}`` where zone_name
    comes from config -- "taz"/"maz" here, "TAZ1454" and others in production --
    so the set is per-project and cannot be declared as fixed model fields.
    Reading it back from the config keeps the exclusion exact rather than a
    loose pattern that would also swallow genuine mistakes.
    """
    config = yaml.safe_load((output_dir / "config.yaml").read_text())
    names = [
        geo["zone_name"]
        for step in config.get("steps", [])
        if step.get("name") == "add_zone_ids"
        for geo in step.get("params", {}).get("zone_geographies", [])
    ]
    return {f"{prefix}_{name}" for name in names for prefix in _ZONE_PREFIXES}


def test_computed_columns_are_declared(full_result, full_input_dir, full_output_dir):
    """No pipeline-derived column reaches a canonical table undeclared."""
    vendor = _raw_columns(full_input_dir) | _zone_columns(full_output_dir)
    models = CanonicalData().models

    undeclared: dict[str, list[str]] = {}
    for table, model in models.items():
        df = getattr(full_result, table, None)
        if df is None or not isinstance(df, pl.DataFrame):
            continue
        declared = set(model.model_fields)
        extra = [
            c
            for c in df.columns
            if c not in declared
            and c not in vendor
            and not c.startswith("_")
            and not c.endswith(_GENERATED_SUFFIXES)
        ]
        if extra:
            undeclared[table] = sorted(extra)

    assert not undeclared, (
        "Columns computed by the pipeline but absent from their canonical model.\n"
        "Declare them with schema_field(), or drop them before the table is\n"
        "returned if they are internal working columns:\n"
        + "\n".join(f"  {t}: {', '.join(cols)}" for t, cols in sorted(undeclared.items()))
    )
