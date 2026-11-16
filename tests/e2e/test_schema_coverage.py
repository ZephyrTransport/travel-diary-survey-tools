"""Every column the pipeline computes must be declared on its canonical model.

Row validation passes undeclared columns through, which is right for survey columns
carried from the raw input and wrong for derived ones: an undeclared derived column
ships with no schema, validation or codebook entry. A column must be declared on the
model, present in the raw input, or registered by the step that generated it.
"""

import polars as pl

from data_canon.core.dataclass import CanonicalData


def _raw_columns(input_dir) -> set[str]:
    """Every column across the raw survey input."""
    cols: set[str] = set()
    for path in (input_dir / "survey").glob("*.parquet"):
        cols |= set(pl.read_parquet_schema(path).keys())
    return cols


def test_computed_columns_are_declared(full_result, full_input_dir):
    """No pipeline-derived column reaches a canonical table undeclared."""
    raw = _raw_columns(full_input_dir)

    undeclared: dict[str, list[str]] = {}
    for table in CanonicalData().models:
        df = getattr(full_result, table, None)
        if df is None or not isinstance(df, pl.DataFrame):
            continue
        # Declared fields plus the columns the generating steps registered.
        public = full_result.public_columns(table)
        extra = [
            c for c in df.columns if c not in public and c not in raw and not c.startswith("_")
        ]
        if extra:
            undeclared[table] = sorted(extra)

    assert not undeclared, (
        "Columns computed by the pipeline but absent from their canonical model.\n"
        "Declare them with schema_field(), or drop them before the table is\n"
        "returned if they are internal working columns:\n"
        + "\n".join(f"  {t}: {', '.join(cols)}" for t, cols in sorted(undeclared.items()))
    )
