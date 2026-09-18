"""Shared assertion helpers for E2E integration tests."""

import polars as pl

from data_canon.core.dataclass import CanonicalData

# ---------------------------------------------------------------------------
# Table presence
# ---------------------------------------------------------------------------


def assert_tables_non_empty(data: CanonicalData, table_names: list[str]):
    """Assert that each named table exists and has at least one row."""
    for name in table_names:
        df = getattr(data, name, None)
        assert df is not None, f"Table '{name}' is None"
        assert isinstance(df, pl.DataFrame), f"Table '{name}' is not a DataFrame"
        assert df.height > 0, f"Table '{name}' is empty"


# ---------------------------------------------------------------------------
# Referential integrity
# ---------------------------------------------------------------------------

_FK_CHAINS = [
    ("persons", "hh_id", "households", "hh_id"),
    ("days", "person_id", "persons", "person_id"),
    ("unlinked_trips", "day_id", "days", "day_id"),
    ("linked_trips", "day_id", "days", "day_id"),
    ("linked_trips", "person_id", "persons", "person_id"),
    ("tours", "person_id", "persons", "person_id"),
    ("tours", "hh_id", "households", "hh_id"),
    ("tours", "day_id", "days", "day_id"),
]


def assert_referential_integrity(data: CanonicalData):
    """Check that all foreign key references resolve to existing parent rows."""
    for child_table, child_col, parent_table, parent_col in _FK_CHAINS:
        child = getattr(data, child_table, None)
        parent = getattr(data, parent_table, None)
        if child is None or parent is None:
            continue
        if child.height == 0 or parent.height == 0:
            continue
        if child_col not in child.columns or parent_col not in parent.columns:
            continue

        parent_ids = set(parent[parent_col].drop_nulls().to_list())
        child_ids = set(child[child_col].drop_nulls().to_list())
        orphans = child_ids - parent_ids
        assert not orphans, (
            f"{child_table}.{child_col} has {len(orphans)} orphaned FK(s) "
            f"not in {parent_table}.{parent_col}: {list(orphans)[:10]}"
        )
