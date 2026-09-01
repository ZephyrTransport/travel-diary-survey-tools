"""Fingerprint the pipeline's output so a change in *results* cannot pass silently.

The e2e suite asserts that the pipeline runs and that specific behaviours hold.
Neither answers the question a reviewer actually has about a data change: did any
number move, and if so which. Six PRs landed on a green suite while altering real
output, and nothing said so -- the shift only surfaced when the pipeline was run
by hand against production data weeks later.

So each run is reduced to a fingerprint and compared against a committed
baseline. A mismatch fails loudly and names the table and column. An intended
change is then *promoted*: regenerate the baseline, and the diff of this file
becomes the reviewable record of what the PR moved.

**No record-level values are stored.** A column contributes a hash, and
aggregates over it -- null count, distinct count, numeric range and sum, and for
low-cardinality non-identifier columns a value-count histogram. The hash is what
detects a change; the aggregates are what make the failure diagnosable without
putting data in the repository. That rule holds even though this fixture is
synthetic, because the same harness should be safe to point at real output.
"""

import hashlib
import json
import os
from pathlib import Path
from typing import Any

import polars as pl

BASELINE_DIR = Path(__file__).parent / "baselines"

#: Env var that rewrites the baseline instead of asserting against it.
UPDATE_ENV = "E2E_BASELINE_UPDATE"

#: Each table's own unique key. Sorting by a *non-unique* column leaves ties in
#: arbitrary order, and a hash over ordered values then reports a difference
#: where the rows are the same set.
PRIMARY_KEYS = {
    "households": ("hh_id",),
    "persons": ("person_id",),
    "days": ("day_id",),
    "unlinked_trips": ("unlinked_trip_id",),
    "linked_trips": ("linked_trip_id",),
    "joint_trips": ("joint_trip_id",),
    "tours": ("tour_id",),
    "joint_tours": ("joint_tour_id",),
    "habitual_locations": ("habitual_location_id",),
    "habitual_location_days": ("habitual_location_id", "day_id"),
}

# A histogram is only kept for columns that classify rather than identify, so a
# fingerprint can never reconstruct a record.
_MAX_HISTOGRAM_CARDINALITY = 15


def _is_classifier(name: str, series: pl.Series) -> bool:
    """Whether a value-count histogram is safe and useful for this column."""
    if name.endswith("_id") or "lat" in name or "lon" in name:
        return False
    if series.dtype == pl.Boolean:
        return True
    if not series.dtype.is_integer():
        return False
    return series.n_unique() <= _MAX_HISTOGRAM_CARDINALITY


def _column_fingerprint(name: str, series: pl.Series) -> dict[str, Any]:
    """Hash plus aggregates for one column. Never individual values."""
    out: dict[str, Any] = {
        "dtype": str(series.dtype),
        "hash": hashlib.sha256(str(series.to_list()).encode()).hexdigest()[:16],
        "nulls": series.null_count(),
        "distinct": series.n_unique(),
    }
    if series.dtype.is_numeric():
        non_null = series.drop_nulls()
        if non_null.len():
            out["min"] = round(float(non_null.min()), 6)
            out["max"] = round(float(non_null.max()), 6)
            out["sum"] = round(float(non_null.sum()), 6)
    if _is_classifier(name, series):
        counts = series.value_counts(sort=True)
        out["counts"] = {str(r[name]): r["count"] for r in counts.to_dicts()}
    return out


def fingerprint(tables: dict[str, pl.DataFrame]) -> dict[str, Any]:
    """Reduce every table to row count, column list, and per-column fingerprints."""
    out: dict[str, Any] = {}
    for table, df in sorted(tables.items()):
        if not isinstance(df, pl.DataFrame):
            continue
        keys = [k for k in PRIMARY_KEYS.get(table, ()) if k in df.columns]
        ordered = df.sort(keys) if keys else df
        out[table] = {
            "rows": ordered.height,
            "sorted_by": keys or None,
            "columns": {
                name: _column_fingerprint(name, ordered[name]) for name in sorted(ordered.columns)
            },
        }
    return out


def baseline_path(profile: str) -> Path:
    return BASELINE_DIR / f"{profile}.json"


def load(profile: str) -> dict[str, Any] | None:
    path = baseline_path(profile)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def save(profile: str, data: dict[str, Any]) -> Path:
    path = baseline_path(profile)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    return path


def update_requested() -> bool:
    return os.environ.get(UPDATE_ENV, "").strip().lower() in {"1", "true", "yes"}


def diff(old: dict[str, Any], new: dict[str, Any]) -> list[str]:
    """Human-readable differences, most structural first. Empty when identical."""
    lines: list[str] = []
    for table in sorted(set(old) | set(new)):
        if table not in old:
            lines.append(f"{table}: NEW table ({new[table]['rows']} rows)")
            continue
        if table not in new:
            lines.append(f"{table}: table GONE (was {old[table]['rows']} rows)")
            continue
        before, after = old[table], new[table]
        if before["rows"] != after["rows"]:
            lines.append(f"{table}: rows {before['rows']:,} -> {after['rows']:,}")
        added = sorted(set(after["columns"]) - set(before["columns"]))
        dropped = sorted(set(before["columns"]) - set(after["columns"]))
        if added:
            lines.append(f"{table}: columns added: {', '.join(added)}")
        if dropped:
            lines.append(f"{table}: columns dropped: {', '.join(dropped)}")
        for col in sorted(set(before["columns"]) & set(after["columns"])):
            b, a = before["columns"][col], after["columns"][col]
            if b["hash"] == a["hash"]:
                continue
            moved = [
                f"{field} {b.get(field)!r} -> {a.get(field)!r}"
                for field in ("dtype", "nulls", "distinct", "min", "max", "sum", "counts")
                if b.get(field) != a.get(field)
            ]
            detail = "; ".join(moved) if moved else "same aggregates, different values"
            lines.append(f"{table}.{col}: {detail}")
    return lines


def report(profile: str, lines: list[str]) -> str:
    """The failure message: what moved, and how to accept it."""
    head = (
        f"Pipeline output changed against tests/e2e/baselines/{profile}.json.\n"
        f"{len(lines)} difference(s):\n\n"
    )
    body = "\n".join(f"  {line}" for line in lines)
    tail = (
        "\n\nIf this change is intended, promote it -- regenerate the baseline and "
        f"commit the diff so the review shows what moved:\n\n"
        f"    {UPDATE_ENV}=1 uv run pytest tests/e2e/test_e2e_baseline.py\n\n"
        "If it is not intended, this is a regression: the pipeline now produces "
        "different numbers than the committed baseline."
    )
    return head + body + tail
