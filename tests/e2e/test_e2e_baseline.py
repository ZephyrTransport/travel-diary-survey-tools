"""Fail when the pipeline's numbers move, and say which ones.

The rest of the e2e suite asks whether the pipeline runs and whether particular
behaviours hold. This asks the question those cannot: did anything change? A
green suite is not evidence that output is unchanged, and treating it as such is
how a batch of PRs altered production results without a single test noticing.

The check is bit-exact by design. Any drift fails, including drift that is
correct and wanted -- that is the point. A wanted change is *promoted* by
regenerating the baseline (see the failure message), which puts the movement in
the PR diff where a reviewer can weigh it.

See [`baseline`][tests.e2e.baseline] for what is stored: hashes and aggregates,
never record-level values.
"""

import pytest

from . import baseline

PROFILE = "full"


@pytest.fixture(scope="module")
def current(full_result) -> dict:
    """Fingerprint of this run's canonical tables."""
    tables = (
        full_result.as_dict_non_null()
        if hasattr(full_result, "as_dict_non_null")
        else dict(full_result)
    )
    return baseline.fingerprint(tables)


class TestOutputIsUnchanged:
    """One assertion, over every table the pipeline produces."""

    def test_matches_the_committed_baseline(self, current):
        """Any difference in any column of any table fails, and is named."""
        if baseline.update_requested():
            path = baseline.save(PROFILE, current)
            pytest.skip(f"baseline rewritten: {path}")

        committed = baseline.load(PROFILE)
        if committed is None:
            path = baseline.save(PROFILE, current)
            pytest.skip(f"no baseline yet; wrote {path}. Commit it and re-run.")

        differences = baseline.diff(committed, current)
        assert not differences, baseline.report(PROFILE, differences)


class TestTheBaselineWouldCatchAChange:
    """A guard that cannot fail is worse than none -- it reads as coverage.

    These perturb a fingerprint rather than the pipeline, so they stay fast and
    do not depend on any particular defect being reachable.
    """

    def test_a_changed_value_is_caught(self, current):
        """The case the hash exists for: same shape, different numbers."""
        tampered = _deepish_copy(current)
        table, col = _any_column(tampered)
        tampered[table]["columns"][col]["hash"] = "0" * 16

        assert any(f"{table}.{col}" in line for line in baseline.diff(current, tampered))

    def test_a_changed_row_count_is_caught(self, current):
        tampered = _deepish_copy(current)
        table = next(iter(tampered))
        tampered[table]["rows"] += 1

        assert any("rows" in line for line in baseline.diff(current, tampered))

    def test_a_dropped_column_is_caught(self, current):
        tampered = _deepish_copy(current)
        table, col = _any_column(tampered)
        del tampered[table]["columns"][col]

        assert any("columns dropped" in line for line in baseline.diff(current, tampered))

    def test_an_identical_fingerprint_reports_nothing(self, current):
        """No false positives, or the gate gets disabled within a week."""
        assert baseline.diff(current, _deepish_copy(current)) == []


class TestTheFingerprintStoresNoRecords:
    """The repository must not gain data, even synthetic data.

    A fingerprint travels into git, so it carries hashes and aggregates only.
    Identifier and coordinate columns contribute no histogram at all, and no
    column contributes its values.
    """

    def test_identifier_columns_have_no_histogram(self, current):
        offenders = [
            f"{t}.{c}"
            for t, spec in current.items()
            for c, col in spec["columns"].items()
            if c.endswith("_id") and "counts" in col
        ]

        assert offenders == []

    def test_coordinate_columns_have_no_histogram(self, current):
        offenders = [
            f"{t}.{c}"
            for t, spec in current.items()
            for c, col in spec["columns"].items()
            if ("lat" in c or "lon" in c) and "counts" in col
        ]

        assert offenders == []

    def test_histograms_are_bounded_and_aggregate(self, current):
        """A histogram over as many categories as rows would be the data itself."""
        for table, spec in current.items():
            for col, fingerprint in spec["columns"].items():
                counts = fingerprint.get("counts")
                if counts is None:
                    continue
                assert len(counts) <= baseline._MAX_HISTOGRAM_CARDINALITY, f"{table}.{col}"
                assert all(isinstance(v, int) for v in counts.values()), f"{table}.{col}"

    def test_every_table_is_sorted_by_a_unique_key(self, current):
        """Ties in a non-unique sort order would make the hash report false drift."""
        unsorted = [t for t, spec in current.items() if not spec["sorted_by"]]

        assert unsorted == [], f"no primary key registered for: {unsorted}"


def _deepish_copy(fp: dict) -> dict:
    return {
        t: {
            "rows": s["rows"],
            "sorted_by": s["sorted_by"],
            "columns": {c: dict(col) for c, col in s["columns"].items()},
        }
        for t, s in fp.items()
    }


def _any_column(fp: dict) -> tuple[str, str]:
    table = next(t for t, s in fp.items() if s["columns"])
    return table, next(iter(fp[table]["columns"]))
