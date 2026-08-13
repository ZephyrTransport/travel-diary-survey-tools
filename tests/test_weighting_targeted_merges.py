"""Tests for zone-specific merges layered on top of global merges."""

import numpy as np
import polars as pl
import pytest

from processing.weighting.balancing.merges import apply_category_merges
from processing.weighting.core.specs import ControlRegistryConfig, MergeSpec
from processing.weighting.diagnostics.data import apply_fit_merges


# ---------------------------------------------------------------------------
# ControlRegistryConfig.from_yaml — zone_merges config key
# ---------------------------------------------------------------------------
class TestParseControlsZoneMerges:
    """ControlRegistryConfig.from_yaml should emit MergeSpecs for zone_merges entries."""

    def test_no_zone_merges(self):
        """Controls without zone_merges produce only global MergeSpecs."""
        controls = [
            {
                "name": "h_size",
                "merge": {"size_5_plus": ["size_5", "size_6"]},
            },
        ]
        cfg = ControlRegistryConfig.from_yaml(controls)
        assert len(cfg.merges_1d) == 1
        assert cfg.merges_1d[0].zones is None

    def test_zone_merges_appended(self):
        """zone_merges produce zone-specific MergeSpecs after the global one."""
        controls = [
            {
                "name": "h_size",
                "merge": {"size_5_plus": ["size_5", "size_6"]},
                "zone_merges": {
                    "Z1": {"size_4_plus": ["size_4", "size_5_plus"]},
                },
            },
        ]
        cfg = ControlRegistryConfig.from_yaml(controls)
        merges = cfg.merges_1d
        assert len(merges) == 2
        # Global first
        assert merges[0].zones is None
        assert "size_5_plus" in merges[0].groups
        # Zone-specific second
        assert merges[1].zones == ["Z1"]
        assert "size_4_plus" in merges[1].groups

    def test_multiple_zone_merges(self):
        """Multiple zone keys each produce their own MergeSpec."""
        controls = [
            {
                "name": "h_size",
                "merge": {"size_5_plus": ["size_5", "size_6"]},
                "zone_merges": {
                    "Z1": {"size_4_plus": ["size_4", "size_5_plus"]},
                    "Z2": {"size_3_plus": ["size_3", "size_5_plus"]},
                },
            },
        ]
        cfg = ControlRegistryConfig.from_yaml(controls)
        merges = cfg.merges_1d
        assert len(merges) == 3
        zone_map = {m.zones[0]: m for m in merges if m.zones is not None}
        assert "size_4_plus" in zone_map["Z1"].groups
        assert "size_3_plus" in zone_map["Z2"].groups

    def test_zone_merges_without_global_merge(self):
        """A control with only zone_merges (no global merge) still works."""
        controls = [
            {
                "name": "h_size",
                "zone_merges": {
                    "Z1": {"size_5_plus": ["size_5", "size_6"]},
                },
            },
        ]
        cfg = ControlRegistryConfig.from_yaml(controls)
        merges = cfg.merges_1d
        assert len(merges) == 1
        assert merges[0].zones == ["Z1"]
        assert merges[0].control == "h_size"


# ---------------------------------------------------------------------------
# apply_category_merges — targeted merge layered on global merge
# ---------------------------------------------------------------------------
def _make_arrays(labels, targets_list):
    """Helper: build incidence, targets, importance arrays from row labels."""
    n_hh = 3
    incidence = np.ones((len(labels), n_hh), dtype=np.float64)
    # Give each row distinct values so we can verify summation
    for i in range(len(labels)):
        incidence[i] = np.array([i + 1, i + 2, i + 3], dtype=np.float64)
    targets = np.array(targets_list, dtype=np.float64)
    importance = np.ones(len(labels), dtype=np.float64)
    master_idx = next(i for i, (c, _) in enumerate(labels) if c == "h_total")
    return incidence, targets, master_idx, importance


class TestApplyMergesTargeted:
    """Targeted merges should be able to reference labels produced by global merges."""

    def test_global_then_targeted_merge(self):
        """A targeted merge can reference a label created by a preceding global merge."""
        # Row labels: h_total + h_size with 4 categories
        labels = [
            ("h_total", "total"),
            ("h_size", "size_3"),
            ("h_size", "size_4"),
            ("h_size", "size_5"),
            ("h_size", "size_6"),
        ]
        targets = [100.0, 30.0, 25.0, 25.0, 20.0]
        incidence, tgt, master_idx, imp = _make_arrays(labels, targets)

        # Global: merge 5+6 → size_5_plus
        global_merge = MergeSpec(
            control="h_size",
            groups={"size_5_plus": ["size_5", "size_6"]},
            zones=None,
        )
        # Targeted: further merge 4 + size_5_plus → size_4_plus (zone Z1 only)
        targeted_merge = MergeSpec(
            control="h_size",
            groups={"size_4_plus": ["size_4", "size_5_plus"]},
            zones=["Z1"],
        )

        # Apply both in order (global first)
        incidence, tgt, master_idx, imp = apply_category_merges(
            incidence, tgt, labels, master_idx, [global_merge, targeted_merge], imp
        )

        # After global: size_5,size_6→size_5_plus  (target=25+20=45)
        # After targeted: size_4+size_5_plus→size_4_plus  (target=25+45=70)
        remaining_members = {member for _, member in labels}
        assert "size_5" not in remaining_members
        assert "size_6" not in remaining_members
        assert "size_4" not in remaining_members
        assert "size_5_plus" not in remaining_members
        assert "size_4_plus" in remaining_members
        assert "size_3" in remaining_members

        # Find the merged target
        idx_4_plus = next(i for i, (c, m) in enumerate(labels) if m == "size_4_plus")
        assert tgt[idx_4_plus] == pytest.approx(70.0)

    def test_targeted_merge_unknown_label_raises(self):
        """Referencing a label that doesn't exist should raise ValueError."""
        labels = [
            ("h_total", "total"),
            ("h_size", "size_3"),
            ("h_size", "size_4"),
        ]
        targets = [100.0, 50.0, 50.0]
        incidence, tgt, master_idx, imp = _make_arrays(labels, targets)

        bad_merge = MergeSpec(
            control="h_size",
            groups={"merged": ["size_4", "nonexistent"]},
            zones=["Z1"],
        )

        with pytest.raises(ValueError, match="unknown categories"):
            apply_category_merges(incidence, tgt, labels, master_idx, [bad_merge], imp)


# ---------------------------------------------------------------------------
# apply_fit_merges — diagnostics table labelling
# ---------------------------------------------------------------------------


def _make_fit(zones: list[str], categories: list[str], control: str = "h_size") -> pl.DataFrame:
    """Build a minimal fit DataFrame with string categories for testing."""
    rows = []
    for z in zones:
        for cat in categories:
            t = float(hash(cat) % 50 + hash(z) % 7 + 10)
            w = t + 1.0  # small diff
            rows.append(
                {
                    "geo_id": z,
                    "control_name": control,
                    "category": cat,
                    "target_total": t,
                    "weighted_total": w,
                    "diff": w - t,
                    "diff_pct": (w - t) / t * 100 if t else 0.0,
                }
            )
    return pl.DataFrame(rows)


class TestApplyFitMergesLabel:
    """apply_fit_merges should add a label column from enum definitions."""

    def test_adds_label_column(self):
        """A label column should be added based on the control and category."""
        fit = _make_fit(["Z1"], ["size_1", "size_2", "size_3"])
        result = apply_fit_merges(fit, None, ["h_size"])
        assert "label" in result.columns
        labels = result["label"].to_list()
        assert "Size 1" in labels
        assert "Size 2" in labels

    def test_merged_category_gets_label(self):
        """A merged category string gets a title-cased label from the merge spec."""
        fit = _make_fit(["Z1"], ["size_3", "size_5_plus"])
        merge = MergeSpec(
            control="h_size",
            groups={"size_5_plus": ["size_5", "size_6"]},
            zones=None,
        )
        result = apply_fit_merges(fit, [merge], ["h_size"])
        labels = result["label"].to_list()
        assert "Size 5 Plus" in labels
        assert "Size 3" in labels


class TestApplyFitMergesZoneAware:
    """Zone merge labels should appear correctly in the fit table."""

    def test_zone_merge_labels_present(self):
        """Zone-specific merged categories get proper labels."""
        # Simulate a fit table where zone Z1 has the merged category
        # and zone Z2 has the originals (this is how the data arrives
        # after apply_zone_merges + merge_control_totals upstream).
        z1_data = _make_fit(["Z1"], ["size_3", "size_4_plus"])
        z2_data = _make_fit(["Z2"], ["size_3", "size_4", "size_5_plus"])
        fit = pl.concat([z1_data, z2_data])

        global_merge = MergeSpec(
            control="h_size",
            groups={"size_5_plus": ["size_5", "size_6"]},
            zones=None,
        )
        zone_merge = MergeSpec(
            control="h_size",
            groups={"size_4_plus": ["size_4", "size_5_plus"]},
            zones=["Z1"],
        )
        result = apply_fit_merges(fit, [global_merge, zone_merge], ["h_size"])

        # Z1 should have the merged label
        z1 = result.filter((pl.col("geo_id") == "Z1") & pl.col("target_total").is_not_null())
        z1_labels = z1["label"].to_list()
        assert "Size 4 Plus" in z1_labels
        assert "Size 3" in z1_labels

        # Z2 should have the original labels
        z2 = result.filter((pl.col("geo_id") == "Z2") & pl.col("target_total").is_not_null())
        z2_labels = z2["label"].to_list()
        assert "Size 4" in z2_labels
        assert "Size 5 Plus" in z2_labels

    def test_null_placeholders_for_consistency(self):
        """Every zone should have every label (real or null placeholder)."""
        # Simulate post-merge data: Z1 has merged, Z2 has originals
        z1_data = _make_fit(["Z1"], ["size_3", "size_4_plus"])
        z2_data = _make_fit(["Z2"], ["size_3", "size_4", "size_5_plus"])
        fit = pl.concat([z1_data, z2_data])

        global_merge = MergeSpec(
            control="h_size",
            groups={"size_5_plus": ["size_5", "size_6"]},
            zones=None,
        )
        zone_merge = MergeSpec(
            control="h_size",
            groups={"size_4_plus": ["size_4", "size_5_plus"]},
            zones=["Z1"],
        )
        result = apply_fit_merges(fit, [global_merge, zone_merge], ["h_size"])

        all_labels = result["label"].unique().sort().to_list()
        for z in ["Z1", "Z2"]:
            zone_labels = result.filter(pl.col("geo_id") == z)["label"].unique().sort().to_list()
            assert zone_labels == all_labels, f"Zone {z} missing some labels"

    def test_zone_merge_target_preserved(self):
        """Merged category preserves its target total from upstream merge."""
        z1_data = _make_fit(["Z1"], ["size_4_plus"])
        z2_data = _make_fit(["Z2"], ["size_4", "size_5_plus"])
        fit = pl.concat([z1_data, z2_data])

        zone_merge = MergeSpec(
            control="h_size",
            groups={"size_4_plus": ["size_4", "size_5_plus"]},
            zones=["Z1"],
        )
        result = apply_fit_merges(fit, [zone_merge], ["h_size"])
        z1_merged = result.filter((pl.col("geo_id") == "Z1") & (pl.col("label") == "Size 4 Plus"))
        assert z1_merged.height == 1
        assert z1_merged["target_total"].item() is not None
