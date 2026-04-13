"""Category merge operations for balancing controls.

Handles collapsing control categories to reduce matrix size and simplify
sparse cross-tabs. Supports both 1D merges (simple category lists) and
N-D merges (cartesian products for cross-tabulated controls).
"""

import logging
from itertools import product

import numpy as np

from processing.weighting.specs import MergeSpec

logger = logging.getLogger(__name__)


def apply_category_merges(
    incidence: np.ndarray,
    targets: np.ndarray,
    row_labels: list[tuple[str, str]],
    master_idx: int,
    merges: list[MergeSpec],
    importance: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, int, np.ndarray]:
    """Collapse incidence rows + target entries for merged categories.

    For each ``MergeSpec``, every group maps several base member names to
    a single merged row (1D merge) or maps a cartesian product of dimension
    members to a single merged row (N-D merge for cross-tabs).

    The incidence rows are summed element-wise and the target values are
    summed. Original rows are replaced by the single merged row.

    Parameters
    ----------
    incidence : np.ndarray
        Shape ``(n_controls, n_households)``.
    targets : np.ndarray
        One entry per row.
    row_labels : list[tuple[str, str]]
        ``(control_name, member_name)`` per row.  **Modified in place**
        to reflect the merged labels.
    master_idx : int
        Row index of the total-HH master control.
    merges : list[MergeSpec]
        Merge specifications to apply.
    importance : np.ndarray
        Importance weights per row.  Merged rows keep the first row's value.

    Returns:
    --------
    incidence, targets, master_idx, importance
        Reduced arrays and (possibly shifted) master index.
    """
    for spec in merges:
        for merged_label, base_members in spec.groups.items():
            # Check if this is a 1D merge (list) or N-D merge (dict)
            if isinstance(base_members, dict):
                # N-D merge for cross-tabs: cartesian product of dimension members
                # Need to know dimension order, so look up the control
                base_member_names = expand_crosstab_dimensions(
                    spec.control, base_members, row_labels
                )
            else:
                # 1D merge: direct list of member names
                base_member_names = base_members

            # Validate every named member exists as a row label for this control
            known = {member for ctrl, member in row_labels if ctrl == spec.control}
            missing = [m for m in base_member_names if m not in known]
            if missing:
                # Check if this looks like a cross-tab control (has composite names)
                sample_known = next(iter(known), "")
                has_composite_names = "_" in sample_known and len(sample_known.split("_")) > 2  # noqa: PLR2004

                msg = (
                    f"Merge group '{merged_label}' for control '{spec.control}' "
                    f"references unknown categories: {missing}. "
                    f"Available: {sorted(known)}"
                )

                if has_composite_names and isinstance(base_members, list):
                    msg += (
                        f"\n\nNote: '{spec.control}' appears to be a cross-tab control with "
                        f"composite category names. For cross-tabs, you must either:\n"
                        f"1. Use full composite names (e.g., 'size_3_income_under25')\n"
                        f"2. Use N-D merge syntax with dimension dict, e.g.:\n"
                        f"     merges:\n"
                        f"       {spec.control}:\n"
                        f"         {merged_label}:\n"
                        f"           dimension1: [cat1, cat2]\n"
                        f"           dimension2: [catA, catB]"
                    )

                raise ValueError(msg)

            # Find row indices matching this control + base members
            idxs = [
                i
                for i, (ctrl, member) in enumerate(row_labels)
                if ctrl == spec.control and member in base_member_names
            ]
            if len(idxs) < 2:  # noqa: PLR2004
                continue  # single member — nothing to merge

            # Sum incidence rows and target entries
            merged_row = incidence[idxs].sum(axis=0)
            merged_target = targets[idxs].sum()

            # Keep the first index, mark rest for removal
            keep = idxs[0]
            remove = set(idxs[1:])

            incidence[keep] = merged_row
            targets[keep] = merged_target
            row_labels[keep] = (spec.control, merged_label)

            # Remove collapsed rows (reverse order to preserve indices)
            keep_mask = [i not in remove for i in range(len(row_labels))]
            incidence = incidence[keep_mask]
            targets = targets[keep_mask]
            importance = importance[keep_mask]
            row_labels[:] = [lbl for i, lbl in enumerate(row_labels) if i not in remove]

            # Recompute master_idx after removal
            master_idx = next(i for i, (c, _) in enumerate(row_labels) if c == "h_total")

    return incidence, targets, master_idx, importance


def expand_crosstab_dimensions(
    control_name: str,
    dimensions: dict[str, list[str]],
    row_labels: list[tuple[str, str]],
) -> list[str]:
    """Expand N-D merge dimensions to composite enum member names.

    Looks up the cross-tab control to determine the correct dimension order,
    then builds composite member names matching the enum's naming convention.

    Parameters
    ----------
    control_name : str
        Name of the cross-tab control (for dimension order lookup).
    dimensions : dict[str, list[str]]
        Dimension control name -> list of member names to include.
        E.g. ``{"h_income": ["income_under25", "income_25to50"],
               "h_size": ["size_1", "size_2"]}``
    row_labels : list[tuple[str, str]]
        Current row labels (control_name, member_name) - used to extract
        dimension order from actual cross-tab structure.

    Returns:
    --------
    list[str]
        Composite member names from cartesian product in correct dimension order.
        E.g. for h_size_by_income (dimensions: h_size, h_income):
        ``["size_1_income_under25", "size_1_income_25to50",
           "size_2_income_under25", "size_2_income_25to50"]``

    Examples:
    ---------
    >>> expand_crosstab_dimensions(
    ...     "h_size_by_income",
    ...     {"h_size": ["size_1", "size_2"], "h_income": ["income_under25"]},
    ...     [("h_size_by_income", "size_1_income_under25"), ...]
    ... )
    ['size_1_income_under25', 'size_2_income_under25']
    """
    # Infer dimension order from actual cross-tab member names
    # Get any member name for this control
    sample_member = next((member for ctrl, member in row_labels if ctrl == control_name), None)
    if sample_member is None:
        msg = f"No rows found for control '{control_name}'"
        raise ValueError(msg)

    # Parse dimension order from composite member name
    # E.g., "size_1_income_under25" -> dims are ordered [size..., income...]
    # Strategy: check which dimension prefixes appear first in the composite name
    dim_names = list(dimensions.keys())
    if len(dim_names) != 2:  # noqa: PLR2004
        # For now, only support 2D cross-tabs - can extend later
        msg = (
            f"N-D merges currently only support 2-dimensional cross-tabs. "
            f"Control '{control_name}' has {len(dim_names)} dimensions."
        )
        raise ValueError(msg)

    # Check order by seeing which dimension's members appear first in the sample
    # Assume dimension control names roughly match the prefix pattern
    # (h_size -> size, h_income -> income)
    dim1_key = dim_names[0].replace("h_", "").replace("p_", "")
    dim2_key = dim_names[1].replace("h_", "").replace("p_", "")

    # Find which key appears first in sample member name
    sample_lower = sample_member.lower()
    pos1 = sample_lower.find(dim1_key)
    pos2 = sample_lower.find(dim2_key)

    ordered_dims = [dim_names[0], dim_names[1]] if pos1 < pos2 else [dim_names[1], dim_names[0]]

    # Build cartesian product in correct dimension order
    dim_members = [dimensions[dim] for dim in ordered_dims]
    composite_names = []
    for member_combo in product(*dim_members):
        composite_name = "_".join(member_combo).lower()
        composite_names.append(composite_name)

    return composite_names
