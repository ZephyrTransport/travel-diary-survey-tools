"""Control registry and resolution helpers.

The ``CONTROLS`` dict is the single lookup table mapping control names
to :class:`ControlTarget` instances.  ``resolve_targets`` and
``pums_variables`` provide the main query API used by the rest of the
weighting pipeline.

Dynamic cross-tab creation:
``register_crosstab`` creates a CrosstabControlTarget instance at runtime
from dimension control names, allowing cross-tabs to be defined in YAML
config without requiring Python class definitions.
``register_puma_control`` does the same for ``h_puma``, whose categories are the
crosswalk's PUMAs.
"""

import logging
from typing import TYPE_CHECKING

from processing.weighting.controls.base import ControlLevel, ControlTarget, CrosstabControlTarget
from processing.weighting.controls.enums import make_crosstab_enum
from processing.weighting.controls.household import (
    HHChildrenControl,
    HHIncomeControl,
    HHPumaControl,
    HHSizeControl,
    HHTargetZoneControl,
    HHTotalControl,
    HHVehiclesControl,
    HHVehiclesPerDriverControl,
    HHWorkersControl,
)
from processing.weighting.controls.person import (
    AgeControl,
    CommuteModeControl,
    EducationControl,
    EmploymentControl,
    GenderControl,
    PersonTotalControl,
    RaceEthnicityControl,
    StudentControl,
)

if TYPE_CHECKING:
    from processing.weighting.core.specs import ControlSpec

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════
# Registry
# ══════════════════════════════════════════════════════════════════════════

CONTROLS: dict[str, ControlTarget] = {
    t.name: t
    for t in [
        HHTotalControl(),
        HHSizeControl(),
        HHIncomeControl(),
        HHWorkersControl(),
        HHVehiclesControl(),
        HHVehiclesPerDriverControl(),
        HHChildrenControl(),
        PersonTotalControl(),
        GenderControl(),
        EmploymentControl(),
        CommuteModeControl(),
        StudentControl(),
        EducationControl(),
        RaceEthnicityControl(),
        AgeControl(),
    ]
}


def resolve_targets(
    targets: list[str],
    level: ControlLevel | None = None,
) -> list[ControlTarget]:
    """Return ``ControlTarget`` objects for *targets*, optionally filtered."""
    bad = [t for t in targets if t not in CONTROLS]
    if bad:
        msg = f"Unknown targets: {bad}. Valid: {sorted(CONTROLS)}"
        raise ValueError(msg)
    ctrls = [CONTROLS[t] for t in targets]
    if level is not None:
        ctrls = [c for c in ctrls if c.level == level]
    return ctrls


def pums_variables(level: ControlLevel) -> set[str]:
    """PUMS variable names needed for all controls at *level*."""
    return {f for ctrl in CONTROLS.values() if ctrl.level == level for f in ctrl.pums_fields}


def register_puma_control(puma_ids: list[str] | None) -> None:
    """Register ``h_puma`` for this run's PUMAs, or remove it when ``None``.

    Always replaces: a second run in one process may have another geography, and
    keeping the first run's PUMAs would recode its households against the wrong list.
    """
    CONTROLS.pop(HHPumaControl.name, None)
    if puma_ids is not None:
        CONTROLS[HHPumaControl.name] = HHPumaControl(puma_ids)


def register_target_zone_control(zone_ids: list[str] | None) -> None:
    """Register ``h_target_zone`` for this run's target zones, or remove it when ``None``.

    Always replaces, for the same reason as `register_puma_control`.
    """
    CONTROLS.pop(HHTargetZoneControl.name, None)
    if zone_ids is not None:
        CONTROLS[HHTargetZoneControl.name] = HHTargetZoneControl(zone_ids)


def register_crosstab(
    name: str,
    dimension_names: list[str],
    merges: dict[str, dict[str, list[str]]] | None = None,
) -> ControlTarget:
    """Dynamically create and register a crosstab control.

    Merges are applied at registration time so the enum and expression
    reflect the effective (post-merge) cell count.

    Parameters
    ----------
    name : str
        Name for the cross-tab control (will be registered in CONTROLS).
    dimension_names : list[str]
        Names of dimension controls to cross-tabulate (must exist in CONTROLS).
    merges : dict | None
        Optional per-dimension merge specs.  Keys are dimension control
        names, values are ``{merged_label: [source_member_names]}``.
        Source names must match the dimension enum member names
        (case-insensitive).  Unmentioned members are kept as-is.

    Returns:
    --------
    ControlTarget
        The newly created and registered CrosstabControlTarget instance.

    Examples:
    ---------
    >>> xtab = register_crosstab(
    ...     "h_size_by_income",
    ...     ["h_size", "h_income"],
    ...     merges={
    ...         "h_size": {"size_3_plus": ["size_3", "size_4", "size_5"]},
    ...         "h_income": {"income_under_100": ["income_under25", "income_25to50"]},
    ...     },
    ... )
    """
    if name in CONTROLS:
        msg = f"Control '{name}' already exists in registry"
        raise ValueError(msg)

    bad_dims = [d for d in dimension_names if d not in CONTROLS]
    if bad_dims:
        msg = f"Unknown dimension controls: {bad_dims}. Valid: {sorted(CONTROLS)}"
        raise ValueError(msg)

    dim_controls = tuple(CONTROLS[d] for d in dimension_names)

    levels = {ctrl.level for ctrl in dim_controls}
    if len(levels) > 1:
        msg = f"Cross-tab dimensions must be at the same level. Got: {levels}"
        raise ValueError(msg)

    level = dim_controls[0].level

    # -- Build effective value groups per dimension -------------------------
    dim_value_groups = _build_dim_value_groups(dim_controls, merges or {})

    # -- Enum and metadata --------------------------------------------------
    enum_name = "".join(d.title().replace("_", "") for d in dimension_names) + "Category"
    composite_enum = make_crosstab_enum(enum_name, dim_value_groups)

    dim_desc = " x ".join(ctrl.description for ctrl in dim_controls)
    description = f"{dim_desc} (cross-tab)"

    xtab_class = type(
        f"{name.title().replace('_', '')}Control",
        (CrosstabControlTarget,),
        {
            "name": name,
            "level": level,
            "description": description,
            "dim_controls": dim_controls,
            "dim_value_groups": dim_value_groups,
            "categories": composite_enum,
            "survey_fields": (),
            "pums_fields": (),
        },
    )

    instance = xtab_class()
    CONTROLS[name] = instance

    return instance


def _build_dim_value_groups(
    dim_controls: tuple[ControlTarget, ...],
    merges: dict[str, dict[str, list[str]]],
) -> list[list[tuple[str, list[int]]]]:
    """Compute effective (name, values) groups per dimension after merges.

    For each dimension, start with the original non-sentinel members.
    If a merge spec exists for that dimension, replace the source members
    with a single merged group.  Unmentioned members pass through as-is.
    """
    all_groups: list[list[tuple[str, list[int]]]] = []

    for ctrl in dim_controls:
        original = ctrl.valid_members  # [(value, name), ...]
        dim_merges = merges.get(ctrl.name, {})

        if not dim_merges:
            # No merges — each original member is its own group
            all_groups.append([(name, [val]) for val, name in original])
            continue

        # Build name→value lookup (case-insensitive)
        name_to_val = {name.lower(): val for val, name in original}

        # Track which original members are consumed by merges
        consumed: set[str] = set()
        merged_groups: list[tuple[str, list[int]]] = []

        for merged_label, source_names in dim_merges.items():
            source_lower = [s.lower() for s in source_names]
            bad = [s for s in source_lower if s not in name_to_val]
            if bad:
                msg = (
                    f"Merge '{merged_label}' for dimension '{ctrl.name}' references "
                    f"unknown members: {bad}. Available: {sorted(name_to_val)}"
                )
                raise ValueError(msg)
            vals = [name_to_val[s] for s in source_lower]
            consumed.update(source_lower)
            merged_groups.append((merged_label.upper(), vals))

        # Keep unmerged originals in their original order
        kept = [(name, [val]) for val, name in original if name.lower() not in consumed]
        all_groups.append(kept + merged_groups)

    return all_groups


def register_crosstabs_from_config(specs: "list[ControlSpec]") -> None:
    """Register the cross-tab controls the config defines.

    Each spec with ``dimensions`` becomes a CrosstabControlTarget.  Its
    ``merges``, if any, are applied at registration time so the enum reflects
    the effective cell count.
    """
    for spec in specs:
        if spec.dimensions is None:
            continue
        if spec.name in CONTROLS:
            logger.debug("Cross-tab '%s' already registered, skipping dynamic creation", spec.name)
            continue
        logger.info(
            "Registering dynamic cross-tab: %s (dimensions: %s)", spec.name, spec.dimensions
        )
        register_crosstab(spec.name, spec.dimensions, merges=spec.merges)
