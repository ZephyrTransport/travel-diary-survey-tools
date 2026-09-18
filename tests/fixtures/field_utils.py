"""Utility functions for field resolution and handling in test data builders.

This module provides reusable utilities for:
- Resolving fields with fallback chains
- Adding optional fields conditionally

All functions expect raw values (not enums). Callers must use .value on enums.
"""


def resolve_field_with_fallback(fallback_chain: list[str], **kwargs):
    """Resolve a field value using a fallback chain.

    Tries each field name in the fallback chain until a non-None value is
    found.

    Args:
        fallback_chain: List of field names to try in order
        **kwargs: Keyword arguments containing field values (raw values, not
            enums)

    Returns:
        First non-None value found in fallback chain, or None if all are None

    Example:
        # Try o_purpose_category first, fall back to purpose_category
        value = resolve_field_with_fallback(
            ["o_purpose_category", "purpose_category"],
            o_purpose_category=None,
            purpose_category=PurposeCategory.HOME.value  # Must use .value
        )
        # Returns: 1 (the raw value)
    """
    for source_field in fallback_chain:
        if source_field in kwargs and kwargs[source_field] is not None:
            return kwargs[source_field]
    return None


def add_optional_fields_batch(record: dict, **fields) -> None:
    """Add fields to record only if they are not None.

    Modifies record in-place. Expects raw values, not enums.

    Args:
        record: Dictionary to add fields to (modified in-place)
        **fields: Keyword arguments of field_name=value pairs (raw values,
            not enums)

    Example:
        record = {"person_id": 101}
        add_optional_fields_batch(
            record,
            work_lat=37.75,
            work_lon=None,  # Not added
            work_taz=200
        )
        # record is now: {"person_id": 101, "work_lat": 37.75,
        # "work_taz": 200}
    """
    record.update({k: v for k, v in fields.items() if v is not None})


def ensure_fields_exist(record: dict, field_names: list[str], default_value=None) -> None:
    """Ensure specified fields exist in record, adding default if missing.

    Modifies record in-place. Useful for formatters that require certain
    fields to be present even if None.

    Args:
        record: Dictionary to check/modify (modified in-place)
        field_names: List of field names that must exist
        default_value: Value to use for missing fields (default: None)

    Example:
        record = {"person_id": 101}
        ensure_fields_exist(record, ["work_lat", "work_lon", "work_taz"])
        # record is now: {"person_id": 101, "work_lat": None,
        # "work_lon": None, "work_taz": None}
    """
    for field_name in field_names:
        if field_name not in record:
            record[field_name] = default_value
