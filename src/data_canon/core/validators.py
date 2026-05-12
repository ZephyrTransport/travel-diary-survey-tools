"""Re-exports from :mod:`pipeline.step_registry` for backward compatibility.

All step-aware validation metadata now lives in the step registry.
Prefer importing directly from ``pipeline.step_registry``.
"""

from pipeline.step_registry import (  # noqa: F401
    get_all_required_fields,
    get_step_validation_summary,
)
