"""Data model for the weighting's own working columns.

The balancer needs a geography to balance within and a seed weight to start
from. Those are inputs to a procedure, not facts the survey collected, so they
live in their own table rather than riding along on every delivered household
record: a delivery is not obliged to explain the estimator that produced its
weights, and a run with no weighting step should not owe columns it never
computed.

``households.hh_weight`` remains the deliverable. This table is the audit trail
behind it -- it is populated only when the weighting runs, and only written
when a config names it, so it costs nothing otherwise.
"""

from pydantic import BaseModel

from data_canon.core.schema_field import schema_field


class HouseholdWeightingModel(BaseModel):
    """Weighting geography and seed weight for one household."""

    # Not required_child: households exist whether or not weighting ran.
    hh_id: int = schema_field(ge=1, unique=True, fk_to="households.hh_id")
    study_geoid: str | None = schema_field(
        default=None,
        description="Geography the household is assigned to for weighting (e.g. county FIPS).",
    )
    ctrl_geoid: str | None = schema_field(
        default=None,
        description=(
            "The control geography `study_geoid` maps into. Several study "
            "geographies can share one control zone where sample is too thin to "
            "balance them separately."
        ),
    )
    bg_geo_id: str | None = schema_field(
        default=None,
        description="Census block group GEOID containing the home location.",
    )
    base_weight: float | None = schema_field(
        ge=0,
        default=None,
        description=(
            "Pre-balancing seed weight, before the balancer fits it to the "
            "controls. Null for households the weighting could not seed."
        ),
    )
    hh_weight: float | None = schema_field(
        ge=0,
        default=None,
        description=(
            "Final balanced weight, repeated from `households` so the expansion "
            "factor `hh_weight / base_weight` can be read off this table alone."
        ),
    )
