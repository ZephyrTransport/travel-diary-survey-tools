"""Data model for the weighting's own working columns.

The balancer needs a geography to balance within and a seed weight to start
from. Those are inputs to a procedure, not facts the survey collected, so they
live in their own table rather than riding along on every delivered household
record: a delivery is not obliged to explain the estimator that produced its
weights, and a run with no weighting step should not owe columns it never
computed.

The household weight remains the deliverable, on ``households``. This table is
the audit trail behind it -- populated only when the weighting runs, and only
written when a config names it, so it costs nothing otherwise.

Its weight columns are not declared below: they are named per fitted profile, so
the weighting registers them alongside the ones it writes to ``households``. Only
the geography is fixed, because where a household is does not depend on which
universe was weighted.
"""

from pydantic import BaseModel

from data_canon.core.schema_field import schema_field


class HouseholdWeightingModel(BaseModel):
    """Weighting geography and seed weight for one household."""

    # Not required_child: households exist whether or not weighting ran.
    hh_id: int = schema_field(ge=1, unique=True, fk_to="households.hh_id")
    target_zone_id: str | None = schema_field(
        default=None,
        description="Target zone the home is in, from the target-zone file (e.g. county FIPS).",
    )
    zone_group_id: str | None = schema_field(
        default=None,
        description=(
            "Zone group the target zone belongs to: the balancer fits each group. "
            "Several target zones share one group where sample is too thin to "
            "balance them separately."
        ),
    )
    home_puma: str | None = schema_field(
        default=None,
        description=(
            "PUMA containing the home location, from the PUMA boundaries the "
            "PUMS targets are allocated by."
        ),
    )
    bg_geo_id: str | None = schema_field(
        default=None,
        description="Census block group GEOID containing the home location.",
    )
