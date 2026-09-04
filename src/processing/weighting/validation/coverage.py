"""How much of a profile's universe the control geography can place.

Two independent tests decide whether a household is "in the region", and they
are allowed to disagree:

* the **model** geography, assigned by ``add_zone_ids`` with a nearest-zone snap
  tolerance, which the usability profiles read through their zone axis;
* the **control** geography, a strict point-in-polygon of the home coordinate
  against the target-zone file, which is what gives a household a balancing zone.

A household that passes the first and fails the second is admitted by the
profile and cannot be weighted: it belongs to no zone, so no zone's controls
describe it. That is a bound on what the weighting can answer. This module
counts it, so the bound is stated rather than left to be inferred from a column
of nulls.
"""

import logging

import polars as pl

from processing.weighting.core.specs import GeographyCoverage

logger = logging.getLogger(__name__)


def check_control_geography_coverage(
    seed: pl.DataFrame,
    *,
    profile: str | None,
    geo_col: str = "ctrl_geoid",
    max_unplaceable_share: float,
) -> GeographyCoverage:
    """Count the profile's households the control geography cannot place.

    No new geography test is applied: *geo_col* is the point-in-polygon result
    the crosswalk already assigned, and a null there is the definition of
    outside.

    Args:
        seed: The profile's seed, one row per household, carrying *geo_col*.
        profile: Usability profile being fitted, or None.
        geo_col: Balancing-zone column assigned by the crosswalk.
        max_unplaceable_share: Largest share of the universe that may be
            unplaceable before this is treated as a misconfigured geography
            rather than a boundary effect.

    Returns:
        The coverage counts, for the log and the diagnostics report.

    Raises:
        ValueError: If the unplaceable share exceeds *max_unplaceable_share*.
    """
    n_universe = seed.height
    n_placed = seed.filter(pl.col(geo_col).is_not_null()).height if geo_col in seed.columns else 0
    coverage = GeographyCoverage(profile=profile, n_universe=n_universe, n_placed=n_placed)

    label = profile or "the survey"
    if coverage.n_unplaceable == 0:
        logger.info("Control geography places all %d %s households", n_universe, label)
        return coverage

    logger.warning(
        "%d of %d %s households (%.2f%%) fall outside the control geography and can "
        "carry no fitted weight; they are held out of the seed and reported.",
        coverage.n_unplaceable,
        n_universe,
        label,
        coverage.unplaceable_share * 100,
    )

    if coverage.unplaceable_share > max_unplaceable_share:
        msg = (
            f"{coverage.n_unplaceable} of {n_universe} households admitted by {label} "
            f"({coverage.unplaceable_share:.2%}) have no control geography, above the "
            f"{max_unplaceable_share:.2%} tolerance.\n"
            "  A household is placed by a strict point-in-polygon of its home against the "
            "target-zone file, while the model zones a usability profile reads are assigned "
            "with a snap tolerance -- so the two disagree at the boundary by construction, "
            "but only slightly.\n"
            "  A share this large usually means the target-zone geography does not cover the "
            "survey area. Check geography.target_zones, or raise max_unplaceable_share if the "
            "survey genuinely extends beyond the region being weighted."
        )
        raise ValueError(msg)

    return coverage
