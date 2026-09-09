"""Codebook enumerations for vehicle table."""

from data_canon.core.labeled_enum import LabeledEnum


class FuelType(LabeledEnum):
    """fuel_type value labels."""

    canonical_field_name = "fuel_type"

    GAS = (1, "Gas")
    HEV = (2, "Hybrid (HEV)")
    PHEV = (3, "Plug-in hybrid (PHEV)")
    EV = (4, "Electric (EV)")
    DIESEL = (5, "Diesel")
    FLEX = (6, "Flex fuel (FFV)")
    BIO = (997, "Other (e.g., natural gas, bio-diesel)")
