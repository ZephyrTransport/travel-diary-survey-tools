"""Data models for trip linking."""

import pandera.polars as pa
import polars as pl
import pydantic as pd


# Data Models ------------------------------------------------------------------
class CoordModel(pd.BaseModel):
    """Coordinate data model for validation."""

    latitude: float = pd.Field(ge=-90.0, le=90.0)
    longitude: float = pd.Field(ge=-180.0, le=180.0)


# Minimal data schema for trip linking
class TripModel(pa.DataFrameModel):
    """Trip data model for validation."""

    trip_id: pl.Int64 = pa.Field(ge=1)
    day_id: pl.Int64 = pa.Field(ge=1)
    person_id: pl.Int64 = pa.Field(ge=1)
    hh_id: pl.Int64 = pa.Field(ge=1)
    depart_date: pl.Utf8
    depart_hour: pl.Int64 = pa.Field(ge=0, le=23)
    depart_minute: pl.Int64 = pa.Field(ge=0, le=59)
    depart_seconds: pl.Int64 = pa.Field(ge=0, le=59)
    arrive_date: pl.Utf8
    arrive_hour: pl.Int64 = pa.Field(ge=0, le=23)
    arrive_minute: pl.Int64 = pa.Field(ge=0, le=59)
    arrive_seconds: pl.Int64 = pa.Field(ge=0, le=59)
    o_purpose_category: pl.Int64
    d_purpose_category: pl.Int64
    mode_type: pl.Int64
    duration_minutes: pl.Float64 = pa.Field(ge=0, nullable=True, coerce=True)
    distance_miles: pl.Float64 = pa.Field(ge=0, nullable=True, coerce=True)

    depart_time: pl.Datetime = pa.Field(ge=0)
    arrive_time: pl.Datetime = pa.Field(ge=0)

    # Checks
    @pa.check("arrive_time")
    def arrival_after_departure(cls, data) -> pl.LazyFrame:  # noqa: N805, ANN001
        """Ensure arrive_time is after depart_time."""
        return data.lazyframe.select(
            pl.col("arrive_time").ge(pl.col("depart_time")).all(),
        )


# Subclassing allows you to extend TripModel cleanly
class LinkedTripModel(TripModel):
    """Linked Trip data model for validation."""

    trip_id: None
    linked_trip_id: pl.Int64 = pa.Field(ge=1, nullable=True)
