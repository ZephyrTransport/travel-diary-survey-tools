"""Pipeline framework for data processing workflows."""

from .decoration import step
from .pipeline import Pipeline

__all__ = ["Pipeline", "step"]
