"""Validation exceptions for canonical survey data."""

from dataclasses import dataclass


@dataclass
class DataValidationError(Exception):
    """Structured validation error with context.

    Attributes:
        table: Name of the table being validated
        rule: Name of the validation rule that failed
        message: Human-readable error description
        row_id: Optional row identifier for row-level errors
        column: Optional column name for column-level errors
    """

    table: str
    rule: str
    message: str
    row_id: int | None = None
    column: str | None = None

    def __str__(self) -> str:
        """Format error message."""
        parts = [f"Table '{self.table}'"]
        if self.row_id is not None:
            parts.append(f"row {self.row_id}")
        if self.column:
            parts.append(f"column '{self.column}'")
        parts.append(f"- {self.rule}:")
        parts.append(self.message)
        return " ".join(parts)


__all__ = ["DataValidationError"]
