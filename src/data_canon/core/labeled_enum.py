"""Base class for labeled enumerations with values and human-readable labels.

This module provides the LabeledEnum base class for creating enumerations
that store both integer values and descriptive labels, with support for
canonical field names for pydantic model mapping.
"""

from enum import Enum, EnumType
from typing import Optional


class LabeledEnumMeta(EnumType):
    """Metaclass for LabeledEnum that reserves canonical_field_name."""

    @classmethod
    def __prepare__(metacls, cls, bases, **kwds):  # noqa: ANN001, ANN003, ANN206, N804
        """Prepare the class namespace, ignoring reserved fields."""
        namespace = super().__prepare__(cls, bases, **kwds)
        # Add _ignore_ to automatically ignore reserved fields
        namespace["_ignore_"] = ["canonical_field_name", "field_description"]
        return namespace

    def __new__(metacls, cls, bases, classdict, **kwds):  # noqa: ANN001, ANN003, ANN204
        """Create the enum class and preserve reserved fields."""
        # Extract reserved fields before enum processing
        canonical_name = classdict.get("canonical_field_name")
        field_desc = classdict.get("field_description")

        # Create the enum class
        enum_class = super().__new__(metacls, cls, bases, classdict, **kwds)

        # Restore reserved fields as class attributes
        if canonical_name is not None:
            enum_class.canonical_field_name = canonical_name
        if field_desc is not None:
            enum_class.field_description = field_desc

        return enum_class


class LabeledEnum(Enum, metaclass=LabeledEnumMeta):
    """Base class for enumerations with values and labels.

    Each enum member is defined as a tuple of (value, label):
        MEMBER_NAME = (1, "Descriptive Label")

    The enum provides:
    - value: The integer value
    - label: The human-readable label
    - field_name: The canonical field name
    - description: The field description

    Class Methods:
    - from_value(val): Look up an enum member by its value
    - from_label(label): Look up an enum member by its label
    - get_field_name(): Get the canonical field name for the enum
    - get_description(): Get the field description for the enum

    Example:
        class Gender(LabeledEnum):
            canonical_field_name = "gender"
            field_description = "Respondent's gender identity"

            MALE = (1, "Male")
            FEMALE = (2, "Female")

        member = Gender.MALE
        print(member.value)  # 1
        print(member.label)  # "Male"
        print(member.field_name)  # "gender"
        print(member.description)  # "Respondent's gender identity"

        found = Gender.from_value(1)  # Returns Gender.MALE
        found = Gender.from_label("Female")  # Returns Gender.FEMALE
    """

    def __new__(cls, value: int, label: str) -> "LabeledEnum":
        """Create a new enum member with value and label.

        Args:
            value: The integer value for the enum member
            label: The human-readable label for the enum member
        """
        obj = object.__new__(cls)
        obj._value_ = value
        obj._label_ = label
        return obj

    @property
    def label(self) -> str:
        """Get the human-readable label for this enum member."""
        return self._label_

    @property
    def field_name(self) -> str:
        """Get the canonical field name for this enum.

        Returns the canonical_field_name class attribute if defined,
        otherwise returns None.
        """
        return getattr(self.__class__, "canonical_field_name", None)

    @property
    def description(self) -> str | None:
        """Get the field description for this enum.

        Returns the field_description class attribute if defined,
        otherwise returns None.
        """
        return getattr(self.__class__, "field_description", None)

    @classmethod
    def from_value(cls, value: int) -> Optional["LabeledEnum"]:
        """Look up an enum member by its value.

        Args:
            value: The integer value to search for

        Returns:
            The enum member with the matching value, or None if not found
        """
        for member in cls:
            if member.value == value:
                return member
        return None

    @classmethod
    def from_label(cls, label: str) -> Optional["LabeledEnum"]:
        """Look up an enum member by its label.

        Args:
            label: The label to search for (case-sensitive)

        Returns:
            The enum member with the matching label, or None if not found
        """
        for member in cls:
            if member.label == label:
                return member
        return None

    @classmethod
    def get_field_name(cls) -> str | None:
        """Get the canonical field name for this enum class.

        Returns:
            The canonical_field_name class attribute if defined, otherwise None
        """
        return getattr(cls, "canonical_field_name", None)

    @classmethod
    def get_description(cls) -> str | None:
        """Get the field description for this enum class.

        Returns:
            The field_description class attribute if defined, otherwise None
        """
        return getattr(cls, "field_description", None)

    @classmethod
    def to_dict(cls) -> dict[int, str]:
        """Get a dictionary mapping enum values to labels.

        Returns:
            A dictionary where keys are enum values and values are labels
        """
        return {member.value: member.label for member in cls}
