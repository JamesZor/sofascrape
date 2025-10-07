from pathlib import Path
from typing import Any, List, Optional, Tuple

from pydantic import BaseModel, Field, field_validator

# --- Pydantic Models for Odds Data ---


def parse_fraction(v: str) -> Tuple[int, int]:
    """Parses a string like '4/6' into a tuple (4, 6)."""
    if not isinstance(v, str):
        raise TypeError("Fractional value must be a string")
    try:
        numerator, denominator = map(int, v.split("/"))
        return (numerator, denominator)
    except (ValueError, IndexError):
        # This will catch errors like "4-6", "N/A", or "10"
        raise ValueError(f"'{v}' is not a valid fractional value string")


class OddsChoiceSchema(BaseModel):
    """Represents a single outcome with parsed fractional values."""

    # These fields will now store the parsed tuple, e.g., (4, 6)
    initial_fractional_value: Tuple[int, int] = Field(
        ..., alias="initialFractionalValue"
    )
    fractional_value: Tuple[int, int] = Field(..., alias="fractionalValue")

    source_id: int = Field(..., alias="sourceId")
    name: str
    winning: Optional[bool] = None
    change: int

    # Use the @field_validator decorator to apply the parsing logic
    # The '*' applies this validator to all fields in the model
    @field_validator("initial_fractional_value", "fractional_value", mode="before")
    @classmethod
    def parse_fractional_values(cls, v: Any) -> Tuple[int, int]:
        # 'mode='before'' tells Pydantic to run this validator on the raw input data
        # before it does any other type checking.
        if isinstance(v, str):
            return parse_fraction(v)
        return v  # type: ignore[no-any-return]


class OddsMarketSchema(BaseModel):
    """Represents a betting market (e.g., 'Full time', 'Both teams to score')."""

    market_id: int = Field(..., alias="marketId")
    market_name: str = Field(..., alias="marketName")
    market_group: str = Field(..., alias="marketGroup")
    is_live: bool = Field(..., alias="isLive")
    suspended: bool
    choices: List[OddsChoiceSchema]
    # 'choiceGroup' is not always present, so we make it optional
    choice_group: Optional[str] = Field(None, alias="choiceGroup")


class OddsSchema(BaseModel):
    """Top-level schema for the entire odds API response for a match."""

    markets: List[OddsMarketSchema]
    event_id: int = Field(..., alias="eventId")
