import json
import os
from pathlib import Path
from typing import List, Optional

import json5  # Import the json5 library
from pydantic import BaseModel, Field

# --- Pydantic Models for Odds Data ---


class OddsChoiceSchema(BaseModel):
    """Represents a single outcome within a betting market (e.g., '1', 'X', '2')."""

    initial_fractional_value: str = Field(..., alias="initialFractionalValue")
    fractional_value: str = Field(..., alias="fractionalValue")
    source_id: int = Field(..., alias="sourceId")
    name: str
    winning: Optional[bool] = None
    change: int


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


# --- Main execution block to test the models ---

if __name__ == "__main__":
    # 1. Define the path to your example JSON file
    # Assumes 'example_odds.json' is in the same directory
    json_path = Path(__file__).parent / "example_odds.json"
    json_path = Path(
        "/home/james/bet_project/sofascrape/experiments/scottish_leagues/example_odds.json"
    )
    json_path

    try:
        with open(json_path, "r") as f:
            # Read the entire file content into a string
            file_content = f.read()

            # Use json5.loads() to parse the string into a dictionary
            odds_data = json5.loads(file_content)

        # Now you have a valid Python dictionary to pass to Pydantic
        validated_odds = OddsSchema.model_validate(odds_data)
        print("Successfully validated data!")
        # print(validated_odds)

    except Exception as e:
        print(f"An error occurred: {e}")

    # 3. Validate the data using your Pydantic model
    try:
        validated_odds = OddsSchema.model_validate(odds_data)
        print("\n✅ Data successfully validated!")

        # 4. Print some of the validated data to see it working
        print(f"\nEvent ID: {validated_odds.event_id}")
        print(f"Total Markets Found: {len(validated_odds.markets)}")

        # Print details of the first market
        if validated_odds.markets:
            first_market = validated_odds.markets[0]
            print(f"\n--- Example of First Market ---")
            print(f"Market Name: {first_market.market_name}")
            print(f"Market Group: {first_market.market_group}")

            # Print choices for the first market
            for choice in first_market.choices:
                print(
                    f"  - Choice: {choice.name}, "
                    f"Fractional Value: {choice.fractional_value}, "
                    f"Winning: {choice.winning}"
                )

        # You can also easily convert it back to a dictionary or JSON
        # print("\n--- Full Model Dump (JSON) ---")
        # print(validated_odds.model_dump_json(indent=2, by_alias=True))

    except Exception as e:
        print(f"\n❌ Validation Failed: {e}")
