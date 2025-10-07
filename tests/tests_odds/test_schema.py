# tests/tests_odds/test_schema
from pathlib import Path
from typing import Any, Dict

import json5
import pytest
from pydantic import ValidationError

from sofascrape.schemas.odds import OddsSchema, parse_fraction

# --- Pytest Fixture for Loading Data ---


@pytest.fixture(scope="module")
def odds_data() -> Dict[str, Any]:
    """
    Loads the example odds data from the JSON file using json5.
    This fixture is run once per module and its result is cached.
    """
    # Create a path relative to this test file.
    # json_path = Path(__file__).parent / "example_odds.json"
    json_path = Path(
        "/home/james/bet_project/sofascrape/experiments/scottish_leagues/example_odds.json"
    )

    if not json_path.exists():
        pytest.fail(f"Test data file not found at: {json_path}")

    with open(json_path, "r") as f:
        return json5.load(f)


# --- Tests for the Pydantic Schemas ---


class TestOddsSchema:
    def test_successful_validation(self, odds_data: Dict[str, Any]):
        """
        Tests that the valid example data can be successfully loaded and validated
        by the top-level OddsSchema.
        """
        # The test will fail automatically if a ValidationError is raised.
        validated_odds = OddsSchema.model_validate(odds_data)

        # --- Displaying the validated data for visual inspection ---
        print("\n--- ✅ Validated Pydantic Model Output ---")
        print(validated_odds.model_dump_json(indent=2, by_alias=True))
        print("--- End of Model Output ---")

        assert isinstance(validated_odds, OddsSchema)

        assert validated_odds.event_id == 14035666
        assert len(validated_odds.markets) > 0

        first_market = validated_odds.markets[0]
        assert first_market.market_name == "Full time"

        first_choice = first_market.choices[0]
        assert first_choice.name == "1"

        assert first_choice.fractional_value == (21, 20)


# --- Unit Tests for the Helper Function ---


@pytest.mark.parametrize(
    "input_string, expected_output",
    [
        ("4/6", (4, 6)),
        ("11/10", (11, 10)),
        ("1/1", (1, 1)),
        ("0/1", (0, 1)),
    ],
)
def test_parse_fraction_valid(input_string, expected_output):
    """
    Tests the parse_fraction helper function with various valid inputs.
    """
    assert parse_fraction(input_string) == expected_output


@pytest.mark.parametrize(
    "invalid_input, expected_exception",
    [
        ("4-6", ValueError),  # Invalid separator
        ("N/A", ValueError),  # Not integers
        ("10", ValueError),  # Missing denominator
        (None, TypeError),  # Invalid input type
        ("1/two", ValueError),  # Contains non-integer
    ],
)
def test_parse_fraction_invalid(invalid_input, expected_exception):
    """
    Tests that the parse_fraction helper function correctly raises errors
    for malformed inputs.
    """
    with pytest.raises(expected_exception):
        parse_fraction(invalid_input)
