# tests/tests_odds/test_odds_component.py

import json
from typing import List, Tuple

import pytest
from webdriver import ManagerWebdriver, MyWebDriver

# Adjust the import path if your component is in a different location
from sofascrape.football import FootballOddsComponentScraper

# A list of match IDs can remain at the module level
scottish_l2_s25_matchids: List[int] = [
    14035666,
    14035662,
    14035664,
    14035663,
    14035665,
    14035670,
]

# --- Fixtures can remain outside the class for reusability ---


@pytest.fixture(scope="module")
def webdriver_manager() -> ManagerWebdriver:
    """Provides a single webdriver manager for the entire test module."""
    return ManagerWebdriver()


@pytest.fixture
def odds_scraper(webdriver_manager: ManagerWebdriver) -> FootballOddsComponentScraper:
    """Provides a fresh scraper instance for each test."""
    driver: MyWebDriver = webdriver_manager.spawn_webdriver()
    scraper = FootballOddsComponentScraper(
        webdriver=driver, matchid=scottish_l2_s25_matchids[0]
    )
    yield scraper
    driver.close()


# --- Test Class for the FootballOddsComponentScraper ---


class TestFootballOddsComponentScraper:
    def test_get_data(self, odds_scraper: FootballOddsComponentScraper):
        """
        Tests the get_data method to ensure it fetches and stores raw data correctly.
        """
        odds_scraper.get_data()

        assert odds_scraper.raw_data is not None, "raw_data should not be None"
        assert isinstance(odds_scraper.raw_data, dict)

        print("\n--- Raw Data Fetched ---")
        print(json.dumps(odds_scraper.raw_data, indent=2))

    def test_parse_data(self, odds_scraper: FootballOddsComponentScraper):
        """
        Tests the parse_data method to ensure it validates the raw data successfully.
        """
        odds_scraper.get_data()
        assert (
            odds_scraper.raw_data is not None
        ), "Setup failed: raw_data was not fetched."

        odds_scraper.parse_data()

        assert odds_scraper.data is not None, "data attribute should be populated"

        print("\n--- Parsed Pydantic Model ---")
        print(odds_scraper.data.model_dump_json(indent=2, by_alias=True))

    @pytest.mark.parametrize("matchid", scottish_l2_s25_matchids[:3])
    def test_component_process_sweep(
        self, webdriver_manager: ManagerWebdriver, matchid: int
    ):
        """
        An integration test that runs the full .process() method and prints key odds.
        """
        driver = webdriver_manager.spawn_webdriver()
        scraper = FootballOddsComponentScraper(webdriver=driver, matchid=matchid)

        result = scraper.process()
        assert result is not None, f"Processing failed for match ID {matchid}"

        # --- Logic to find and print specific odds ---

        # Use a dictionary to store the odds we find, with defaults
        odds_to_print = {
            "1": "N/A",
            "X": "N/A",
            "2": "N/A",
            "U0.5": "N/A",
            "U1.5": "N/A",
            "U2.5": "N/A",
        }

        def format_odds(odds: Tuple[int, int]) -> str:
            return f"{odds[0]}/{odds[1]}"

        # Iterate through markets to find the ones we want
        for market in result.markets:
            if market.market_name == "Full time":
                for choice in market.choices:
                    if choice.name in ["1", "X", "2"]:
                        odds_to_print[choice.name] = format_odds(
                            choice.fractional_value
                        )

            if market.market_name == "Match goals":
                # The choice_group tells us which Over/Under line it is
                key = f"U{market.choice_group}"
                if key in odds_to_print:
                    for choice in market.choices:
                        if choice.name == "Under":
                            odds_to_print[key] = format_odds(choice.fractional_value)

        # Print the final, formatted output for the match
        print(
            f"\nMatch ID: {matchid} | "
            f"1x2: [1: {odds_to_print['1']}, X: {odds_to_print['X']}, 2: {odds_to_print['2']}] | "
            f"Unders: [0.5: {odds_to_print['U0.5']}, 1.5: {odds_to_print['U1.5']}, 2.5: {odds_to_print['U2.5']}]"
        )

        driver.close()
