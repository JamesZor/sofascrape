# tests/tests_football/test_match_scraper.py
import json
from pathlib import Path
from typing import List

import pytest
from webdriver import ManagerWebdriver

import sofascrape.schemas.general as schemas
import sofascrape.schemas.odds as SchemaOdds

# Adjust imports as per your project structure
from sofascrape.football.matchScraper import FootballMatchScraper, MatchComponentType

# Data for the tests
scottish_l2_s25_matchids: List[int] = [
    14035666,
    14035662,
    14035664,
]

# --- Fixtures ---


@pytest.fixture(scope="module")
def webdriver_manager() -> ManagerWebdriver:
    """Provides a single webdriver manager for the entire test module for efficiency."""
    return ManagerWebdriver()


# --- Test Class for FootballMatchScraper ---


class TestFootballMatchScraper:
    def test_scrape_single_match_with_odds(
        self, webdriver_manager: ManagerWebdriver, tmp_path: Path
    ):
        """
        Tests scraping a single match, explicitly including the odds component,
        and saves the result to a temporary JSON file.
        """
        match_id = scottish_l2_s25_matchids[0]
        driver = webdriver_manager.spawn_webdriver()

        # Instantiate the main scraper
        scraper = FootballMatchScraper(webdriver=driver, matchid=match_id)

        # Explicitly scrape BASE and ODDS to test integration
        result = scraper.scrape(
            components=[MatchComponentType.BASE, MatchComponentType.ODDS]
        )

        # --- Assertions ---
        assert result is not None, "Scraping returned None"
        assert result.match_id == match_id

        # CRITICAL: Assert that the odds component was successfully scraped
        assert result.odds is not None, "The 'odds' attribute should be populated"
        assert isinstance(
            result.odds, SchemaOdds.OddsSchema
        ), "Odds data is not the correct Pydantic type"
        assert (
            result.errors.odds.status == schemas.ComponentStatus.SUCCESS
        ), "Error status for odds should be SUCCESS"

        # --- Save Output to File ---
        output_file = tmp_path / f"match_{match_id}_details.json"
        output_file.write_text(result.model_dump_json(indent=2))

        # Print the path to the console so you can easily find it
        print(f"\n✅ Saved single match result to: {output_file}")

        driver.close()

    @pytest.mark.parametrize("matchid", scottish_l2_s25_matchids)
    def test_scrape_sweep_and_save(
        self, webdriver_manager: ManagerWebdriver, tmp_path: Path, matchid: int
    ):
        """
        Tests the full scraping process for multiple matches and saves each result
        to a separate JSON file in a temporary directory.
        """
        driver = webdriver_manager.spawn_webdriver()
        scraper = FootballMatchScraper(webdriver=driver, matchid=matchid)

        # This assumes your config is set to scrape all components, including ODDS
        result = scraper.scrape()

        # --- Assertions ---
        assert result is not None, f"Scraping failed for matchid {matchid}"
        assert result.odds is not None, f"Odds data was not found for matchid {matchid}"
        assert result.errors.odds.status == schemas.ComponentStatus.SUCCESS

        # --- Save Output to File ---
        output_file = tmp_path / f"match_{matchid}_sweep.json"
        output_file.write_text(result.model_dump_json(indent=2))

        print(f"\n✅ Saved sweep result for match {matchid} to: {output_file}")

        driver.close()
