import json
import logging

import pytest
from omegaconf import DictConfig, OmegaConf
from webdriver import ManagerWebdriver

from sofascrape.general import TournamentComponentScraper


@pytest.fixture
def get_driver():
    mw = ManagerWebdriver()
    return mw.spawn_webdriver()


@pytest.fixture
def tournamentscraper_1(get_driver):
    tournamentscraper_id1 = TournamentComponentScraper(
        tournamentid=1, webdriver=get_driver
    )
    return tournamentscraper_id1


@pytest.fixture
def tournamentscraper_27(get_driver):
    tournamentscraper = TournamentComponentScraper(tournamentid=1, webdriver=get_driver)
    return tournamentscraper


def test_basic_setup(get_driver):
    tournamentScraper = TournamentComponentScraper(tournamentid=1, webdriver=get_driver)
    print(OmegaConf.to_yaml(tournamentScraper.cfg))


def test_get_page_data(tournamentscraper_1):
    tournamentscraper_1.get_data()
    print(json.dumps(tournamentscraper_1.raw_data, indent=4))


def test_process(tournamentscraper_1):
    result = tournamentscraper_1.process()
    assert result is not None, "error getting the data"
    if result:
        print(f"Tournament: {result.tournament.name}")
        print(f"Sport: {result.tournament.category.sport.name}")
        print(f"Country: {result.tournament.category.name}")
        print(result)


@pytest.mark.parametrize("tournament_id", [27, 34, 73, 19])  # Add more IDs as needed
def test_process_multiple_tournaments(get_driver, tournament_id):
    tournamentscraper = TournamentComponentScraper(
        tournamentid=tournament_id, webdriver=get_driver
    )

    result = tournamentscraper.process()
    assert result is not None, f"Error getting data for tournament {tournament_id}"

    if result:
        print(f"Tournament ID: {tournament_id}")
        print(f"Tournament: {result.tournament.name}")
        print(f"Sport: {result.tournament.category.sport.name}")
        print(f"Country: {result.tournament.category.name}")
        print(f"Result: {result}")
