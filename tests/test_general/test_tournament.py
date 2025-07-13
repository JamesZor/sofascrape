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
def tournamentscraper_1():
    mw = ManagerWebdriver()
    tournamentscraper_id1 = TournamentComponentScraper(
        tournamentid=1, webdriver=mw.spawn_webdriver()
    )
    return tournamentscraper_id1


def test_basic_setup(get_driver):
    tournamentScraper = TournamentComponentScraper(tournamentid=1, webdriver=get_driver)
    print(OmegaConf.to_yaml(tournamentScraper.cfg))


def test_get_page_data(tournamentscraper_1):
    tour_id1_page_data = tournamentscraper_1.get_data()
    print(json.dumps(tour_id1_page_data, indent=4))
