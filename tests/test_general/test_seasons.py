import json
import logging

import pytest
from omegaconf import DictConfig, OmegaConf
from webdriver import ManagerWebdriver

from sofascrape.general import SeasonsComponentScraper


@pytest.fixture
def get_driver():
    mw = ManagerWebdriver()
    return mw.spawn_webdriver()


def test_basic_setup(get_driver):
    season_scraper = SeasonsComponentScraper(tournamentid=1, webdriver=get_driver)
    print(OmegaConf.to_yaml(cfg=season_scraper.cfg))


@pytest.fixture
def tournamentscraper_1(get_driver):
    tournamentscraper_id1 = SeasonsComponentScraper(
        tournamentid=1, webdriver=get_driver
    )
    return tournamentscraper_id1


def test_process(tournamentscraper_1):
    results = tournamentscraper_1.process()
    assert results is not None, "Error getting the data."
    for season in results.seasons[:10]:
        print(season)
