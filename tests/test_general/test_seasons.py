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
