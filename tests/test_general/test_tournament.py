import logging

import pytest
from omegaconf import DictConfig, OmegaConf
from webdriver import ManagerWebdriver

from sofascrape.general import Tournament


@pytest.fixture
def get_driver():
    mw = ManagerWebdriver()
    return mw.spawn_webdriver()


def test_basic_setup(get_driver):
    tournamentScraper = Tournament(webdriver=get_driver)
    print(OmegaConf.to_yaml(tournamentScraper.cfg))
