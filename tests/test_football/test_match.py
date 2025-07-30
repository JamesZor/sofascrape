import json
from pathlib import Path

import pytest
from omegaconf import OmegaConf
from webdriver import ManagerWebdriver

from sofascrape.football import FootballMatchScraper
from sofascrape.utils import NoteBookType, NotebookUtils
from sofascrape.utils.testing import load_match_ids

# Load match IDs from JSON file
pl_24_25_ids = load_match_ids()


@pytest.fixture
def get_driver():
    mw = ManagerWebdriver()
    return mw.spawn_webdriver()


def test_basic_setup(get_driver):
    graph_scraper = FootballMatchScraper(webdriver=get_driver, matchid=12436870)
    print(OmegaConf.to_yaml(graph_scraper.cfg))


def test_process():
    mw = ManagerWebdriver()
    d1 = mw.spawn_webdriver()
    scraper = FootballMatchScraper(webdriver=d1, matchid=pl_24_25_ids[20])
    result = scraper.scrape()
    if result:
        print(result.model_dump_json(indent=10))
    d1.close()


@pytest.mark.parametrize("matchid", pl_24_25_ids[0:10])
def test_process_sweep(matchid):
    mw = ManagerWebdriver()
    d1 = mw.spawn_webdriver()
    scraper = FootballMatchScraper(webdriver=d1, matchid=matchid)
    result = scraper.scrape()
    assert result is not None, "None data"
    d1.close()


def test_save_match():
    nbu = NotebookUtils(type=NoteBookType.FOOTBALL, web_on=True)
    scraper = FootballMatchScraper(webdriver=nbu.driver, matchid=pl_24_25_ids[20])
    result = scraper.scrape()
    if result:
        nbu.save_pickle("football_match_0", result)
