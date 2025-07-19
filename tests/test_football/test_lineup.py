import json

import pytest
from omegaconf import OmegaConf
from webdriver import ManagerWebdriver

from sofascrape.football import FootballLineupComponentScraper
from sofascrape.utils.testing import load_match_ids

# Load match IDs from JSON file
pl_24_25_ids = load_match_ids()


@pytest.fixture
def get_driver():
    mw = ManagerWebdriver()
    return mw.spawn_webdriver()


@pytest.fixture
def lineup_scraper(get_driver):
    return FootballLineupComponentScraper(webdriver=get_driver, matchid=11352253)


def test_basic_setup(get_driver):
    lineup_scraper = FootballLineupComponentScraper(
        webdriver=get_driver, matchid=12436870
    )
    print(pl_24_25_ids[0:5])
    print(OmegaConf.to_yaml(lineup_scraper.cfg))


def test_getpage(lineup_scraper):
    lineup_scraper.get_data()
    assert lineup_scraper.raw_data is not None, "Expected raw data got none."
    assert isinstance(
        lineup_scraper.raw_data, dict
    ), f"Expect dict data, got {type(lineup_scraper.raw_data)=}."

    print(json.dumps(lineup_scraper.raw_data, indent=4))


def test_process(lineup_scraper):
    result = lineup_scraper.process()

    assert result is not None, "Error getting the data"

    if result:
        print(result.model_dump_json(indent=6))


@pytest.mark.parametrize("matchid", pl_24_25_ids[0:10])  # might take awhile
def test_process_sweep(matchid):
    mw = ManagerWebdriver()
    d1 = mw.spawn_webdriver()
    lineup_scraper = FootballLineupComponentScraper(webdriver=d1, matchid=matchid)
    result = lineup_scraper.process()
    if result:
        print(result)
    d1.close()
