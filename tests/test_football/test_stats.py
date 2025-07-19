import json

import pytest
from omegaconf import OmegaConf
from webdriver import ManagerWebdriver

from sofascrape.football import FootballStatsComponentScraper
from sofascrape.utils.testing import load_match_ids

# Load match IDs from JSON file
pl_24_25_ids = load_match_ids()


@pytest.fixture
def get_driver():
    mw = ManagerWebdriver()
    return mw.spawn_webdriver()


@pytest.fixture
def stats_scraper(get_driver):
    return FootballStatsComponentScraper(webdriver=get_driver, matchid=11352253)


def test_basic_setup(get_driver):
    stats_Scraper = FootballStatsComponentScraper(
        webdriver=get_driver, matchid=12436870
    )
    print(OmegaConf.to_yaml(stats_Scraper.cfg))


def test_getpage(stats_scraper):
    stats_scraper.get_data()
    assert stats_scraper.raw_data is not None, "Expected raw data got none."
    assert isinstance(
        stats_scraper.raw_data, dict
    ), f"Expect dict data, got {type(stats_scraper.raw_data)=}."

    print(json.dumps(stats_scraper.raw_data, indent=4))


def test_process(stats_scraper):
    result = stats_scraper.process()

    assert result is not None, "Error getting the data"

    if result:
        print(result.model_dump_json(indent=6))


@pytest.mark.parametrize("matchid", pl_24_25_ids[0:20])  # might take awhile
def test_process_sweep(matchid):
    mw = ManagerWebdriver()
    d1 = mw.spawn_webdriver()
    stats_scraper = FootballStatsComponentScraper(webdriver=d1, matchid=matchid)
    result = stats_scraper.process()
    if result:
        print(result)
    d1.close()
