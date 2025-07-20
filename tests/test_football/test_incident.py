import json
from pathlib import Path

import pytest
from omegaconf import OmegaConf
from webdriver import ManagerWebdriver

from sofascrape.football import FootballIncidentsComponentScraper
from sofascrape.utils.testing import load_match_ids

# Load match IDs from JSON file
pl_24_25_ids = load_match_ids()


@pytest.fixture
def get_driver():
    mw = ManagerWebdriver()
    return mw.spawn_webdriver()


@pytest.fixture
def incidents_scraper(get_driver):
    return FootballIncidentsComponentScraper(webdriver=get_driver, matchid=11352253)


def test_basic_setup(get_driver):
    incidents_scraper = FootballIncidentsComponentScraper(
        webdriver=get_driver, matchid=12436870
    )
    print(OmegaConf.to_yaml(incidents_scraper.cfg))


def test_getpage(incidents_scraper):
    incidents_scraper.get_data()
    assert incidents_scraper.raw_data is not None, "Expected raw data got none."
    assert isinstance(
        incidents_scraper.raw_data, dict
    ), f"Expect dict data, got {type(incidents_scraper.raw_data)=}."

    print(json.dumps(incidents_scraper.raw_data, indent=4))


def test_process(incidents_scraper):
    result = incidents_scraper.process()

    assert result is not None, "Error getting the data"

    if result:
        print(result.model_dump_json(indent=6))


@pytest.mark.parametrize("matchid", pl_24_25_ids[0:10])  # might take awhile
def test_process_sweep(matchid):
    mw = ManagerWebdriver()
    d1 = mw.spawn_webdriver()
    incidents_scraper = FootballIncidentsComponentScraper(webdriver=d1, matchid=matchid)
    result = incidents_scraper.process()

    assert result is not None, "Error getting the data"
    print(result)

    d1.close()
