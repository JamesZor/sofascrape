import json

import pytest
from omegaconf import OmegaConf
from webdriver import ManagerWebdriver

from sofascrape.football import EventFootallComponentScraper
from sofascrape.utils.testing import load_match_ids

# Load match IDs from JSON file
pl_24_25_ids = load_match_ids()


@pytest.fixture
def get_driver():
    mw = ManagerWebdriver()
    return mw.spawn_webdriver()


@pytest.fixture
def match_details_scraper(get_driver):
    return EventFootallComponentScraper(webdriver=get_driver, matchid=11352253)


def test_basic_setup(get_driver):
    eventDetailsScraper = EventFootallComponentScraper(
        webdriver=get_driver, matchid=12436870
    )
    print(OmegaConf.to_yaml(eventDetailsScraper.cfg))


def test_getpage(match_details_scraper):
    match_details_scraper.get_data()
    assert match_details_scraper.raw_data is not None, "Expected raw data got none."
    assert isinstance(
        match_details_scraper.raw_data, dict
    ), f"Expect dict data, got {type(match_details_scraper.raw_data)=}."

    print(json.dumps(match_details_scraper.raw_data, indent=4))


def test_process(match_details_scraper):
    result = match_details_scraper.process()

    assert result is not None, "Error getting the data"

    if result:
        print(result.model_dump_json(indent=6))


@pytest.mark.parametrize("matchid", pl_24_25_ids)  # might take awhile
def test_process_sweep(matchid):
    mw = ManagerWebdriver()
    d1 = mw.spawn_webdriver()
    event_details_scraper = EventFootallComponentScraper(webdriver=d1, matchid=matchid)
    result = event_details_scraper.process()
    if result:
        print(result.event.slug)
    d1.close()
