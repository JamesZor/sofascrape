from typing import List

import pytest
from omegaconf import OmegaConf
from webdriver import ManagerWebdriver

from sofascrape.general import EventsComponentScraper, SeasonsComponentScraper

seaonsid_list: List[int] = [61627, 52186, 41886]


@pytest.fixture
def get_driver():
    mw = ManagerWebdriver()
    return mw.spawn_webdriver()


def test_basic_setup(get_driver):
    events_scraper = EventsComponentScraper(
        seasonid=61627, tournamentid=1, webdriver=get_driver
    )
    print(OmegaConf.to_yaml(cfg=events_scraper.cfg))


# def test_procces(get_driver):
#    season_events_scraper = EventsComponentScraper(
#        webdriver=get_driver, tournamentid=1, seasonid=seaonsid_list[1]
#    )
#    result = season_events_scraper.process()
#
#    assert result is not None, "Error getting the data"
#
#    if result:
#        for event in result.events[:1]:
#            print("+" * 20)
#            print(event)
#
#        valid_count: int = 0
#        not_valid_count: int = 0
#        for event in result.events:
#            if event.status.code == 100:
#                valid_count += 1
#            else:
#                not_valid_count += 1
#
#        print("+" * 20)
#        print(f" {valid_count =} ..... {not_valid_count=}")
#        print("+" * 20)
#
#        print("+" * 20)
#        print(f" {valid_count =} ..... {not_valid_count=}")
#


@pytest.mark.parametrize("seasonid", seaonsid_list)
def test_procces_sweep(get_driver, seasonid):
    season_events_scraper = EventsComponentScraper(
        webdriver=get_driver, tournamentid=1, seasonid=seasonid
    )
    result = season_events_scraper.process()

    assert result is not None, "Error getting the data"

    if result:
        for event in result.events[:1]:
            print("+" * 20)
            print("+" * 5, seasonid)
            print("+" * 20)

            print(event)

        valid_count: int = 0
        not_valid_count: int = 0
        for event in result.events:
            if event.status.code == 100:
                valid_count += 1
            else:
                not_valid_count += 1
        print("+" * 20)
        print(f" {valid_count =} ..... {not_valid_count=}")
