import json

import pytest
from omegaconf import OmegaConf

from sofascrape.football import SeasonFootballScraper
from sofascrape.utils.testing import load_match_ids

# Load match IDs from JSON file
pl_24_25_ids = load_match_ids()

tournament_id = 1
season_id = 61627


def test_basic_setup():
    season_scraper = SeasonFootballScraper(
        tournamentid=tournament_id, seasonid=season_id
    )
    print(OmegaConf.to_yaml(season_scraper.cfg))


def test_get_events():
    season_scraper = SeasonFootballScraper(
        tournamentid=tournament_id, seasonid=season_id
    )
    season_scraper._get_events()

    for event in season_scraper.events_scraper.data.events[0:3]:
        print(event.model_dump_json(indent=10))

    print(season_scraper.valid_matchids[0:4])


def test_debug_scrape():
    season_scraper = SeasonFootballScraper(
        tournamentid=tournament_id, seasonid=season_id
    )
    season_scraper._scrape_debug(use_threading=True, max_workers=3)

    print(f"Total matches = {season_scraper.data.total_matches}")

    for match in season_scraper.data.matches:
        print(match.data.base.model_dump_json(indent=4))


def test_same_stats():

    s1 = SeasonFootballScraper(tournamentid=tournament_id, seasonid=season_id)
    s1._scrape_debug(use_threading=True, max_workers=3)

    s2 = SeasonFootballScraper(tournamentid=tournament_id, seasonid=season_id)
    s2._scrape_debug(use_threading=True, max_workers=3)

    for i in range(len(s1.data.matches)):
        print(s1.data.matches[i].match_id)
        print(
            f"{s1.data.matches[i].data.base.event.homeTeam.name}, {s1.data.matches[i].data.base.event.awayTeam.name}"
        )

        for j in range(
            len(s1.data.matches[i].data.stats.statistics[0].groups[0].statisticsItems)
        ):

            print(
                f"{s1.data.matches[i].data.stats.statistics[0].groups[0].statisticsItems[j].home},{s1.data.matches[i].data.stats.statistics[0].groups[0].statisticsItems[j].away}."
            )

            print(
                f"{s2.data.matches[i].data.stats.statistics[0].groups[0].statisticsItems[j].home},{s2.data.matches[i].data.stats.statistics[0].groups[0].statisticsItems[j].away}."
            )

        print("--- ----" * 20)


def test_process():

    season_scraper = SeasonFootballScraper(
        tournament_id=tournament_id, season_id=season_id
    )
    data = season_scraper.scrape(use_threading=True, max_workers=10)
    print(data.model_dump_json(indent=20))
