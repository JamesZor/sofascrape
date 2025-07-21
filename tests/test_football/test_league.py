import json

import pytest
from omegaconf import OmegaConf

from sofascrape.football import LeagueFootballScraper
from sofascrape.utils.testing import load_match_ids

# Load match IDs from JSON file
pl_24_25_ids = load_match_ids()

tournament_id = 1
"""
{
  "name": "Premier League 24/25",
  "id": 61627,
  "year": "24/25"
}
{
  "name": "Premier League 23/24",
  "id": 52186,
  "year": "23/24"
}
{
  "name": "Premier League 22/23",
  "id": 41886,
  "year": "22/23"
}
{
  "name": "Premier League 21/22",
  "id": 37036,
  "year": "21/22"
}
{
  "name": "Premier League 20/21",
  "id": 29415,
  "year": "20/21"
}
"""


def test_basic_setup():
    league_scraper = LeagueFootballScraper(tournamentid=tournament_id)
    print(OmegaConf.to_yaml(league_scraper.cfg))


def test_wip_scrape():
    league_scraper = LeagueFootballScraper(tournamentid=tournament_id)
    results_dict = league_scraper._wip_scrape()

    for seasonid, season_scrape in results_dict.items():
        print(seasonid)
        for event in season_scrape.data.matches[0:2]:
            print(event.data.base.model_dump_json(indent=5))
