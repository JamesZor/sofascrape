import json
from pathlib import Path

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
        tournament_id=tournament_id, season_id=season_id
    )
    print(OmegaConf.to_yaml(season_scraper.cfg))


def test_process():

    season_scraper = SeasonFootballScraper(
        tournament_id=tournament_id, season_id=season_id
    )
    data = season_scraper.scrape(use_threading=True, max_workers=10)
    print(data.model_dump_json(indent=20))
