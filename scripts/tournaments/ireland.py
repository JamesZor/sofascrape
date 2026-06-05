"""
Ireland:
  - Premier Division (ID: 79, Slug: premier-division)
  - First Division (ID: 718, Slug: first-division)
"""

import logging

from sqlalchemy import select
from sqlalchemy.orm import Session

from sofascrape.conf.config import load_config
from sofascrape.db.manager import DatabaseManager
from sofascrape.db.models import Component, Events, MatchComponentAudit, Season
from sofascrape.pipeline.orchestrator import Orchestrator
from sofascrape.utils.scrap_tournament_script_helpers import (
    get_seasonid_year_from_tournament,
    queue_list_of_seasons,
)

logging.basicConfig(level=logging.WARNING, force=True)
logging.getLogger("sofascrape").setLevel(logging.WARNING)

config = load_config()
db = DatabaseManager(config)
pipeline = Orchestrator(db, config)

tournaments = [79, 718]

target_components = [
    Component.BASE,
    Component.ODDS,
    Component.LINEUPS,
    Component.INCIDENTS,
    Component.STATS,
    Component.GRAPH,
]

all_seasons = []

for tour_id in tournaments:
    pipeline.setup_tournament(tour_id)

    list_season_ids = get_seasonid_year_from_tournament(
        pipeline=pipeline, tournament_id=tour_id, result_limit=6
    )
    all_seasons.extend(list_season_ids)

queue_list_of_seasons(
    season_tournament_list=all_seasons,
    target_components=target_components,
    pipeline=pipeline,
)

pipeline.run_worker_loop(
    max_workers=config.pipeline.max_workers,
    task_limit=None,
)
