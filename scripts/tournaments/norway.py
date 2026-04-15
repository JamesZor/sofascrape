"""
Norway:
  - Eliteserien (ID: 5, Slug: eliteserien)
  - 1st Division (ID: 6, Slug: 1st-division)
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


eliteserien_tour_id = 5
division_1st_tour_id = 6


pipeline.setup_tournament(eliteserien_tour_id)
pipeline.setup_tournament(division_1st_tour_id)

pipeline.setup_tournament(eliteserien_tour_id)
pipeline.setup_tournament(division_1st_tour_id)


list_season_ids_l1 = get_seasonid_year_from_tournament(
    pipeline=pipeline, tournament_id=eliteserien_tour_id, result_limit=3
)

list_season_ids_l2 = get_seasonid_year_from_tournament(
    pipeline=pipeline, tournament_id=division_1st_tour_id, result_limit=3
)


target_components = [
    Component.BASE,
    Component.STATS,
    Component.ODDS,
    Component.LINEUPS,
    Component.INCIDENTS,
]

base_one = queue_list_of_seasons(
    season_tournament_list=list_season_ids_l1,
    target_components=target_components,
    pipeline=pipeline,
)

base_two = queue_list_of_seasons(
    season_tournament_list=list_season_ids_l2,
    target_components=target_components,
    pipeline=pipeline,
)

pipeline.run_worker_loop(
    max_workers=config.pipeline.max_workers,
    task_limit=5000,  # <-- Let it run until it's finished!
)
