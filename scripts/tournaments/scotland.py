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


eliteserien_tour_id = 54
division_1st_tour_id = 55


# pipeline.setup_tournament(eliteserien_tour_id)
# pipeline.setup_tournament(division_1st_tour_id)
#
# pipeline.setup_tournament(eliteserien_tour_id)
# pipeline.setup_tournament(division_1st_tour_id)
# #
#
# list_season_ids_l1 = get_seasonid_year_from_tournament(
#     pipeline=pipeline, tournament_id=eliteserien_tour_id, result_limit=6
# )
#
# list_season_ids_l2 = get_seasonid_year_from_tournament(
#     pipeline=pipeline, tournament_id=division_1st_tour_id, result_limit=6
# )
#
#
# target_components = [
#     Component.BASE,
#     Component.STATS,
#     Component.ODDS,
#     Component.LINEUPS,
#     Component.INCIDENTS,
# ]
#
# base_one = queue_list_of_seasons(
#     season_tournament_list=list_season_ids_l1,
#     target_components=target_components,
#     pipeline=pipeline,
# )
#
# base_two = queue_list_of_seasons(
#     season_tournament_list=list_season_ids_l2,
#     target_components=target_components,
#     pipeline=pipeline,
# )
#
pipeline.run_worker_loop(
    max_workers=config.pipeline.max_workers,
    task_limit=6000,  # <-- Let it run until it's finished!
)


pipeline.retry_failed_components()
pipeline.run_worker_loop(
    max_workers=2, task_limit=2000  # Just need 1 or 2 workers for a quick cleanup
)
