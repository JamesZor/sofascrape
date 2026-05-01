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

# ********************
# --- Init tournaments
# ********************
scotland_league_one = 56
scotland_league_two = 57


# scotland_pl = 54
# scotland_ch = 55
#

# pipeline.setup_tournament(scotland_league_one)
# pipeline.setup_tournament(scotland_league_two)


# scrape

list_season_ids_l1 = get_seasonid_year_from_tournament(
    scotland_league_one, result_limit=1
)
list_season_ids_l1


list_season_ids_l2 = get_seasonid_year_from_tournament(
    scotland_league_two, result_limit=1
)
list_season_ids_l2

target_components = [
    Component.BASE,
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

pipeline.retry_failed_components()
pipeline.run_worker_loop(
    max_workers=2, task_limit=200  # Just need 1 or 2 workers for a quick cleanup
)


season_1, l1_id = list_season_ids_l1[0]
season_2, l2_id = list_season_ids_l2[0]

pipeline.sync_events(tournament_id=l1_id, season_id=season_1)
pipeline.sync_events(tournament_id=l2_id, season_id=season_2)


# weekly update runner
pipeline.sync_season(
    tournament_id=l1_id,
    season_id=season_1,
    components=target_components,
)


pipeline.sync_season(
    tournament_id=l2_id,
    season_id=season_2,
    components=target_components,
)
