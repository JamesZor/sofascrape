import logging

from sqlalchemy import select
from sqlalchemy.orm import Session

from sofascrape.conf.config import load_config
from sofascrape.db.manager import DatabaseManager
from sofascrape.db.models import Component, Events, MatchComponentAudit, Season
from sofascrape.pipeline.orchestrator import Orchestrator

logging.basicConfig(level=logging.WARNING, force=True)
logging.getLogger("sofascrape").setLevel(logging.WARNING)

config = load_config()
db = DatabaseManager(config)
pipeline = Orchestrator(db, config)


# ********************
# --- Init tournaments
# ********************

pipeline.setup_tournament(3284)  # K league one
pipeline.setup_tournament(6230)  # K league two


# --- get season ids for a tournament


def get_seasonid_year_from_tournament(
    tournament_id: int, result_limit: int = 3
) -> tuple[int, int]:
    """
    returns a list [ (<season_id, tournament_id>) ]
    orded desc by date
    """
    with pipeline.db.SessionLocal() as session:
        stmt = (
            select(
                Season.season_id,
                Season.tournament_id,
            )
            .where(Season.tournament_id == tournament_id)
            .order_by(Season.year.desc())
            .limit(result_limit)
        )

        seasons = session.execute(stmt).all()

        if not seasons:
            print(f"No seasons can befound for {tournament_id}.")

        return seasons


def queue_list_of_seasons(
    season_tournament_list: list[int, int],
    target_components: list,
    pipeline: Orchestrator,
) -> int:

    print("📥 Building the historical backfill queue...")
    total_queued = 0

    for season_id, tournament_id in season_tournament_list:
        print(f"\n--- Processing Tournament {tournament_id} | Season {season_id} ---")

        # 1. Update the Events table first!
        pipeline.sync_events(tournament_id=tournament_id, season_id=season_id)

        # 2. Now that the DB knows about the matches, queue up the components!
        queued_dict = pipeline.queue_season_missing_components(
            season_id=season_id, components=target_components
        )

        # Flatten the dict to get the int count and add it to our total
        season_total = sum(queued_dict.values())
        total_queued += season_total

        print(f" -> Season {season_id}: Queued {season_total} new tasks.")

    print(f"\n🚀 Total tasks queued: {total_queued}.")
    return total_queued


ses = get_seasonid_year_from_tournament(3284)
target_components = [Component.BASE]

target_components = [Component.STATS]

target_components = [Component.ODDS]


target_components = [Component.BASE, Component.STATS, Component.ODDS]

base_one = queue_list_of_seasons(
    season_tournament_list=ses, target_components=target_components, pipeline=pipeline
)

"""
    ran for: base. 
    Out[45]: [(88606, 3284), (70830, 3284), (57878, 3284)]
"""


pipeline.run_worker_loop(
    max_workers=config.pipeline.max_workers,
    task_limit=500,  # <-- Let it run until it's finished!
)


pipeline.run_worker_loop(
    max_workers=2, task_limit=20  # Just need 1 or 2 workers for a quick cleanup
)


ses
