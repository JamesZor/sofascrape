# src/sofascrape/utils/scrap_tournament_script_helpers.py


from sqlalchemy import select

from sofascrape.db.models import Season
from sofascrape.pipeline.orchestrator import Orchestrator


def get_seasonid_year_from_tournament(
    pipeline: Orchestrator, tournament_id: int, result_limit: int = 3
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

    print("Building the historical backfill queue...")
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

    print(f"\nTotal tasks queued: {total_queued}.")
    return total_queued
