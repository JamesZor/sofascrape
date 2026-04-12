from sqlalchemy import select
from sqlalchemy.orm import Session

from sofascrape.conf.config import load_config
from sofascrape.db.manager import DatabaseManager
from sofascrape.db.models import Component, Events, MatchComponentAudit, Season
from sofascrape.pipeline.orchestrator import Orchestrator

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


ses = get_seasonid_year_from_tournament(3284)
