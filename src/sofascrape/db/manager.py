import logging
from typing import Any, List, Optional

from sqlalchemy import create_engine, select, update
from sqlalchemy.orm import Session, sessionmaker

from sofascrape.conf.config import AppConfig
from sofascrape.db.models import (
    Match,
    MatchComponentAudit,
    MatchIncidents,
    MatchLineups,
    MatchStats,
    Tournament,
)

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, config: AppConfig):
        # Create the connection pool
        self.engine = create_engine(config.database.url)
        # Create a factory for generating database sessions
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

    def get_pending_tasks(self, limit: int = 50) -> List[MatchComponentAudit]:
        """Fetches a batch of pending or retryable tasks from the queue."""
        with self.SessionLocal() as session:
            # We want tasks that are PENDING, or QA_MISMATCH/API_ERROR that haven't hit the retry limit
            stmt = (
                select(MatchComponentAudit)
                .where(
                    MatchComponentAudit.status.in_(
                        ["PENDING", "QA_MISMATCH", "API_ERROR"]
                    ),
                    MatchComponentAudit.retry_count
                    < 3,  # Hardcoded for now, could use config
                )
                .limit(limit)
            )

            # session.scalars() unpacks the SQLAlchemy rows into actual Python objects
            results = session.scalars(stmt).all()
            return results

    def update_task_status(
        self,
        audit_id: int,
        status: str,
        error_message: Optional[str] = None,
        increment_retry: bool = False,
    ) -> None:
        """Updates the state of a specific task."""
        with self.SessionLocal() as session:
            task = session.get(MatchComponentAudit, audit_id)
            if not task:
                return

            task.status = status
            if error_message:
                task.error_message = error_message
            if increment_retry:
                task.retry_count += 1

            session.commit()

    def save_component_data(
        self, match_id: int, component_name: str, pydantic_data: Any
    ) -> None:
        """Saves the dumped Pydantic JSON to the correct table."""
        with self.SessionLocal() as session:
            # Pydantic v2 uses model_dump(mode='json'), v1 uses dict().
            # This ensures we get a JSON-safe dictionary.
            json_data = (
                pydantic_data.model_dump(mode="json")
                if hasattr(pydantic_data, "model_dump")
                else pydantic_data.dict()
            )

            if component_name == "stats":
                record = MatchStats(match_id=match_id, data=json_data)
            elif component_name == "lineups":
                record = MatchLineups(match_id=match_id, data=json_data)
            elif component_name == "incidents":
                record = MatchIncidents(match_id=match_id, data=json_data)
            else:
                raise ValueError(f"Unknown component: {component_name}")

            # 'merge' performs an UPSERT (inserts if new, updates if existing)
            session.merge(record)
            session.commit()

    # FIXME:
    def upsert_tournament(self, tournament_data: Any, raw_data: dict = None) -> None:
        """Upserts a Tournament into the database."""
        with self.SessionLocal() as session:
            # Assuming your Pydantic model has .name and .id or similar
            # Adjust these fields based on your actual TournamentData schema
            t = Tournament(
                tournament_id=tournament_data.tournament.id,
                name=tournament_data.tournament.name,
                sport=tournament_data.tournament.category.sport.slug,
                country=(
                    tournament_data.tournament.category.name
                    if hasattr(tournament_data.tournament, "category")
                    else None
                ),
                raw_data=raw_data,
            )
            session.merge(t)
            session.commit()


# %%
# --- IPython Playground ---
if __name__ == "__main__":
    from sofascrape.conf.config import load_config
    from sofascrape.db.manager import DatabaseManager
    from sofascrape.db.models import Match, MatchComponentAudit

    config = load_config()
    db = DatabaseManager(config)

    # 1. Let's manually create a fake match and a pending task to test the logic
    with db.SessionLocal() as session:
        # Make a dummy match
        dummy_match = Match(match_id=99999, home_team="Celtic", away_team="Rangers")
        session.merge(dummy_match)

        # Make a dummy task
        dummy_task = MatchComponentAudit(
            match_id=99999, component_name="stats", status="PENDING"
        )
        session.add(dummy_task)
        session.commit()

    # 2. Test our new manager method!
    tasks = db.get_pending_tasks(limit=5)
    for t in tasks:
        print(
            f"Found Task - ID: {t.audit_id}, Match: {t.match_id}, Component: {t.component_name}, Status: {t.status}"
        )

    # 3. Test updating the status
    if tasks:
        first_task = tasks[0]
        db.update_task_status(first_task.audit_id, status="SUCCESS")
        print(f"Updated task {first_task.audit_id} to SUCCESS!")

#
