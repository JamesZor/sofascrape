import logging
from typing import Any, List, Optional

from sqlalchemy import create_engine, select, update
from sqlalchemy.orm import Session, sessionmaker

from sofascrape.conf.config import AppConfig
from sofascrape.db.models import (
    Events,
    Match,
    MatchComponentAudit,
    MatchIncidents,
    MatchLineups,
    MatchStats,
    Season,
    Tournament,
)
from sofascrape.schemas.general import EventSchema, SeasonSchema

logger = logging.getLogger(__name__)


def safe_get(obj, *attrs, default=None):
    """Safely fetch nested attributes without throwing NoneType errors."""
    for attr in attrs:
        if obj is None:
            return default
        # Move one level deeper
        obj = getattr(obj, attr, default)
    return obj


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

    def upsert_seasons(
        self, tournament_id: int, parsed_seasons: list[SeasonSchema], raw_season: dict
    ) -> None:
        """upserts seasons into the database"""

        with self.SessionLocal() as session:

            for parsed_season, raw_season_dict in zip(parsed_seasons, raw_season):

                s = Season(
                    season_id=parsed_season.id,
                    tournament_id=tournament_id,
                    year=parsed_season.year,
                    name=parsed_season.name,
                    raw_data=raw_season_dict,
                )

                session.merge(s)

            session.commit()

    def upsert_events(
        self,
        tournament_id: int,
        parsed_events: list[EventSchema],
        raw_event: list[dict],
    ) -> None:

        with self.SessionLocal() as session:
            for parsed_event, raw_event in zip(parsed_events, raw_event):
                ev = Events(
                    id=parsed_event.id,
                    tournament_id=tournament_id,
                    season_id=parsed_event.season.id,
                    name=parsed_event.slug,
                    round=parsed_event.roundInfo.round,
                    home_team=parsed_event.homeTeam.slug,
                    away_team=parsed_event.awayTeam.slug,
                    status_type=parsed_event.status.type,
                    start_timestamp=parsed_event.startTimestamp,
                    hasGlobalHighlights=parsed_event.hasGlobalHighlights,
                    hasXg=parsed_event.hasXg,
                    hasEventPlayerStatistics=parsed_event.hasEventPlayerStatistics,
                    hasEventPlayerHeatMap=parsed_event.hasEventPlayerStatistics,
                    raw_data=raw_event,
                )

                session.merge(ev)
            # finished the zip loop
            session.commit()
