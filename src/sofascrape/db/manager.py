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
    MatchPlayerLineup,
    MatchStats,
    Season,
    Tournament,
)
from sofascrape.schemas.general import (
    EventSchema,
    FootballEventSchema,
    FootballLineupSchema,
    MatchPlayerLineup,
    SeasonSchema,
    TeamLineupSchema,
)

logger = logging.getLogger(__name__)

# --- Helper functions ---


def safe_get(obj, *attrs, default=None):
    """Safely fetch nested attributes without throwing NoneType errors."""
    for attr in attrs:
        if obj is None:
            return default
        # Move one level deeper
        obj = getattr(obj, attr, default)
    return obj


def parse_unix_timestamp(ts: Optional[int]) -> Optional[datetime.datetime]:
    """Converts a Unix epoch integer to a timezone-aware UTC datetime."""
    if not ts:
        return None
    return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)


# --- Main ----


class DatabaseManager:
    def __init__(self, config: AppConfig):
        # Create the connection pool
        self.engine = create_engine(config.database.url)
        # Create a factory for generating database sessions
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )

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

    # TODO::
    def upsert_match(
        self, match_id: int, parsed_match: FootballEventSchema, raw: dict
    ) -> None:
        """Upserts a Match into the database."""

        with self.SessionLocal() as session:
            event = getattr(parsed_match, "event", parsed_match)

            match_record = Match(
                id=match_id,
                tournament_id=safe_get(event, "tournament", "id"),
                season_id=safe_get(event, "season", "id"),
                name=safe_get(event, "slug"),
                # Wrapped safely in our new DRY helper!
                start_timestamp=parse_unix_timestamp(safe_get(event, "startTimestamp")),
                home_team=safe_get(event, "homeTeam", "slug"),
                away_team=safe_get(event, "awayTeam", "slug"),
                status_type=safe_get(event, "status", "type"),
                # Time / Score logic
                injury_time1=safe_get(event, "time", "injuryTime1", default=0),
                injury_time2=safe_get(event, "time", "injuryTime2", default=0),
                home_score_ht=safe_get(event, "homeScore", "period1"),
                home_score=safe_get(event, "homeScore", "current"),
                away_score_ht=safe_get(event, "awayScore", "period1"),
                away_score=safe_get(event, "awayScore", "current"),
                # Match Info
                round=safe_get(event, "roundInfo", "round"),
                winner_code=safe_get(event, "winnerCode"),
                # Extracted Nested Details
                mananger_home=safe_get(event, "homeTeam", "manager", "name"),
                mananger_away=safe_get(event, "awayTeam", "manager", "name"),
                venue_name_home=safe_get(event, "homeTeam", "venue", "name"),
                venue_name_away=safe_get(event, "awayTeam", "venue", "name"),
                venue_city_home=safe_get(event, "homeTeam", "venue", "city", "name"),
                venue_city_away=safe_get(event, "awayTeam", "venue", "city", "name"),
                raw_data=raw,
            )

            session.merge(match_record)
            session.commit()

    def upsert_match_lineup(
        self, match_id: int, parsed_lineup: FootballLineupSchema
    ) -> None:
        """Upserts all players from a match lineup into the database."""

        with self.SessionLocal() as session:

            def process_team(team_data: TeamLineupSchema, is_home: bool):
                """Helper to iterate through a team's players and stage them for upsert."""
                # Use getattr safely in case team_data is None or missing players
                players = getattr(team_data, "players", []) if team_data else []

                for entry in players:
                    player_info = entry.player
                    stats = entry.statistics

                    # Dump the specific stats payload to a dict for the JSONB column
                    stats_dict = {}
                    if stats:
                        stats_dict = (
                            stats.model_dump(mode="json", exclude_none=True)
                            if hasattr(stats, "model_dump")
                            else stats.dict(exclude_none=True)
                        )

                    record = MatchPlayerLineup(
                        match_id=match_id,
                        player_id=player_info.id,
                        team_id=entry.teamId,
                        is_home_team=is_home,
                        player_name=player_info.name,
                        player_slug=player_info.slug,
                        position=entry.position or player_info.position,
                        shirt_number=entry.shirtNumber,
                        jersey_number=entry.jerseyNumber or player_info.jerseyNumber,
                        substitute=entry.substitute,
                        captain=entry.captain,
                        # Core stats pulled to top-level columns
                        minutes_played=safe_get(stats, "minutesPlayed", default=0),
                        rating=safe_get(stats, "rating"),
                        goals=safe_get(stats, "goals", default=0),
                        expected_goals=safe_get(stats, "expectedGoals"),
                        expected_assists=safe_get(stats, "expectedAssists"),
                        # The rest of the stats go here
                        statistics=stats_dict,
                    )

                    # Merge (Upsert) the specific player row
                    session.merge(record)

            # 1. Process Home Team
            process_team(parsed_lineup.home, is_home=True)

            # 2. Process Away Team
            process_team(parsed_lineup.away, is_home=False)

            # 3. Commit the transaction for the entire match lineup
            session.commit()

    # def get_pending_tasks(self, limit: int = 50) -> List[MatchComponentAudit]:
    #     """Fetches a batch of pending or retryable tasks from the queue."""
    #     with self.SessionLocal() as session:
    #         # We want tasks that are PENDING, or QA_MISMATCH/API_ERROR that haven't hit the retry limit
    #         stmt = (
    #             select(MatchComponentAudit)
    #             .where(
    #                 MatchComponentAudit.status.in_(
    #                     ["PENDING", "QA_MISMATCH", "API_ERROR"]
    #                 ),
    #                 MatchComponentAudit.retry_count
    #                 < 3,  # Hardcoded for now, could use config
    #             )
    #             .limit(limit)
    #         )
    #
    #         # session.scalars() unpacks the SQLAlchemy rows into actual Python objects
    #         results = session.scalars(stmt).all()
    #         return results
    #
    # def update_task_status(
    #     self,
    #     audit_id: int,
    #     status: str,
    #     error_message: Optional[str] = None,
    #     increment_retry: bool = False,
    # ) -> None:
    #     """Updates the state of a specific task."""
    #     with self.SessionLocal() as session:
    #         task = session.get(MatchComponentAudit, audit_id)
    #         if not task:
    #             return
    #
    #         task.status = status
    #         if error_message:
    #             task.error_message = error_message
    #         if increment_retry:
    #             task.retry_count += 1
    #
    #         session.commit()
    #
    # def save_component_data(
    #     self, match_id: int, component_name: str, pydantic_data: Any
    # ) -> None:
    #     """Saves the dumped Pydantic JSON to the correct table."""
    #     with self.SessionLocal() as session:
    #         # Pydantic v2 uses model_dump(mode='json'), v1 uses dict().
    #         # This ensures we get a JSON-safe dictionary.
    #         json_data = (
    #             pydantic_data.model_dump(mode="json")
    #             if hasattr(pydantic_data, "model_dump")
    #             else pydantic_data.dict()
    #         )
    #
    #         if component_name == "stats":
    #             record = MatchStats(match_id=match_id, data=json_data)
    #         elif component_name == "lineups":
    #             record = MatchLineups(match_id=match_id, data=json_data)
    #         elif component_name == "incidents":
    #             record = MatchIncidents(match_id=match_id, data=json_data)
    #         else:
    #             raise ValueError(f"Unknown component: {component_name}")
    #
    #         # 'merge' performs an UPSERT (inserts if new, updates if existing)
    #         session.merge(record)
    #         session.commit()
    #
