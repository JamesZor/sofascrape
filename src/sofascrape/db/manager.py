# src/sofascrape/db/models.py

import datetime
import logging
from typing import Any, List, Optional

from sqlalchemy import create_engine, delete, select, update
from sqlalchemy.orm import Session, sessionmaker

from sofascrape.conf.config import AppConfig
from sofascrape.db.models import (
    Events,
    Match,
    MatchComponentAudit,
    MatchGraph,
    MatchIncidents,
    MatchOdd,
    MatchPlayerLineup,
    MatchStatistic,
    Season,
    Tournament,
)
from sofascrape.schemas.general import (
    EventSchema,
    FootballEventSchema,
    FootballGraphSchema,
    FootballIncidentsSchema,
    FootballLineupSchema,
    FootballStatsSchema,
    SeasonSchema,
    TeamLineupSchema,
)
from sofascrape.schemas.odds import OddsSchema

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

        # HACK: 2026-04-07 -
        # 1. The Registry (Dictionary Dispatch)
        self._component_savers = {
            "base": lambda mid, parsed, raw: self.upsert_match(mid, parsed, raw or {}),
            "stats": lambda mid, parsed, raw: self.upsert_match_statistics(mid, parsed),
            "lineups": lambda mid, parsed, raw: self.upsert_match_lineup(mid, parsed),
            "incidents": lambda mid, parsed, raw: self.upsert_match_incident(
                mid, parsed
            ),
            "graph": lambda mid, parsed, raw: self.upsert_match_graph(mid, parsed),
            "odds": lambda mid, parsed, raw: self.upsert_match_odds(
                mid, parsed, raw or []
            ),
        }

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
                    match_id=parsed_event.id,
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

    def upsert_match(
        self, match_id: int, parsed_match: FootballEventSchema, raw: dict
    ) -> None:
        """Upserts a Match into the database."""

        with self.SessionLocal() as session:
            event = getattr(parsed_match, "event", parsed_match)

            match_record = Match(
                match_id=match_id,
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

    def upsert_match_incident(
        self, match_id: int, parsed_incidents: FootballIncidentsSchema
    ) -> None:
        """Saves all incidents for a match using a clean Wipe & Replace strategy."""

        with self.SessionLocal() as session:
            # 1. Clear existing incidents for this match to avoid duplicates on re-runs
            session.execute(
                delete(MatchIncidents).where(MatchIncidents.match_id == match_id)
            )

            incident_records = []

            # Use getattr safely just in case the payload is empty
            incidents_list = getattr(parsed_incidents, "incidents", [])

            for inc in incidents_list:
                record = MatchIncidents(
                    match_id=match_id,
                    incident_type=getattr(inc, "incidentType", "unknown"),
                    time=safe_get(inc, "time", default=0),
                    added_time=safe_get(inc, "addedTime", default=0),
                    is_home=safe_get(inc, "isHome", default=False),
                    data=inc.to_sql_dict(),
                )
                incident_records.append(record)

            # 3. Bulk insert the freshly parsed records
            if incident_records:
                session.add_all(incident_records)

            session.commit()

    def upsert_match_statistics(
        self, match_id: int, parsed_stats: FootballStatsSchema
    ) -> None:
        """Flattens nested statistics and saves them using Wipe & Replace."""

        with self.SessionLocal() as session:
            # 1. Wipe existing stats for this match to prevent duplicates or orphaned rows
            session.execute(
                delete(MatchStatistic).where(MatchStatistic.match_id == match_id)
            )

            stat_records = []

            # Use getattr safely just in case the payload is empty
            periods_list = getattr(parsed_stats, "statistics", [])

            # 2. Triple-loop to flatten the data: Period -> Group -> Item
            for period_data in periods_list:
                period_name = period_data.period

                for group_data in period_data.groups:
                    group_name = group_data.groupName

                    for item in group_data.statisticsItems:
                        record = MatchStatistic(
                            match_id=match_id,
                            period=period_name,
                            group_name=group_name,
                            stat_key=item.key,
                            stat_name=item.name,
                            home_display=item.home,
                            away_display=item.away,
                            home_value=item.homeValue,
                            away_value=item.awayValue,
                            home_total=item.homeTotal,
                            away_total=item.awayTotal,
                            compare_code=item.compareCode,
                            statistics_type=item.statisticsType,
                            value_type=item.valueType,
                            render_type=item.renderType,
                        )
                        stat_records.append(record)

            # 3. Bulk insert all the flattened stats
            if stat_records:
                session.add_all(stat_records)

            session.commit()

    def upsert_match_graph(
        self, match_id: int, parsed_graph: FootballGraphSchema
    ) -> None:
        """Upserts the entire momentum graph payload into a single JSONB column."""

        with self.SessionLocal() as session:
            # 1. Safely extract the points list
            raw_points = safe_get(parsed_graph, "graphPoints") or []

            # 2. Convert the list of Pydantic models into a list of plain dictionaries!
            points_dict_list = (
                [
                    # Use your custom to_sql_dict() if you have it, or standard Pydantic dumps
                    (
                        p.to_sql_dict()
                        if hasattr(p, "to_sql_dict")
                        else (
                            p.model_dump(mode="json")
                            if hasattr(p, "model_dump")
                            else p.dict()
                        )
                    )
                    for p in raw_points
                ]
                if raw_points
                else None
            )

            # 3. Create the DB record
            record = MatchGraph(
                match_id=match_id,
                period_time=safe_get(parsed_graph, "periodTime", default=0),
                overtime_length=safe_get(parsed_graph, "overtimeLength", default=0),
                period_count=safe_get(parsed_graph, "periodCount", default=0),
                points=points_dict_list,  # Pass the pure Python dicts, not Pydantic models!
            )

            # session.merge will cleanly Insert if new, or Update if the match_id exists
            session.merge(record)
            session.commit()

    def upsert_match_odds(
        self, match_id: int, parsed_odds_markets: OddsSchema, raw_odds: list[dict]
    ) -> None:
        """Saves match betting odds using a Wipe & Replace strategy."""

        with self.SessionLocal() as session:
            # 1. Wipe existing odds for this match to avoid duplicates on re-runs
            session.execute(delete(MatchOdd).where(MatchOdd.match_id == match_id))

            odd_records = []

            # 2. Loop through markets and their respective choices
            for market, raw_market in zip(parsed_odds_markets, raw_odds):
                # Primary Keys cannot be NULL, so fallback to "default" if choice_group is missing
                c_group = market.choice_group or "default"

                for choice in market.choices:
                    record = MatchOdd(
                        match_id=match_id,
                        market_id=market.market_id,
                        choice_group=c_group,
                        choice_name=choice.name,
                        market_name=market.market_name,
                        is_live=market.is_live,
                        suspended=market.suspended,
                        # Unpack the Tuples created by your Pydantic validator
                        initial_fraction_num=choice.initial_fractional_value[0],
                        initial_fraction_den=choice.initial_fractional_value[1],
                        fraction_num=choice.fractional_value[0],
                        fraction_den=choice.fractional_value[1],
                        winning=choice.winning,
                        change=choice.change,
                        raw_data=raw_market,
                    )
                    odd_records.append(record)

            # 3. Bulk insert the freshly parsed records
            if odd_records:
                session.add_all(odd_records)

            session.commit()

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

    def save_component_data(
        self, match_id: int, component_name: str, parsed_data: Any, raw_data: Any = None
    ) -> None:
        """Routes the Pydantic model to the specific relational upsert method."""

        # 2. Grab the function from the dictionary
        save_func = self._component_savers.get(component_name)

        if not save_func:
            raise ValueError(f"Unknown component: {component_name}")

        # 3. Execute the function
        save_func(match_id, parsed_data, raw_data)

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
