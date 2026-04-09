# src/sofascrape/db/models.py

from datetime import datetime
from enum import StrEnum

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class EventsStatusTypes(StrEnum):
    POSTPONED = "postponed"
    NOTSTARTED = "notstarted"
    FINISHED = "finished"


class AuditStatusTypes(StrEnum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    UNAVAILABLE = "UNAVAILABLE"
    SKIPPED_MISSING = "SKIPPED_MISSING"


class Component(StrEnum):
    STATS = "stats"
    LINEUPS = "lineups"
    INCIDENTS = "incidents"
    ODDS = "odds"
    BASE = "base"
    GRAPH = "graph"


# --- Core Relational Tables ---


class Tournament(Base):
    __tablename__ = "tournaments"
    tournament_id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    sport = Column(String)
    country = Column(String)
    raw_data = Column(JSONB)


class Season(Base):
    __tablename__ = "seasons"
    season_id = Column(Integer, primary_key=True)
    tournament_id = Column(Integer, ForeignKey("tournaments.tournament_id"))
    year = Column(String, nullable=False)
    name = Column(String)
    raw_data = Column(JSONB)


class Events(Base):
    __tablename__ = "events"
    match_id = Column(Integer, primary_key=True)
    tournament_id = Column(Integer, ForeignKey("tournaments.tournament_id"))
    season_id = Column(Integer, ForeignKey("seasons.season_id"))
    name = Column(String)
    round = Column(Integer)
    home_team = Column(String, nullable=False)
    away_team = Column(String, nullable=False)
    status_type = Column(String)
    start_timestamp = Column(Integer)
    hasGlobalHighlights = Column(Boolean)
    hasXg = Column(Boolean)
    hasEventPlayerStatistics = Column(Boolean)
    hasEventPlayerHeatMap = Column(Boolean)
    raw_data = Column(JSONB)


# --- The State Machine (Queue) ---


class MatchComponentAudit(Base):
    __tablename__ = "match_component_audit"
    audit_id = Column(Integer, primary_key=True, autoincrement=True)
    match_id = Column(Integer, ForeignKey("events.match_id"), nullable=False)
    component_name = Column(
        String, nullable=False
    )  # 'base', 'stats', 'lineups', 'odds'
    status = Column(String, nullable=False, default="PENDING")
    retry_count = Column(Integer, default=0)
    last_attempted_at = Column(DateTime, default=datetime.utcnow)
    error_message = Column(Text, nullable=True)


class Match(Base):
    __tablename__ = "matches"
    match_id = Column(Integer, ForeignKey("events.match_id"), primary_key=True)
    tournament_id = Column(Integer, ForeignKey("tournaments.tournament_id"))
    season_id = Column(Integer, ForeignKey("seasons.season_id"))
    name = Column(String)
    start_timestamp = Column(DateTime(timezone=True))
    home_team = Column(String, nullable=False)
    away_team = Column(String, nullable=False)
    status_type = Column(String)
    injury_time1 = Column(Integer)
    injury_time2 = Column(Integer)
    home_score_ht = Column(Integer)
    home_score = Column(Integer)
    away_score_ht = Column(Integer)
    away_score = Column(Integer)
    round = Column(Integer)
    winner_code = Column(Integer)
    mananger_home = Column(String)
    mananger_away = Column(String)
    venue_name_home = Column(String)
    venue_name_away = Column(String)
    venue_city_home = Column(String)
    venue_city_away = Column(String)
    raw_data = Column(JSONB)


class MatchStatistic(Base):
    __tablename__ = "match_statistics"

    # Composite Primary Key guarantees no duplicate stats for a single match period
    match_id = Column(Integer, ForeignKey("events.match_id"), primary_key=True)
    period = Column(String, primary_key=True)  # e.g., 'ALL', '1ST', '2ND'
    group_name = Column(String, primary_key=True)  # e.g., 'Match overview', 'Shots'
    stat_key = Column(
        String, primary_key=True
    )  # e.g., 'ballPossession', 'expectedGoals'

    # Human-readable names and display strings
    stat_name = Column(
        String, nullable=False
    )  # e.g., 'Ball possession', 'Hit woodwork'
    home_display = Column(String)  # e.g., '55%', '114/147 (78%)'
    away_display = Column(String)  # e.g., '45%', '57/87 (66%)'

    # Pure numeric values for SQL math (AVERAGE, SUM, etc.)
    home_value = Column(Float)
    away_value = Column(Float)
    home_total = Column(
        Integer, nullable=True
    )  # Only present in renderType 3 (e.g. Passes)
    away_total = Column(Integer, nullable=True)  # Only present in renderType 3

    # Metadata for UI rendering
    compare_code = Column(Integer)
    statistics_type = Column(String)  # 'positive', 'negative'
    value_type = Column(String)  # 'event', 'team'
    render_type = Column(Integer)  # 1, 2, 3


class MatchPlayerLineup(Base):
    __tablename__ = "match_player_lineups"

    # Composite Primary Key: A specific player in a specific match
    match_id = Column(Integer, ForeignKey("events.match_id"), primary_key=True)
    player_id = Column(Integer, primary_key=True)

    # Team Info
    team_id = Column(Integer, nullable=False)
    is_home_team = Column(Boolean, nullable=False)

    # Player Info
    player_name = Column(String, nullable=False)
    player_slug = Column(String)
    position = Column(String)
    shirt_number = Column(Integer)

    # Match Status
    substitute = Column(Boolean)
    captain = Column(Boolean)

    minutes_played = Column(Integer)
    rating = Column(Float)
    goals = Column(Integer)
    expected_goals = Column(Float)
    expected_assists = Column(Float)

    # Detailed Stats (Keeps the table clean while retaining everything)
    statistics = Column(JSONB)


class MatchIncidents(Base):
    __tablename__ = "match_incidents"
    id = Column(Integer, primary_key=True, autoincrement=True)
    match_id = Column(Integer, ForeignKey("events.match_id"), nullable=False)
    # 'card', 'goal', 'substitution', 'period'
    incident_type = Column(String, nullable=False)
    time = Column(Integer)
    added_time = Column(Integer)
    is_home = Column(Boolean)
    data = Column(JSONB)


class MatchGraph(Base):
    __tablename__ = "match_graph"
    match_id = Column(Integer, ForeignKey("events.match_id"), primary_key=True)

    period_time = Column(Integer)  # Length of each period (usually 45 minutes)
    overtime_length = Column(Integer)  # Length of overtime periods (usually 15 minutes)
    period_count = Column(Integer)  # Number of periods (usually 2 for football)
    points = Column(JSONB)


class MatchOdd(Base):
    __tablename__ = "match_odds"

    # Composite Primary Key
    match_id = Column(Integer, ForeignKey("events.match_id"), primary_key=True)
    market_id = Column(Integer, primary_key=True)
    choice_group = Column(String, primary_key=True, default="default")
    choice_name = Column(String, primary_key=True)

    # Market Info
    market_name = Column(String, nullable=False)
    is_live = Column(Boolean)
    suspended = Column(Boolean)

    # Odds Values (Split into Numerator and Denominator)
    initial_fraction_num = Column(Integer)
    initial_fraction_den = Column(Integer)
    fraction_num = Column(Integer)
    fraction_den = Column(Integer)

    # Outcome
    winning = Column(Boolean, nullable=True)
    change = Column(Integer)

    raw_data = Column(JSONB)
