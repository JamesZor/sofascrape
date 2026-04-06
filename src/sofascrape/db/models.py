# src/sofascrape/db/models.py

from datetime import datetime

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
    id = Column(Integer, primary_key=True)
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
    match_id = Column(Integer, ForeignKey("matches.match_id"), nullable=False)
    component_name = Column(
        String, nullable=False
    )  # 'base', 'stats', 'lineups', 'odds'
    status = Column(String, nullable=False, default="PENDING")
    retry_count = Column(Integer, default=0)
    last_attempted_at = Column(DateTime, default=datetime.utcnow)
    error_message = Column(Text, nullable=True)


class Match(Base):
    __tablename__ = "matches"
    match_id = Column(Integer, primary_key=True)
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


class MatchStats(Base):
    __tablename__ = "match_stats"
    match_id = Column(Integer, ForeignKey("matches.match_id"), primary_key=True)
    data = Column(JSONB)


class MatchPlayerLineup(Base):
    __tablename__ = "match_player_lineups"

    # Composite Primary Key: A specific player in a specific match
    match_id = Column(Integer, ForeignKey("matches.match_id"), primary_key=True)
    player_id = Column(Integer, primary_key=True)

    # Team Info
    team_id = Column(Integer, nullable=False)
    is_home_team = Column(Boolean, nullable=False)

    # Player Info
    player_name = Column(String, nullable=False)
    player_slug = Column(String)
    position = Column(String)
    shirt_number = Column(Integer)
    jersey_number = Column(String)

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
    match_id = Column(Integer, ForeignKey("matches.match_id"), nullable=False)
    # 'card', 'goal', 'substitution', 'period'
    incident_type = Column(String, nullable=False)
    time = Column(Integer)
    added_time = Column(Integer)
    is_home = Column(Boolean)
    data = Column(JSONB)
