# src/sofascrape/db/models.py

from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
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


# TODO:
class Events(Base):
    __tablename__ = "Events"
    id = Column(Integer, primary_key=True)
    tournament_id = Column(Integer, ForeignKey("tournaments.tournament_id"))
    season_id = Column(Integer, ForeignKey("seasons.season_id"))
    name = Column(String)
    home_team = Column(String, nullable=False)
    away_team = Column(String, nullable=False)
    status_type = Column(String)
    start_timestamp = Column(Integer)
    injury_time1 = Column(Integer)
    injury_time2 = Column(Integer)
    home_score_ht = Column(Integer)
    home_score = Column(Integer)
    away_score_ht = Column(Integer)
    away_score = Column(Integer)
    round = Column(Integer)
    winner_code = Column(Integer)
    hasGlobalHighlights = Column(Boolean)
    hasXg = Column(Boolean)
    hasEventPlayerStatistics = Column(Boolean)
    hasEventPlayerHeatMap = Column(Boolean)


class TournamentSeasonMetadata(Base):
    __tablename__ = "tournament_season_metadata"
    tournament_id = Column(
        Integer, ForeignKey("tournaments.tournament_id"), primary_key=True
    )
    season_id = Column(Integer, ForeignKey("seasons.season_id"), primary_key=True)
    has_stats = Column(Boolean, default=True)
    has_incidents = Column(Boolean, default=True)
    has_lineups = Column(Boolean, default=True)


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


# --- The Golden Data Sink ---


class Match(Base):
    __tablename__ = "matches"
    match_id = Column(Integer, primary_key=True)
    season_id = Column(Integer, ForeignKey("seasons.season_id"))
    start_timestamp = Column(DateTime)
    home_team = Column(String)
    away_team = Column(String)
    home_score = Column(Integer)
    away_score = Column(Integer)


class MatchStats(Base):
    __tablename__ = "match_stats"
    match_id = Column(Integer, ForeignKey("matches.match_id"), primary_key=True)
    data = Column(JSONB)  # This is where your Pydantic model gets dumped!


class MatchLineups(Base):
    __tablename__ = "match_lineups"
    match_id = Column(Integer, ForeignKey("matches.match_id"), primary_key=True)
    data = Column(JSONB)


class MatchIncidents(Base):
    __tablename__ = "match_incidents"
    match_id = Column(Integer, ForeignKey("matches.match_id"), primary_key=True)
    data = Column(JSONB)
