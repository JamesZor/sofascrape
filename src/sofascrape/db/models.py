from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, Text
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
    tournament_id = Column(Integer, ForeignKey("tournaments.tournament_id"))
    season_id = Column(Integer, ForeignKey("seasons.season_id"))
    name = Column(String)
    status_code = Column(Integer)
    status_description = Column(String)
    status_type = Column(String)


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


# %%
# --- IPython Playground ---
if __name__ == "__main__":
    from sqlalchemy import create_engine

    from sofascrape.conf.config import load_config
    from sofascrape.db.models import Base

    # 1. Load the config we made in Phase 1
    config = load_config()

    # 2. Create the SQLAlchemy Engine (the actual connection to Postgres)
    # We set echo=True so you can watch it generate the raw SQL!
    engine = create_engine(config.database.url, echo=True)

    # 3. Build the schema
    print("Connecting to database and creating tables...")
    Base.metadata.create_all(engine)
    print("Schema creation complete!")

    # ----- Reset the tables, beca

    from sqlalchemy import create_engine

    from sofascrape.conf.config import load_config
    from sofascrape.db.models import Base

    config = load_config()
    engine = create_engine(config.database.url, echo=True)

    # 1. Drop EVERYTHING (Say goodbye to our dummy data!)
    Base.metadata.drop_all(engine)

    # 2. Recreate everything with the new raw_data column
    Base.metadata.create_all(engine)
    print("Database schema successfully recreated!")
