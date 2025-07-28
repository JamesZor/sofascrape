import pickle
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from sqlmodel import Field, Session, SQLModel, create_engine, select

from sofascrape.general import TournamentProcessScraper


class Tournament(SQLModel, table=True):
    """Simple tournament table with all info flattened"""

    __tablename__ = "tournaments"

    # Tournament info
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    slug: str

    # Category info (flattened)
    category_id: int
    category_name: str
    category_slug: str

    # Sport info (flattened)
    sport_id: int
    sport_name: str
    sport_slug: str

    # Timestamps
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


# Cell 4: Helper Function to Convert Your Data
def convert_tournament_data_to_sqlmodel(tournament_data):
    """Convert your TournamentData to SQLModel Tournament"""
    if tournament_data is None:
        return None

    # Check if the scraper has valid data
    if tournament_data.data is None:
        print(
            f"⚠️  Skipping tournament {tournament_data.tournamentid} - no data scraped"
        )
        return None

    t = tournament_data.data.tournament
    print(f"t: {repr(t)}, type {type(t)}")
    return Tournament(
        id=t.id,
        name=t.name,
        slug=t.slug,
        category_id=t.category.id,
        category_name=t.category.name,
        category_slug=t.category.slug,
        sport_id=t.category.sport.id,
        sport_name=t.category.sport.name,
        sport_slug=t.category.sport.slug,
    )


# Cell 5: Insert Your Tournament Data
def save_tournaments_to_db(tournament_data_list):
    """Save your scraped tournaments to database"""
    with Session(engine) as session:
        saved_count = 0

        for tournament_data in tournament_data_list:
            if tournament_data is None:
                continue

            # Convert to SQLModel

            tournament = convert_tournament_data_to_sqlmodel(tournament_data)

            # Check if already exists
            existing = session.get(Tournament, tournament.id)

            if existing:
                # Update existing
                existing.name = tournament.name
                existing.slug = tournament.slug
                existing.category_name = tournament.category_name
                existing.category_slug = tournament.category_slug
                existing.sport_name = tournament.sport_name
                existing.sport_slug = tournament.sport_slug
                existing.updated_at = datetime.utcnow()
                print(f"📝 Updated: {tournament.name}")
            else:
                # Add new
                session.add(tournament)
                print(f"➕ Added: {tournament.name}")

            saved_count += 1

        session.commit()
        print(f"\n✅ Saved {saved_count} tournaments to database!")


# Create a data directory if it doesn't exist
data_dir = Path("tournament_data")


# Option 1: Using pickle (built-in Python)
def save_tournaments_pickle(tournaments_details, filename="tournaments_data.pkl"):
    """Save tournament data using pickle"""
    filepath = data_dir / filename

    with open(filepath, "wb") as f:
        pickle.dump(tournaments_details, f)

    print(f"✅ Saved tournament data to {filepath}")
    print(
        f"📊 Saved {len([t for t in tournaments_details.tournaments if t is not None])} tournaments"
    )


def load_tournaments_pickle(filename="tournaments_data.pkl"):
    """Load tournament data using pickle"""
    filepath = data_dir / filename

    if not filepath.exists():
        print(f"❌ File {filepath} not found!")
        return None

    with open(filepath, "rb") as f:
        tournaments_details = pickle.load(f)

    print(f"✅ Loaded tournament data from {filepath}")
    print(
        f"📊 Loaded {len([t for t in tournaments_details.tournaments if t is not None])} tournaments"
    )

    return tournaments_details


##############################

DATABASE_URL = "postgresql://test_user:test@localhost:5432/tournament_db"
engine = create_engine(DATABASE_URL)
SQLModel.metadata.create_all(engine)

# tournaments_details = TournamentProcessScraper()
# tournaments_details.process()

# save_tournaments_pickle(tournaments_details)  # Auto-generates filename with timestamp

tournaments_details = load_tournaments_pickle("tournaments_data.pkl")

for t in tournaments_details.tournaments:
    if t is not None:
        print(t.data)
tournaments_details = [x for x in tournaments_details.tournaments if x is not None]
tournaments_details


t0 = tournaments_details.tournament[0]
print(t0)
sql1 = convert_tournament_data_to_sqlmodel(t0.data.tournament)
print(sql1)

with Session(engine) as session:
    existing = session.get(Tournament, sql1.id)
    print(existing)
    session.add(sql1)

    session.commit()
save_tournaments_to_db(tournaments_details[0:3])


# save_tournaments_to_db(tournaments_details.tournaments)
#
#
def show_tournaments():
    """Show some tournaments from database"""
    with Session(engine) as session:
        # Get first 10 tournaments
        tournaments = session.exec(select(Tournament).limit(10)).all()

        print("🏆 Tournaments in Database:")
        print("-" * 80)
        for t in tournaments:
            print(f"ID: {t.id:3d} | {t.name:30s} | {t.sport_name}")

        # Count by sport
        print(f"\n📊 Total tournaments: {len(session.exec(select(Tournament)).all())}")


# Run this to see your data
show_tournaments()
