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


# --- Nuclear options ---
