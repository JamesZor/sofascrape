import logging

from sofascrape.conf.config import load_config
from sofascrape.db.manager import DatabaseManager
from sofascrape.db.models import Component, Events, MatchComponentAudit
from sofascrape.pipeline.orchestrator import Orchestrator

logging.basicConfig(level=logging.WARNING, force=True)
logging.getLogger("sofascrape").setLevel(logging.WARNING)

config = load_config()
db = DatabaseManager(config)
pipeline = Orchestrator(db, config)


# --- Init tournaments

pipeline.setup_tournament(3281)  # K league one
pipeline.setup_tournament(6230)  # K league two
