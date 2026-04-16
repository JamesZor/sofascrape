import logging

from sofascrape.conf.config import load_config
from sofascrape.db.manager import DatabaseManager
from sofascrape.pipeline.orchestrator import Orchestrator

logging.basicConfig(level=logging.WARNING, force=True)
logging.getLogger("sofascrape").setLevel(logging.WARNING)


config = load_config()
db = DatabaseManager(config)
pipeline = Orchestrator(db, config)


pipeline.retry_failed_components()
pipeline.run_worker_loop(
    max_workers=2, task_limit=2000  # Just need 1 or 2 workers for a quick cleanup
)
