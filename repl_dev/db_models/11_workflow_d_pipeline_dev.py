from webdriver import ManagerWebdriver

from sofascrape.conf.config import load_config
from sofascrape.db.manager import DatabaseManager
from sofascrape.pipeline.orchestrator import Orchestrator

config = load_config()
db = DatabaseManager(config)
pipeline = Orchestrator(db, config)


mw = ManagerWebdriver()
driver = mw.spawn_webdriver()


tasks = db.get_pending_tasks(limit=3)


# Unleash the threads!
pipeline.run_worker_loop()
