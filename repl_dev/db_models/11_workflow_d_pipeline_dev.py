import os

os.environ["DISPLAY"] = ":0"

from webdriver import ManagerWebdriver

from sofascrape.conf.config import load_config
from sofascrape.db.manager import DatabaseManager
from sofascrape.pipeline.orchestrator import Orchestrator

config = load_config()
db = DatabaseManager(config)
pipeline = Orchestrator(db, config)

driver = pipeline.mw.spawn_webdriver()


tasks = db.get_pending_tasks(limit=3)

pipeline._process_single_task(task=tasks[1], driver=driver)

# Unleash the threads!
pipeline.run_worker_loop(max_workers=4, task_limit=17)
