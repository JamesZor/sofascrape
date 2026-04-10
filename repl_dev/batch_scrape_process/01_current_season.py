import os

os.environ["DISPLAY"] = ":0"


from sqlalchemy import select

from sofascrape.conf.config import load_config
from sofascrape.db.manager import DatabaseManager
from sofascrape.db.models import Component, Events, MatchComponentAudit
from sofascrape.pipeline.orchestrator import Orchestrator

config = load_config()
db = DatabaseManager(config)
pipeline = Orchestrator(db, config)


TOURNAMENT_ID = 56  # scottish league two
TARGET_SEASON_ID = 77129  # season 25/26


target_components: list[Component] = [Component.BASE]


results_queue = pipeline.queue_season_missing_components(
    season_id=TARGET_SEASON_ID, components=target_components
)

pipeline.run_worker_loop(
    max_workers=config.pipeline.max_workers, task_limit=config.pipeline.batch_size
)

pipeline.sync_season(
    tournament_id=TOURNAMENT_ID,
    season_id=TARGET_SEASON_ID,
    components=target_components,
)

# ----

import logging
import os

# 1. Set the global logging level to WARNING (Hides INFO and DEBUG)
# Adding force=True ensures it overrides any logging setups hidden inside imported libraries
logging.basicConfig(level=logging.WARNING, force=True)

# 2. (Optional) Force your specific package to be quiet just in case
logging.getLogger("sofascrape").setLevel(logging.WARNING)

os.environ["DISPLAY"] = ":0"

from sqlalchemy import select

from sofascrape.conf.config import load_config
from sofascrape.db.manager import DatabaseManager
from sofascrape.db.models import Component, Events, MatchComponentAudit
from sofascrape.pipeline.orchestrator import Orchestrator

config = load_config()
db = DatabaseManager(config)
pipeline = Orchestrator(db, config)

TOURNAMENT_ID = 56  # scottish league one
TARGET_SEASON_ID = 77129  # season 25/26


target_components: list[Component] = [Component.BASE]

# 3. Fire it up!
results_queue = pipeline.queue_season_missing_components(
    season_id=TARGET_SEASON_ID, components=target_components
)

pipeline.run_worker_loop(
    max_workers=config.pipeline.max_workers, task_limit=config.pipeline.batch_size
)


TOURNAMENT_ID = 57  # scottish league two
TARGET_SEASON_ID = 77045  # season 25/26


results_queue = pipeline.queue_season_missing_components(
    season_id=TARGET_SEASON_ID, components=target_components
)

pipeline.run_worker_loop(
    max_workers=config.pipeline.max_workers, task_limit=config.pipeline.batch_size
)


"""

 season_id | tournament_id | year  |         name         |
-----------+---------------+-------+----------------------+
     77045 |            57 | 25/26 | League 2 25/26       |
     62487 |            57 | 24/25 | League 2 24/25       |
     52604 |            57 | 23/24 | League 2 23/24       |
     41960 |            57 | 22/23 | League Two 22/23     |
     37032 |            57 | 21/22 | League Two 21/22     |
     29269 |            57 | 20/21 | League Two 20/21     |

 season_id | tournament_id | year  |         name          |
-----------+---------------+-------+-----------------------+
     77129 |            56 | 25/26 | League 1 25/26        |
     62416 |            56 | 24/25 | League 1 24/25        |
     52605 |            56 | 23/24 | League 1 23/24        |
     41959 |            56 | 22/23 | League One 22/23      |
     37031 |            56 | 21/22 | League One 21/22      |
     29270 |            56 | 20/21 | League One 20/21      |
"""
import logging
import os

from sqlalchemy import select

from sofascrape.conf.config import load_config
from sofascrape.db.manager import DatabaseManager
from sofascrape.db.models import Component
from sofascrape.pipeline.orchestrator import Orchestrator

# Keep the logs quiet so we can see the progress bars
logging.basicConfig(level=logging.WARNING, force=True)
logging.getLogger("sofascrape").setLevel(logging.WARNING)
os.environ["DISPLAY"] = ":0"

# --- Setup ---
config = load_config()
db = DatabaseManager(config)
pipeline = Orchestrator(db, config)

target_components = [Component.BASE]

target_components = [Component.ODDS]

target_components = [Component.INCIDENTS]

# Create a list of tuples containing (tournament_id, season_id)
# This ensures the API gets the exact right combination every time!
historical_targets = [
    # League One (Tournament 56)
    (56, 77129),
    (56, 62416),
    (56, 52605),
    (56, 41959),
    (56, 37031),
    (56, 29270),
    # League Two (Tournament 57)
    (57, 77045),
    (57, 62487),
    (57, 52604),
    (57, 41960),
    (57, 37032),
    (57, 29269),
]

print("📥 Building the historical backfill queue...")
total_queued = 0

for tournament_id, season_id in historical_targets:
    print(f"\n--- Processing Tournament {tournament_id} | Season {season_id} ---")

    # insert_pipeline.test_events(target_id=tournament_id, season_id=season_id)

    # 1. Update the Events table first!
    # This hits the API and upserts all 200+ matches for the season into the DB.
    pipeline.sync_events(tournament_id=tournament_id, season_id=season_id)

    # 2. Now that the DB knows about the matches, queue up the components!
    queued_dict = pipeline.queue_season_missing_components(
        season_id=season_id, components=target_components
    )

    # Flatten the dict to get the int count and add it to our total
    season_total = sum(queued_dict.values())
    total_queued += season_total

    print(f" -> Season {season_id}: Queued {season_total} new tasks.")

# --- 3. The Engine Run ---
if total_queued > 0:
    print(f"\n🚀 Total tasks queued: {total_queued}. Spinning up the worker pool...")
    pipeline.run_worker_loop(
        max_workers=config.pipeline.max_workers,
        task_limit=2000,  # <-- Let it run until it's finished!
    )
    print("\n✅ Historical backfill complete!")
else:
    print("\n✅ Database is already fully populated for these seasons.")


# 1. Sweep up the 6 QA_MISMATCH errors (Sets them back to PENDING)
pipeline.retry_failed_components()

# 2. Run the engine to fetch those last 6 matches
pipeline.run_worker_loop(
    max_workers=2, task_limit=10  # Just need 1 or 2 workers for a quick cleanup
)
