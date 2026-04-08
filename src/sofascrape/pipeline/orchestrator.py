import logging
import queue
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Type

# Import your heavy webdriver manager
from webdriver import ManagerWebdriver

from sofascrape.abstract.base import BaseComponentScraper
from sofascrape.conf.config import AppConfig
from sofascrape.db.manager import DatabaseManager
from sofascrape.db.models import MatchComponentAudit
from sofascrape.football.eventComponent import EventFootallComponentScraper
from sofascrape.football.graphComponent import FootballGraphComponentScraper
from sofascrape.football.incidentsComponent import FootballIncidentsComponentScraper
from sofascrape.football.lineupComonent import FootballLineupComponentScraper
from sofascrape.football.oddsComponent import OddsComponentScraper

# Import your Scraper Classes (Adjust paths if needed!)
from sofascrape.football.statsComponent import FootballStatsComponentScraper
from sofascrape.schemas.general import ConvertibleBaseModel

# Add your base match scraper and graph scraper here too when ready!

logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self, db: DatabaseManager, config: AppConfig):
        self.db = db
        self.config = config

        # We instantiate your proxy manager once for the whole pipeline
        self.mw = ManagerWebdriver()

        # --- THE DICTIONARY DISPATCH ---
        # This acts as our "Registry". If you ever add a new scraper (like 'weather'),
        # you just add one line here instead of writing massive if/else blocks!
        self._scraper_registry: Dict[str, Type] = {
            "stats": FootballStatsComponentScraper,
            "lineups": FootballLineupComponentScraper,
            "incidents": FootballIncidentsComponentScraper,
            "odds": OddsComponentScraper,
            "base": EventFootallComponentScraper,
            "graph": FootballGraphComponentScraper,
        }

    def _scraper_process(
        self, scraper: BaseComponentScraper
    ) -> tuple[ConvertibleBaseModel, dict]:
        """Runs the scraper and returns parsed_data and raw_data."""
        scraper.get_data()
        scraper.parse_data()
        parsed_data = scraper.data
        raw_data = getattr(scraper, "raw_data", None)
        return parsed_data, raw_data

    def _handle_qa_success(
        self,
        task: MatchComponentAudit,
        parsed_data: ConvertibleBaseModel,
        raw_data: dict,
    ) -> None:
        """Saves golden data to the database and marks task as SUCCESS."""
        self.db.save_component_data(
            match_id=task.match_id,
            component_name=task.component_name,
            parsed_data=parsed_data,
            raw_data=raw_data,
        )
        self.db.update_task_status(task.audit_id, status="SUCCESS")
        logger.info(f"✅ QA Passed for Match {task.match_id} ({task.component_name})!")

    def _handle_qa_mismatch(self, task: MatchComponentAudit) -> None:
        """Flags the task for retry due to data randomization."""
        logger.warning(
            f"❌ QA Mismatch for Match {task.match_id} ({task.component_name})."
        )
        self.db.update_task_status(
            task.audit_id,
            status="QA_MISMATCH",
            error_message="Fetch A did not match Fetch B",
            increment_retry=True,
        )

    def _handle_scraper_error(self, task: MatchComponentAudit, e: Exception) -> None:
        """Logs the exception and flags the task for retry."""
        logger.error(
            f"⚠️ Error on Match {task.match_id} ({task.component_name}): {str(e)}"
        )
        self.db.update_task_status(
            task.audit_id,
            status="API_ERROR",
            error_message=str(e),
            increment_retry=True,
        )

    def _process_single_task(self, task: MatchComponentAudit, driver: Any) -> None:
        """The core worker logic executed by a single thread."""
        logger.info(
            f"[Thread] Processing Match {task.match_id} | {task.component_name}"
        )

        scraper_class = self._scraper_registry.get(task.component_name)
        if not scraper_class:
            logger.error(f"No scraper mapped for '{task.component_name}'")
            return

        try:
            scraper = scraper_class(
                matchid=task.match_id, webdriver=driver, cfg=self.config
            )

            # Fetch A
            data_a, raw_a = self._scraper_process(scraper)

            # Anti-Randomization Pause
            time.sleep(self.config.pipeline.qa_pause_seconds)

            # Fetch B
            data_b, _ = self._scraper_process(scraper)

            # In-Memory Comparison
            if data_a == data_b and data_a is not None:
                self._handle_qa_success(task, data_a, raw_a)
            else:
                self._handle_qa_mismatch(task)

        except Exception as e:
            # Safely passes the exception without risking UnboundLocalError
            self._handle_scraper_error(task, e)

    def run_worker_loop(self, max_workers: int = 2, task_limit: int = 3):
        """Fetches a batch of tasks, spins up webdrivers, and processes them concurrently."""
        # tasks = self.db.get_pending_tasks(limit=self.config.pipeline.batch_size)
        tasks = self.db.get_pending_tasks(limit=task_limit)

        if not tasks:
            logger.info("No pending tasks found. Queue is empty.")
            return

        # Ensure we don't spin up 5 webdrivers if we only have 2 tasks
        # num_workers = min(self.config.pipeline.max_workers, len(tasks))
        num_workers = min(max_workers, len(tasks))

        logger.info(f"Spinning up {num_workers} webdrivers for {len(tasks)} tasks...")

        # 1. CREATE THE DRIVER POOL (The Bucket)
        driver_pool = queue.Queue()
        drivers = []
        for _ in range(num_workers):
            driver = self.mw.spawn_webdriver()
            driver_pool.put(driver)
            drivers.append(driver)

        logger.info(f"Starting ThreadPoolExecutor...")

        try:
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                futures = {}

                # 2. SUBMIT TASKS TO THREADS
                for task in tasks:
                    # Thread blocks here until a driver is available in the pool
                    driver = driver_pool.get()

                    # Submit the task to the background thread
                    future = executor.submit(self._process_single_task, task, driver)

                    # IMPORTANT: When the thread finishes, put the driver back in the bucket!
                    def return_driver_to_pool(f, d=driver):
                        driver_pool.put(d)

                    future.add_done_callback(return_driver_to_pool)
                    futures[future] = task

                # 3. WAIT FOR COMPLETION
                for future in as_completed(futures):
                    future.result()  # Will raise any fatal thread crashes so we can see them

        finally:
            # 4. CLEANUP
            logger.info("Worker loop complete. Shutting down webdrivers...")
            for driver in drivers:
                try:
                    driver.close()
                except Exception as e:
                    logger.error(f"Error closing driver: {e}")
