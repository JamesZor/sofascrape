# src/sofascrape/pipeline/orchestrator.py

import logging
import queue
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, Type

from sqlalchemy import select
from tqdm import tqdm

# Import your heavy webdriver manager
from webdriver import ManagerWebdriver

from sofascrape.abstract.base import BaseComponentScraper
from sofascrape.conf.config import AppConfig
from sofascrape.db.manager import DatabaseManager
from sofascrape.db.models import (
    AuditStatusTypes,
    Component,
    Events,
    EventsStatusTypes,
    MatchComponentAudit,
)
from sofascrape.football.eventComponent import EventFootallComponentScraper
from sofascrape.football.graphComponent import FootballGraphComponentScraper
from sofascrape.football.incidentsComponent import FootballIncidentsComponentScraper
from sofascrape.football.lineupComonent import FootballLineupComponentScraper
from sofascrape.football.oddsComponent import FootballOddsComponentScraper

# Import your Scraper Classes (Adjust paths if needed!)
from sofascrape.football.statsComponent import FootballStatsComponentScraper
from sofascrape.schemas.general import ConvertibleBaseModel

# Add your base match scraper and graph scraper here too when ready!

logger = logging.getLogger(__name__)


# ==========================================
# 1. Validation Strategies
# ==========================================


def validate_stats(row: Any) -> bool:
    """Validation logic specific to the STATS component."""
    if not row.hasXg:
        logger.warning(f"⚠️ [STATS] Match {row.match_id} is missing xG data.")
        return False
    return True


def validate_graph(row: Any) -> bool:
    """Validation logic specific to the GRAPH component."""
    if not row.hasEventPlayerHeatMap:
        logger.warning(f"⚠️ [GRAPH] Match {row.match_id} is missing Player Heat Maps.")
        return False
    return True


# Map components to their respective validation functions
VALIDATOR_STRATEGIES: Dict[Component, Callable[[Any], bool]] = {
    Component.STATS: validate_stats,
    Component.GRAPH: validate_graph,
}

# ==========================================
# 2. Main Class - Orchestrator
# ==========================================


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
            "odds": FootballOddsComponentScraper,
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

    def _handle_unavailable(self, task: MatchComponentAudit) -> None:
        """Flags the task as permanently unavailable because the API returned no data both times."""
        logger.info(
            f"⏭️ Data unavailable for Match {task.match_id} ({task.component_name}). Skipping."
        )

        # We don't increment retry here, because retrying a 404 over and over is a waste of time.
        self.db.update_task_status(
            task.audit_id,
            status="UNAVAILABLE",
            error_message="API returned no data (404/Empty)",
            increment_retry=False,
        )

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

    def _pbar_update_run_work_loop(
        self,
        pbar: tqdm,
        success: bool,
        completed_task: MatchComponentAudit,
        success_count: int,
    ) -> None:
        """helper function to update the progress bar (tqdm) in the run_worker_loop method."""

        status_symbol = "✓" if success else "✗"
        pbar.set_postfix(
            {
                "Last": f"{completed_task.match_id}.{completed_task.component_name} {status_symbol}",
                "Success": f"{success_count}/{pbar.n + 1}",
            }
        )
        pbar.update(1)

    def _process_single_task(self, task: MatchComponentAudit, driver: Any) -> bool:
        """The core worker logic executed by a single thread."""
        logger.info(
            f"[Thread] Processing Match {task.match_id} | {task.component_name}"
        )

        scraper_class = self._scraper_registry.get(task.component_name)
        if not scraper_class:
            logger.error(f"No scraper mapped for '{task.component_name}'")
            return False

        try:

            # FETCH A (Isolated Instance)
            scraper_a = scraper_class(
                matchid=task.match_id, webdriver=driver, cfg=self.config
            )
            data_a, raw_a = self._scraper_process(scraper_a)

            # Anti-Randomization Pause
            time.sleep(self.config.pipeline.qa_pause_seconds)

            # FETCH B (Completely Fresh Instance)
            scraper_b = scraper_class(
                matchid=task.match_id, webdriver=driver, cfg=self.config
            )
            data_b, _ = self._scraper_process(scraper_b)

            # In-Memory Comparison
            if data_a is None and data_b is None:
                # Both fetches returned nothing. The data doesn't exist.
                self._handle_unavailable(task)
                return False
            elif data_a == data_b:
                # Data exists and is identical. Golden!
                self._handle_qa_success(task, data_a, raw_a)
                return True
            else:
                # Data exists but is different. We caught them randomizing!
                self._handle_qa_mismatch(task)
                return False

        except Exception as e:
            # Safely passes the exception without risking UnboundLocalError
            self._handle_scraper_error(task, e)
            return False

    def _thread_worker(
        self, task: MatchComponentAudit, driver_pool: queue.Queue
    ) -> tuple[MatchComponentAudit, bool]:
        """Concurrency wrapper to manage driver state and execute the task."""
        driver = driver_pool.get()
        try:
            success = self._process_single_task(task, driver)
            return task, success
        finally:
            driver_pool.put(driver)

    def run_worker_loop(self, max_workers: int = 2, task_limit: int = 3):
        """Fetches a batch of tasks, spins up webdrivers, and processes them concurrently."""
        # tasks = self.db.get_pending_tasks(limit=self.config.pipeline.batch_size)
        tasks = self.db.get_pending_tasks(limit=task_limit)

        if not tasks:
            logger.info("No pending tasks found. Queue is empty.")
            return

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
            with tqdm(
                total=len(tasks),
                desc="Scraping tasks (threaded)",
                unit="match_component",
            ) as pbar:
                with ThreadPoolExecutor(max_workers=num_workers) as executor:

                    futures = {}
                    success_count: int = 0

                    # 2. SUBMIT TASKS TO THREADS
                    for task in tasks:
                        future = executor.submit(self._thread_worker, task, driver_pool)
                        futures[future] = task

                    # 3. LIVE PROGRESS BAR UPDATES
                    for future in as_completed(futures):
                        # Unpack the results from the thread wrapper
                        completed_task, success = future.result()

                        if success:
                            success_count += 1

                        self._pbar_update_run_work_loop(
                            pbar=pbar,
                            success=success,
                            completed_task=completed_task,
                            success_count=success_count,
                        )

        finally:
            # 4. CLEANUP
            logger.info("Worker loop complete. Shutting down webdrivers...")
            for driver in drivers:
                try:
                    driver.close()
                except Exception as e:
                    logger.error(f"Error closing driver: {e}")

    # ---- backfull helpers
    def _determine_task_status(
        self, row: Any, component: Component, strict_mode: bool
    ) -> AuditStatusTypes:
        """
        Determines if a task should be queued or skipped based on validation strategies.
        """
        validator_func = VALIDATOR_STRATEGIES.get(component)

        # If there is a validator, run it
        if validator_func:
            is_valid = validator_func(row)

            # If it failed validation and we are strict, skip it
            if not is_valid and strict_mode:
                logger.info(
                    f"⏭️ Skipping Match {row.match_id} due to missing dependencies."
                )
                return AuditStatusTypes.SKIPPED_MISSING

        # If no validator exists, or it passed, or we aren't strict, queue it!
        return AuditStatusTypes.PENDING

    # ---- main workflow ----

    def backfill_component(
        self,
        season_id: int,
        component: Component,
        debug_limit: int | None = None,
        strict_mode: bool = True,
    ) -> int:
        """
        WORKFLOW D: Backfills a specific component for an entire season.
        """
        logger.info(
            f"--- Starting Backfill | Season: {season_id} | Component: {component.value} | Strict: {strict_mode} ---"
        )

        with self.db.SessionLocal() as session:
            stmt = select(
                Events.match_id,
                Events.hasXg,
                Events.hasEventPlayerStatistics,
                Events.hasEventPlayerHeatMap,
            ).where(
                Events.season_id == season_id,
                Events.status_type == EventsStatusTypes.FINISHED,
            )

            if debug_limit is not None:
                stmt = stmt.limit(debug_limit)

            matches = session.execute(stmt).all()

            if not matches:
                logger.warning(
                    f"⚠️ No matches found in the database: season_id: {season_id}"
                )
                return 0

            logger.info(f"Found {len(matches)} matches. Processing...")

            queued_count = 0
            skipped_count = 0

            for row in matches:
                # 1. SRP Helper: Get the status
                target_status = self._determine_task_status(row, component, strict_mode)

                # 2. Database Action: Create and merge the task
                task = MatchComponentAudit(
                    match_id=row.match_id,
                    component_name=component.value,
                    status=target_status,
                )
                session.merge(task)

                # 3. Counter Action: Update the correct tally
                if target_status == AuditStatusTypes.SKIPPED_MISSING:
                    skipped_count += 1
                else:
                    queued_count += 1

            session.commit()
            logger.info(
                f"✅ Finished! Queued {queued_count} tasks | Skipped {skipped_count} tasks."
            )

        return queued_count
