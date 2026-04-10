# src/sofascrape/pipeline/orchestrator.py

import json
import logging
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, Type

from sqlalchemy import select, update
from sqlalchemy.orm import Session
from tqdm import tqdm

# Import your heavy webdriver manager
from webdriver import (
    ManagerWebdriver,
)

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
from sofascrape.general.events import EventsComponentScraper
from sofascrape.schemas.general import (
    ConvertibleBaseModel,
    EventSchema,
)
from sofascrape.utils.sleepers import smart_sleep

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

    def _get_qa_delta(self, data_a: Any, data_b: Any) -> tuple[bool, dict]:
        """
        Deep compares two payloads.
        Returns (True, {}) if identical.
        Returns (False, {"key": {"a": val, "b": val}}) if different.
        """
        # 1. Handle the None cases
        if data_a is None and data_b is None:
            return True, {}
        if data_a is None or data_b is None:
            return False, {
                "root_payload": {
                    "a": "None" if data_a is None else "Data",
                    "b": "None" if data_b is None else "Data",
                }
            }

        # 2. Convert Pydantic models to dicts (Assuming Pydantic v2 `model_dump`)
        dict_a = data_a.model_dump() if hasattr(data_a, "model_dump") else data_a
        dict_b = data_b.model_dump() if hasattr(data_b, "model_dump") else data_b

        # Safety fallback if they somehow aren't dicts
        if not isinstance(dict_a, dict) or not isinstance(dict_b, dict):
            if dict_a == dict_b:
                return True, {}
            return False, {"raw_value": {"a": str(dict_a), "b": str(dict_b)}}

        # 3. Calculate the deep dictionary delta
        delta = self._calculate_dict_delta(dict_a, dict_b)

        # If delta is empty (length 0), they match!
        return len(delta) == 0, delta

    def _calculate_dict_delta(self, dict_a: dict, dict_b: dict) -> dict:
        """Recursive helper to find differences between two dictionaries."""
        delta = {}
        all_keys = set(dict_a.keys()).union(set(dict_b.keys()))

        for k in all_keys:
            val_a = dict_a.get(k)
            val_b = dict_b.get(k)

            # If both are nested dictionaries, go deeper
            if isinstance(val_a, dict) and isinstance(val_b, dict):
                nested_delta = self._calculate_dict_delta(val_a, val_b)
                if nested_delta:
                    delta[k] = nested_delta
            # If they are different, record the delta
            elif val_a != val_b:
                delta[k] = {"a": val_a, "b": val_b}

        return delta

    # HACK: 2026-04-10 not needed ?
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
            status=AuditStatusTypes.UNAVAILABLE.value,
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
        self.db.update_task_status(task.audit_id, status=AuditStatusTypes.SUCCESS.value)
        logger.info(f"✅ QA Passed for Match {task.match_id} ({task.component_name})!")

    def _handle_qa_mismatch(self, task: MatchComponentAudit, delta: dict) -> None:
        """Flags the task for retry and logs the exact data differences."""
        logger.warning(
            f"❌ QA Mismatch for Match {task.match_id} ({task.component_name})."
        )

        # Convert the dictionary to a string, and optionally truncate it to 500 chars
        # so it doesn't overflow your database column if the diff is massive.
        delta_str = json.dumps(delta)
        error_msg = f"QA Mismatch | Delta: {delta_str}"
        if len(error_msg) > 500:
            error_msg = error_msg[:497] + "..."

        self.db.update_task_status(
            task.audit_id,
            status=AuditStatusTypes.QA_MISMATCH.value,
            error_message=error_msg,
            increment_retry=True,
        )

    def _handle_scraper_error(self, task: MatchComponentAudit, e: Exception) -> None:
        """Logs the exception and flags the task for retry."""
        logger.error(
            f"⚠️ Error on Match {task.match_id} ({task.component_name}): {str(e)}"
        )
        self.db.update_task_status(
            task.audit_id,
            status=AuditStatusTypes.API_ERROR.value,
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
            data_a, raw_a = scraper_a.process()

            # Anti-Randomization Pause
            smart_sleep(
                strategy=self.config.anti_bot_sleep.strategy,
                params=self.config.anti_bot_sleep.params,
            )

            # FETCH B (Completely Fresh Instance)
            scraper_b = scraper_class(
                matchid=task.match_id, webdriver=driver, cfg=self.config
            )

            data_b, _ = scraper_b.process()

            # Check for the Delta
            is_match, delta = self._get_qa_delta(data_a, data_b)

            if is_match:
                if data_a is None:
                    # Both are None -> 404 / No Data
                    self._handle_unavailable(task)
                    return False
                else:
                    # Both matched -> Golden Data!
                    self._handle_qa_success(task, data_a, raw_a)
                    return True
            else:
                # They didn't match -> Anti-Bot triggered!
                self._handle_qa_mismatch(task, delta)
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
                desc=f"Scraping tasks (threaded - {num_workers})",
                unit="component",
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

    def _get_events_season(
        self, session: Session, season_id: int, debug_limit: int | None = None
    ) -> list[Events]:
        """
        helper functions to get the matches from the events table
        """
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
            return []

        return matches

    def _filter_events_seson_matches(
        self,
        session: Session,
        raw_matches: list[
            Any
        ],  # Using Any because it's technically a SQLAlchemy Row containing Event columns
        component: Component,
    ) -> list[Any]:
        """
        Helper method to filter out matches that have already been queued
        in the match_component_audit table for a specific component.
        """
        if not raw_matches:
            return []

        # 1. Extract the match_ids from the matches we just fetched
        match_ids = [row.match_id for row in raw_matches]

        # 2. Query the audit table to see which of these IDs ALREADY exist
        stmt = select(MatchComponentAudit.match_id).where(
            MatchComponentAudit.match_id.in_(match_ids),
            MatchComponentAudit.component_name == component.value,
        )

        # session.scalars() returns a flat list of just the IDs (not full objects/rows)
        # We cast it to a set() for lightning-fast lookups
        existing_match_ids = set(session.scalars(stmt).all())

        # 3. Keep only the rows whose match_id is NOT in the existing set
        filtered_matches = [
            row for row in raw_matches if row.match_id not in existing_match_ids
        ]

        return filtered_matches

    # --- queue_season_missing_component dev  ----
    def queue_season_missing_component(
        self,
        season_id: int,
        component: Component,
        debug_limit: int | None = None,
        strict_mode: bool = True,
    ) -> int:
        """
        Backfills a season for the specfic component.
        debug_limit: limits the amount added - used for testing.
            strict_mode: Some API calls return none as there is no data, this is reflected in events table which we check.
        """

        logger.info(
            f"--- Starting Backfill | Season: {season_id} | Component: {component.value} | Strict: {strict_mode} | Limit: {debug_limit} ---"
        )

        with self.db.SessionLocal() as session:
            raw_matches = self._get_events_season(
                session=session,
                season_id=season_id,
            )

            logger.info(f"Found {len(raw_matches)} matches.")
            # check empty
            if not raw_matches:
                return 0

            # filter
            filtered_matches = self._filter_events_seson_matches(
                session=session, raw_matches=raw_matches, component=component
            )

            if debug_limit is not None:
                filtered_matches = filtered_matches[:debug_limit]

            logger.info(f"Filtered matches: {len(filtered_matches)}")
            if not filtered_matches:
                return 0

            queued_count = 0
            skipped_count = 0

            for row in filtered_matches:
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

    def queue_season_missing_components(
        self,
        season_id: int,
        components: list[Component],
        debug_limit: int | None = None,
        strict_mode: bool = True,
    ) -> dict[str, int]:  # Fixed return type
        """
        Processes a list of components for a season sequentially.
        """
        component_names = [c.value for c in components]
        logger.info(
            f"Starting multi-component queue for Season {season_id}: {component_names}"
        )

        components_queued_count: dict[str, int] = {}

        for component in components:
            try:
                queued = self.queue_season_missing_component(
                    season_id=season_id,
                    component=component,
                    debug_limit=debug_limit,
                    strict_mode=strict_mode,
                )
                components_queued_count[component.value] = queued

            except Exception as e:
                # Catch failures so the rest of the list can still process
                logger.error(
                    f"❌ Failed to queue component '{component.value}' for season {season_id}: {e}"
                )
                components_queued_count[component.value] = 0

        logger.info(
            f"Multi-component queue complete for Season {season_id}: {components_queued_count}"
        )
        return components_queued_count

    # --- helpers sync session ---

    def _call_event_api(
        self, tournament_id: int, season_id: int
    ) -> tuple[Any, list[dict]]:
        """
        Spawns a driver, scrapes the season events, and closes the driver.
        Returns the parsed Pydantic data and raw JSON list.
        """
        driver = self.mw.spawn_webdriver()
        try:
            scraper = EventsComponentScraper(
                tournamentid=tournament_id,
                seasonid=season_id,
                webdriver=driver,
                cfg=self.config,
            )
            parsed_api_data, raw_api_data = scraper.process()

            # Extract the actual list of events from the raw payload
            raw_events_list = raw_api_data.get("events", [])
            return parsed_api_data, raw_events_list
        finally:
            # Ensure the driver always closes, even if the scraper crashes!
            driver.close()

    def _get_sync_event_delta(
        self, api_events_list: list[Any], db_events_set: set[int]
    ) -> list[int]:
        """
        Compares the API 'finished' matches against our DB knowledge.
        Returns a list of NEWLY finished match_ids.
        """
        delta_match_ids = []

        for p_event in api_events_list:
            if p_event.status.type == EventsStatusTypes.FINISHED.value:
                if p_event.id not in db_events_set:
                    delta_match_ids.append(p_event.id)

        logger.info(f"-> Discovered {len(delta_match_ids)} NEWLY finished matches.")
        return delta_match_ids

    def _upsert_calendar(
        self,
        tournament_id: int,
        parsed_events_list: list[Any],
        raw_events_list: list[dict],
    ) -> None:
        """
        Safely upserts the full calendar to update statuses and future kick-off times.
        """
        logger.info("-> Upserting calendar to the Events table...")
        try:
            self.db.upsert_events(
                tournament_id=tournament_id,
                parsed_events=parsed_events_list,
                raw_event=raw_events_list,
            )
            logger.info("✅ Database Calendar Updated.")
        except Exception as e:
            logger.error(f"❌ Failed to upsert calendar: {e}")

    # --- sync session main method ---

    def sync_events(self, tournament_id: int, season_id: int) -> list[int]:
        """
        Updates the events table for the given tournament and season.
        Returns a list of NEWLY finished match IDs.
        """
        # 1. Get the events the DB already knows are finished
        with self.db.SessionLocal() as session:
            matches = self._get_events_season(session=session, season_id=season_id)

        db_events_set = set(m.match_id for m in matches)

        # 2. Scrape the latest calendar from the API
        parsed_api_data, raw_events_list = self._call_event_api(
            tournament_id=tournament_id, season_id=season_id
        )

        parsed_events_list = parsed_api_data.events

        logger.info(
            f"-> API returned {len(parsed_events_list)} total matches for the season."
        )

        # 3. Calculate the Delta
        delta_match_ids = self._get_sync_event_delta(
            api_events_list=parsed_events_list, db_events_set=db_events_set
        )

        # 4. ALWAYS update the calendar if the API returned data
        if parsed_events_list:
            self._upsert_calendar(
                tournament_id=tournament_id,
                parsed_events_list=parsed_events_list,
                raw_events_list=raw_events_list,
            )
        else:
            logger.warning(
                "⚠️ No events parsed from the API. Skipping database upsert."
            )

        return delta_match_ids

    def sync_season(
        self, tournament_id: int, season_id: int, components: list[Component]
    ) -> None:
        """
        WORKFLOW A: The "Weekly Update" Command.
        1. Updates the calendar.
        2. Audits the queue for any missing components for finished matches.
        3. Runs the multi-threaded workers to fetch the missing data.
        """
        logger.info(f"Starting Master Workflow: Syncing Season {season_id}...")

        # Step 1: Update the Calendar (Finds new matches and updates kickoff times)
        self.sync_events(tournament_id=tournament_id, season_id=season_id)

        # Step 2: The Safety Net (Returns a dict like {'stats': 2, 'odds': 0})
        queued_dict: dict[str, int] = self.queue_season_missing_components(
            season_id=season_id, components=components
        )

        # Step 3: Flatten the dictionary to get the total number of tasks
        total_queued: int = sum(queued_dict.values())

        # Step 4: Unleash the Hounds
        if total_queued > 0:
            logger.info(
                f"🔥 {total_queued} new tasks queued! Spinning up the worker pool..."
            )
            self.run_worker_loop(
                max_workers=self.config.pipeline.max_workers,
                task_limit=self.config.pipeline.batch_size,
            )
        else:
            logger.info("✅ Database is fully up-to-date. No workers needed today.")

    # --- helper: janitor ---

    def _build_janitor_conditions(
        self,
        tournament_id: int | None,
        season_id: int | None,
        allowed_statuses: list[AuditStatusTypes],
        max_retries: int,
    ) -> list:
        """
        SRP Helper: Constructs the WHERE clauses for the Janitor's bulk update.
        """
        # 1. Base conditions
        conditions = [
            MatchComponentAudit.status.in_([s.value for s in allowed_statuses]),
            MatchComponentAudit.retry_count < max_retries,
        ]

        # 2. The Subquery (Only needed if we are filtering by tournament/season)
        if tournament_id or season_id:
            from sqlalchemy import select

            from sofascrape.db.models import Events

            stmt_events = select(Events.match_id)
            if tournament_id:
                stmt_events = stmt_events.where(Events.tournament_id == tournament_id)
            if season_id:
                stmt_events = stmt_events.where(Events.season_id == season_id)

            # Turn the SELECT into a subquery constraint
            events_subquery = stmt_events.scalar_subquery()
            conditions.append(MatchComponentAudit.match_id.in_(events_subquery))

        return conditions

    # --- main workflow method ---

    def retry_failed_components(
        self,
        tournament_id: int | None = None,
        season_id: int | None = None,
        allowed_statuses: list[AuditStatusTypes] | None = None,
        max_retries: int | None = None,
    ) -> int:
        """
        WORKFLOW C: The Janitor.
        Finds tasks that failed due to temporary errors (like QA Mismatches)
        and resets them to PENDING so the worker loop can try them again.
        """
        # Fix the Python default argument "gotchas" safely
        if allowed_statuses is None:
            allowed_statuses = [
                AuditStatusTypes.QA_MISMATCH,
                AuditStatusTypes.API_ERROR,
            ]
        if max_retries is None:
            max_retries = self.config.pipeline.max_retries

        logger.info("🧹 Starting Janitor: Sweeping up failed tasks...")

        # 1. SRP Helper: Get the exact SQL conditions we need
        conditions = self._build_janitor_conditions(
            tournament_id=tournament_id,
            season_id=season_id,
            allowed_statuses=allowed_statuses,
            max_retries=max_retries,
        )

        # 2. Database Action: Perform the bulk update
        with self.db.SessionLocal() as session:
            from sqlalchemy import update

            stmt_update = (
                update(MatchComponentAudit)
                .where(*conditions)
                .values(status=AuditStatusTypes.PENDING.value)
            )

            result = session.execute(stmt_update)
            session.commit()

            requeued_count = result.rowcount

        # 3. Logging Action
        if requeued_count > 0:
            logger.info(
                f"♻️ Janitor swept up {requeued_count} tasks and set them to PENDING."
            )
        else:
            logger.info("✨ No eligible failed tasks found. The queue is clean!")

        return requeued_count
