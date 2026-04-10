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

# TOURNAMENT_ID = 54 # scottish PL
TARGET_SEASON_ID = 77128  # season 25/26


pipeline.backfill_component(
    season_id=TARGET_SEASON_ID, component=Component.STATS, debug_limit=10
)

pipeline.run_worker_loop(max_workers=2, task_limit=30)

# ------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------


os.environ["DISPLAY"] = ":0"


from enum import StrEnum
from typing import Any, Callable, Dict

from sqlalchemy import select

from sofascrape.conf.config import load_config
from sofascrape.db.manager import DatabaseManager
from sofascrape.db.models import Events, MatchComponentAudit


class EventsStatusTypes(StrEnum):
    POSTPONED = "postponed"
    NOTSTARTED = "notstarted"
    FINISHED = "finished"


class AuditStatusTypes(StrEnum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    UNAVAILABLE = "UNAVAILABLE"
    SKIPPED_MISSING = "SKIPPED_MISSING"


class Component(StrEnum):
    STATS = "stats"
    LINEUPS = "lineups"
    INCIDENTS = "incidents"
    ODDS = "odds"
    BASE = "base"
    GRAPH = "graph"


# ==========================================
# 1. Validation Strategies (SRP & Strategy Pattern)
# ==========================================


def validate_stats(row: Any) -> bool:
    """Validation logic specific to the STATS component."""
    is_valid = True
    if not row.hasXg:
        print(f"⚠️ [STATS] Match {row.match_id} is missing xG data.")
        is_valid = False
    return is_valid


def validate_graph(row: Any) -> bool:
    """Validation logic specific to the GRAPH component."""
    if not row.hasEventPlayerHeatMap:
        print(f"⚠️ [GRAPH] Match {row.match_id} is missing Player Heat Maps.")
        return False
    return True


# Map components to their respective validation functions
# Components without a specific validator will safely bypass checks
VALIDATOR_STRATEGIES: Dict[Component, Callable[[Any], bool]] = {
    Component.STATS: validate_stats,
    Component.GRAPH: validate_graph,
}


# ==========================================
# 2. Main Orchestration Function
# ==========================================


def backfill_component(
    db_manager: DatabaseManager,
    season_id: int,
    component: Component,
    debug_limit: int | None = None,
    strict_mode: bool = True,
) -> int:
    """
    Backfills a specific component for an entire season.

    :param strict_mode: If True, skips queuing when data is missing.
                        If False, warns but queues the component anyway.
    """
    print(
        f"\n--- Starting Backfill | Season: {season_id} | Component: {component.value} | Strict: {strict_mode} ---"
    )

    with db_manager.SessionLocal() as session:
        # Fetch required flags to power our validators
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
            print(
                f"⚠️ No matches found in the database: season_id: {season_id}, component: {component.value}."
            )
            return 0

        print(f"Found {len(matches)} matches. Processing...")

        queued_count = 0
        skipped_count = 0

        # Grab the specific validator for this component, if it exists
        validator_func = VALIDATOR_STRATEGIES.get(component)

        for row in matches:

            # If a validator exists for this component, run it
            if validator_func:
                is_valid = validator_func(row)

                # If validation fails AND we are in strict mode, skip this match
                if not is_valid and strict_mode:
                    print(
                        f"⏭️ Skipping Match {row.match_id} due to missing dependencies."
                    )

                    task = MatchComponentAudit(
                        match_id=row.match_id,
                        component_name=component.value,
                        status=AuditStatusTypes.SKIPPED_MISSING,
                    )
                    session.merge(task)
                    skipped_count += 1
                    continue

            # Queue the task (either validation passed, no validation needed, or strict_mode is False)
            task = MatchComponentAudit(
                match_id=row.match_id,
                component_name=component.value,
                status=AuditStatusTypes.PENDING,
            )

            session.merge(task)
            queued_count += 1

        session.commit()
        print(
            f"✅ Finished! Queued {queued_count} tasks | Skipped {skipped_count} tasks."
        )
        return queued_count


# --- testing ----
TARGET_SEASON_ID = 77128


TOURNAMENT_ID = 56
SEASON_ID = 77129
MATCH_ID = 14035506

# TOURNAMENT_ID = 54

SEASON_ID_LEAGUE_ONE = 77129

# Test with a limit of 30, checking the STATS component
tasks_added = backfill_component(db, TARGET_SEASON_ID, Component.STATS, debug_limit=5)


tasks_added = backfill_component(
    db, SEASON_ID_LEAGUE_ONE, Component.STATS, debug_limit=5
)


tasks_added = backfill_component(
    db, SEASON_ID_LEAGUE_ONE, Component.STATS, debug_limit=5, strict_mode=False
)


# Run for all matches (no limit)
# tasks_added = backfill_component(db, TARGET_SEASON_ID, Component.GRAPH)


from sofascrape.pipeline.orchestrator import Orchestrator

config = load_config()
db = DatabaseManager(config)

pipeline = Orchestrator(db, config)

pipeline.run_worker_loop(max_workers=5, task_limit=30)
