# SofaScrape ⚽️

An enterprise-grade, multi-threaded web scraping pipeline for extracting historical and live football data. 

Built with resilience in mind, this pipeline uses idempotent database queues, Pydantic data contracts, and advanced anti-bot evasion techniques to scrape thousands of matches without memory leaks or duplicate data.

## 🏗️ Architecture Overview

* **State Management:** PostgreSQL handles the queue. The pipeline uses `INSERT ... ON CONFLICT DO UPDATE` to ensure data is never duplicated. If the scraper crashes, it safely picks up exactly where it left off.
* **Anti-Bot Evasion:** Uses a Gamma-distributed sleep strategy to mimic human behavior, alongside a "Fetch A / Fetch B" delta-check to catch and discard randomized honeypot data.
* **Data Validation:** Pydantic schemas act as strict gatekeepers. If the API payload structure changes, the pipeline flags the mismatch rather than poisoning the database.
* **The Orchestrator:** A central manager that spins up/down proxy-enabled webdrivers and manages multi-threaded worker pools.

## 📂 Core Project Structure

    src/sofascrape/
    ├── conf/           # Config loader and settings.yaml (Set limits & anti-bot params)
    ├── db/             # SQLAlchemy DatabaseManager and Models
    ├── schemas/        # Pydantic validation contracts
    ├── abstract/       # Base scraper templates
    ├── general/        # High-level scrapers (Tournaments, Seasons, Events)
    ├── football/       # Deep-data scrapers (Odds, Stats, Lineups, Incidents)
    └── pipeline/       # orchestrator.py (The brain of the operation)

## 🚀 The Core Workflows

To run these workflows, initialize the pipeline in your REPL or main.py script:

    from sofascrape.conf.config import load_config
    from sofascrape.db.manager import DatabaseManager
    from sofascrape.pipeline.orchestrator import Orchestrator
    from sofascrape.db.models import Component

    config = load_config()
    db = DatabaseManager(config)
    pipeline = Orchestrator(db, config)

### 1. Workflow D: The Bootstrapper
Use this when tracking a new league. It pulls the tournament metadata, all historical season IDs, and the entire match calendar (Events) for every season.

    # Setup Scottish League One
    pipeline.setup_tournament(tournament_id=56) 

### 2. Workflow A: The Weekly Update
Use this for weekly maintenance. It syncs the calendar to find matches that finished over the weekend, queues them up, and runs the webdrivers to fetch the deeper data.

    target_components = [Component.STATS, Component.LINEUPS, Component.ODDS]

    # Syncs the calendar and fetches missing data for the current season
    pipeline.sync_season(
        tournament_id=56, 
        season_id=77129, 
        components=target_components
    )

### 3. The Historical Backfill
Use this to scrape years of data. It builds a massive queue of missing components for older seasons, then drains the queue using the multi-threaded worker pool.

    target_components = [Component.STATS, Component.LINEUPS, Component.ODDS]

    # 1. Queue up the missing tasks (Idempotent - safe to run multiple times)
    pipeline.queue_season_missing_components(
        season_id=17368, 
        components=target_components
    )

    # 2. Unleash the webdrivers (Set task_limit=None to drain the whole queue)
    pipeline.run_worker_loop(max_workers=5, task_limit=None)

### 4. Workflow C: The Janitor
Use this to clean up network timeouts. Network drops and proxy rotations will occasionally cause a task to fail (QA_MISMATCH or API_ERROR). The Janitor sweeps these failures back into the PENDING queue for another attempt.

    # Reset failed tasks for a specific season (or leave blank for a global sweep)
    pipeline.retry_failed_components(season_id=17368)

    # Run a small worker pool to finish them off
    pipeline.run_worker_loop(max_workers=2)

## ⚙️ Configuration (settings.yaml)

If the scraper is getting blocked, adjust your anti_bot_sleep strategy. 
If your computer is running out of RAM, lower max_workers.

    pipeline:
      max_workers: 5         # Number of concurrent Chrome windows
      max_retries: 3         # How many times the Janitor will retry a task
      batch_size: 50         # Tasks per orchestrator loop

    anti_bot_sleep:
      strategy: "gamma"      # Long-tail human-like distribution
      params:
        alpha: 2.5
        beta: 0.8            # Mean sleep = alpha * beta (~2.0 seconds)
