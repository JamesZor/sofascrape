# Quality Assurance System - Code Structure & Skeleton

# Architecture Overview 
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Run Manager   │───▶│  Data Comparator │───▶│ Quality Assessor│
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                        │                       │
         ▼                        ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Store    │    │   Hash Generator │    │  Retry Manager  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                                               │
         ▼                                               ▼
┌─────────────────┐                            ┌─────────────────┐
│Season Validator │                            │ Report Generator│
└─────────────────┘                            └─────────────────┘

## Project Structure
src/sofascrape/quality/
├── __init__.py
├── config/
│   ├── __init__.py
│   └── quality_config.yaml
├── core/
│   ├── __init__.py
│   ├── data_models.py      # All dataclasses and enums
│   ├── hash_generator.py   # Component hashing logic
│   ├── quality_assessor.py # Binary component validation
│   ├── consensus_builder.py # Cross-run consensus logic
│   ├── retry_manager.py    # Selective component retries
│   └── golden_builder.py   # Final dataset creation
├── storage/
│   ├── __init__.py
│   └── run_storage.py      # Save/load runs and results
├── reports/
│   ├── __init__.py
│   └── review_reports.py   # Manual review interfaces
└── manager.py              # Main orchestrator class


# Configuration Structure
# Quality assessment configuration
quality:
  # Which components to process (can disable some for testing)
  active_components:
    - "base"
    - "stats" 
    - "lineup"
    - "incidents"
    - "graph"

  # Component validation rules
  component_rules:
    base:
      required: true
      critical_fields: ["homeTeam", "awayTeam", "status", "startTimestamp"]
    stats:
      required: true
      min_groups: 1
    lineup:
      required: true
      min_players_per_team: 11
    incidents:
      required: false
      allow_empty: true
    graph:
      required: false
      min_points: 5

  # Consensus requirements
  consensus:
    threshold: 1.0  # 100% agreement required
    min_runs_required: 2

  # Retry configuration
  retry:
    max_attempts: 2
    delay_seconds: 30.0
    
  # Hash normalization (fields to ignore when hashing)
  hash_exclusions:
    base:
      - "scraped_at"
      - "currentPeriodStartTimestamp"
    stats:
      - "generated_at"
    lineup:
      - "lastUpdated"
    incidents:
      - "processedAt"
    graph:
      - "calculatedAt"

# Storage paths
storage:
  base_dir: "data"
  runs_subdir: "runs"
  analysis_subdir: "analysis" 
  golden_subdir: "golden"
  logs_subdir: "logs"

# Data Storage Structure
data/
├── tournament_54/
│   ├── season_62408/
│   │   ├── runs/
│   │   │   ├── 1_20250805.pkl
│   │   │   ├── 2_20250812.pkl
│   │   │   └── retry_stats_20250815.pkl
│   │   ├── analysis/
│   │   │   ├── consensus_20250815.pkl
│   │   │   └── review_report_20250815.html
│   │   ├── golden/
│   │   │   └── golden_dataset_20250815.pkl
│   │   └── logs/
│   │       ├── quality_analysis_20250815.log
│   │       └── retry_20250815.log

## Implementation Order
### Phase 1: Foundation

    data_models.py - Start with basic dataclasses, add methods later
    quality_config.yaml - Basic configuration structure
    run_storage.py - Basic save/load functionality

### Phase 2: Core Components

    hash_generator.py - Start with just base component
    quality_assessor.py - Start with just base component
    manager.py - Basic structure, just scraping and storage

### Phase 3: Analysis

    consensus_builder.py - Start with simple hash comparison
    review_reports.py - Basic text output of discrepancies

### Phase 4: Advanced Features

    retry_manager.py - Selective component retry
    golden_builder.py - Final dataset creation
    Add remaining components (stats, lineup, etc.)

Phase 5: Polish


# Usage Pattern

    ## run 1 

        config = OmegaConf.load("quality_config.yaml")
        manager = SeasonQualityManager(tournament_id=54, season_id=62408, config=config)
        run1_id = manager.execute_scraping_run("week1_run")

    ## run 2 

        run2_id = manager.execute_scraping_run("week2_run")

    ## Analysis
        consensus = manager.build_consensus_analysis()
        report = manager.generate_review_report(consensus)

    # Manual review of report.discrepancies
    # Create retry plan based on review

        retry_plan = report.create_retry_plan(selected_issues)
        retry_results = manager.execute_retry_plan(retry_plan)

    # Re-analyze and create golden dataset

        final_consensus = manager.build_consensus_analysis() 
        golden_dataset = manager.create_golden_dataset(final_consensus)

