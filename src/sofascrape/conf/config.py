"""
Simple script to read the YAML of conf/setting.yaml
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass
class DataBaseConfig:
    url: str


@dataclass
class PipelineConfig:
    max_retries: int
    qa_pause_seconds: float
    batch_size: int


@dataclass
class ScraperConfig:
    user_agent: str
    timeout_seconds: int


@dataclass
class LinksConfig:
    tournament_empty: str
    seasons_empty: str
    events_season_empty: str
    # football match
    football_base_match: str
    football_stats: str
    football_lineup: str
    football_incidents: str
    football_graph: str
    football_heatmap: str
    football_odds: str


@dataclass
class AntiBotSleepConfig:
    strategy: str
    params: dict[str, Any]


@dataclass
class AppConfig:
    """The master config dataclass"""

    database: DataBaseConfig
    pipeline: PipelineConfig
    scraper: ScraperConfig
    links: LinksConfig
    anti_bot_sleep: AntiBotSleepConfig


def load_config(config_file_name: str = "settings.yaml") -> AppConfig:
    """
    Loads the YAML file and returns a strongly-typed AppConfig object.
    Defaults to looking for 'settings.yaml' in the same directory as this file.
    """
    # current_dir = Path(__file__).parent
    # config_path = current_dir / config_file_name
    # HACK:
    config_path = Path(
        "/home/james/bet_project/sofascrape/src/sofascrape/conf/settings.yaml"
    )

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found @: {config_path}")

    with open(config_path, "r") as f:
        raw_data = yaml.safe_load(f)

    return AppConfig(
        database=DataBaseConfig(**raw_data.get("database", {})),
        pipeline=PipelineConfig(**raw_data.get("pipeline", {})),
        scraper=ScraperConfig(**raw_data.get("scraper", {})),
        links=LinksConfig(**raw_data.get("links", {})),
        anti_bot_sleep=AntiBotSleepConfig(**raw_data.get("anti_bot_sleep", {})),
    )


# %%
# --- IPython Playground ---
if __name__ == "__main__":
    config = load_config()
    print(f"Loaded config successfully!")
    print(f"DB URL: {config.database.url}")
    print(f"QA Pause: {config.pipeline.qa_pause_seconds} seconds")
