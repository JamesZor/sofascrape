"""
Simple script to read the YAML of conf/setting.yaml
"""

from dataclasses import dataclass
from pathlib import Path

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


@dataclass
class AppConfig:
    """The master config dataclass"""

    database: DataBaseConfig
    pipeline: PipelineConfig
    scraper: ScraperConfig
    links: LinksConfig


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
    )


# %%
# --- IPython Playground ---
if __name__ == "__main__":
    config = load_config()
    print(f"Loaded config successfully!")
    print(f"DB URL: {config.database.url}")
    print(f"QA Pause: {config.pipeline.qa_pause_seconds} seconds")
