import logging
from typing import Any, Dict, List, Optional, Union

import pytest
from omegaconf import DictConfig, OmegaConf

import sofascrape.schemas.general as sofaschema
from sofascrape.quality.core.hash_generator import HasherMain
from sofascrape.quality.storage import StorageHandler

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force=True,
)
# Use the CORRECT logger name from the debug output
storage_logger = logging.getLogger("sofascrape.quality.core.hash_generator")
storage_logger.setLevel(logging.DEBUG)


@pytest.fixture
def run1_data() -> sofaschema.SeasonScrapingResult:
    storage_handler = StorageHandler(tournament_id=1, season_id=123456)
    return storage_handler.load_run(run_number=1)


def test_basic_setup(run1_data):
    print()
    print("- -" * 50)
    print("basic setup")
    cfg = OmegaConf.load(
        "/home/james/bet_project/sofascrape/src/sofascrape/quality/config/quality_config.yaml"
    )
    print(OmegaConf.to_yaml(cfg))
    hasher = HasherMain()
    hasher.process_all("data")

    print(run1_data.season_id)
