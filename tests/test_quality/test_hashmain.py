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


@pytest.fixture
def run2_data() -> sofaschema.SeasonScrapingResult:
    storage_handler = StorageHandler(tournament_id=1, season_id=123456)
    return storage_handler.load_run(run_number=3)


@pytest.mark.skip(reason="Not needed currently")
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


def test_process_base(run1_data, run2_data):
    print()
    print("- -" * 50)
    print("basic setup")
    hasher = HasherMain()
    match_1_data = sorted(run1_data.matches, key=lambda x: x.match_id)[0].data
    match_2_data = sorted(run2_data.matches, key=lambda x: x.match_id)[0].data

    print("match equal", match_1_data == match_2_data)
    print("base equal", match_1_data.base == match_2_data.base)
    print("stats equal", match_1_data.stats == match_2_data.stats)
    print("lineup equal", match_1_data.stats == match_2_data.stats)
    print("incidents equal", match_1_data.incidents == match_2_data.incidents)
    print("graph equal", match_1_data.graph == match_2_data.graph)
