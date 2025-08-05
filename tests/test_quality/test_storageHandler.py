import logging
from typing import Any, Dict, List, Optional, Union

import pytest
from omegaconf import DictConfig, OmegaConf

from sofascrape.quality.storage import StorageHandler

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force=True,
)
# Use the CORRECT logger name from the debug output
storage_logger = logging.getLogger("sofascrape.quality.storage.run_storage")
storage_logger.setLevel(logging.DEBUG)


@pytest.mark.skip(reason="Not needed currently")
def test_basic_setup():
    print("basic setup")
    cfg = OmegaConf.load(
        "/home/james/bet_project/sofascrape/src/sofascrape/quality/config/quality_config.yaml"
    )
    print(OmegaConf.to_yaml(cfg))

    storage_handler = StorageHandler(tournament_id=1, season_id=123456)

    assert storage_handler.tournament_id == 1, "tournament_id not set."
    assert storage_handler.season_id == 123456, "season_id not set."
    assert storage_handler.config == cfg, "config not loaded and set"


@pytest.mark.skip(reason="Not needed currently")
def test_saving():
    print()
    print("- -" * 50)

    storage_handler = StorageHandler(tournament_id=1, season_id=123456)

    example_data1: Dict[str, Union[str, int, bool]] = {
        "key1": 1,
        "key2": "me",
        "key3": False,
    }
    example_data2: Dict[str, Union[str, int, bool]] = {
        "key1": 2,
        "key2": "me",
        "key3": True,
    }
    storage_handler.save_scraping_run(run_data=example_data1)
    storage_handler.save_scraping_run(run_data=example_data2)


def test_loading_run():
    print()
    print("- -" * 50)
    storage_handler = StorageHandler(tournament_id=1, season_id=123456)
    data_1 = storage_handler.load_run(run_number=1)
    print(data_1)

    result = storage_handler.load_run(run_number=999)  # Invalid run number
    assert result is None

    run_resluts = storage_handler.load_avaiable_runs()

    print(run_resluts)


@pytest.mark.skip(reason="Not needed currently")
def test_notebook():
    print()
    print("- -" * 50)
    storage_handler = StorageHandler(tournament_id=1, season_id=123456)

    print(storage_handler._get_runs())
