import logging

import pytest
from omegaconf import DictConfig, OmegaConf

from sofascrape.quality.storage import StorageHandler


def test_basic_setup():
    # Configure logging for the specific module
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        force=True,
    )

    # Use the CORRECT logger name from the debug output
    storage_logger = logging.getLogger("sofascrape.quality.storage.run_storage")
    storage_logger.setLevel(logging.DEBUG)

    print("basic setup")
    cfg = OmegaConf.load(
        "/home/james/bet_project/sofascrape/src/sofascrape/quality/config/quality_config.yaml"
    )
    print(OmegaConf.to_yaml(cfg))

    storage_handler = StorageHandler(tournament_id=1, season_id=123456)

    assert storage_handler.tournament_id == 1, "tournament_id not set."
    assert storage_handler.season_id == 123456, "season_id not set."
    assert storage_handler.config == cfg, "config not loaded and set"

    print(storage_handler._get_runs())

    print(storage_handler._next_run_number())
