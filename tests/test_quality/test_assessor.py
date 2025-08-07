import json
import logging
from dataclasses import asdict
from typing import Any, Dict, List, Optional, Union

import pytest
from omegaconf import DictConfig, OmegaConf

import sofascrape.schemas.general as sofaschema
from sofascrape.quality.core.assessor import SeasonAssessor
from sofascrape.quality.core.comparator import Comparator
from sofascrape.quality.core.dataclasses import (
    ComponentConsensusResult,
    MatchConsensusResult,
    SeasonConsensusResult,
)
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
def load_consensus() -> SeasonConsensusResult:
    sh = StorageHandler(1, 123456)
    return sh.load_most_current_consensus()


def test_basic_setup():
    print()
    print("- -" * 50)
    print("basic setup")

    sh = StorageHandler(1, 123456)
    print(sh.list_consensus_files())
    # load_consensus.season_summary()

    a = SeasonAssessor()
