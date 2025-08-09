import json
import logging
from dataclasses import asdict
from typing import Any, Dict, List, Optional, Union

import pytest
from omegaconf import DictConfig, OmegaConf

import sofascrape.schemas.general as sofaschema
from sofascrape.quality.core.comparator import Comparator
from sofascrape.quality.core.dataclasses import SeasonConsensusResult
from sofascrape.quality.manager import SeasonQualityManager
from sofascrape.quality.storage import StorageHandler

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force=True,
)
# Use the CORRECT logger name from the debug output
manager_logger = logging.getLogger("sofascrape.quality.manager")
manager_logger.setLevel(logging.DEBUG)
# Silence the noisy ones
logging.getLogger("webdriver").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("selenium").setLevel(logging.WARNING)
logging.getLogger("webdriver.core").setLevel(logging.WARNING)
logging.getLogger("webdriver.core.mywebdriver").setLevel(logging.WARNING)
logging.getLogger("webdriver.core.options").setLevel(logging.WARNING)
logging.getLogger("webdriver.core.proxy_manager").setLevel(logging.WARNING)

logging.getLogger("sofascrape.football.season").setLevel(logging.WARNING)
logging.getLogger("sofascrape.football.LineupComonent").setLevel(logging.WARNING)
logging.getLogger("sofascrape.football.base").setLevel(logging.WARNING)
logging.getLogger("sofascrape.football.matchScraper").setLevel(logging.WARNING)
logging.getLogger("sofascrape.football.incidentsComponent").setLevel(logging.WARNING)
logging.getLogger("sofascrape.football.graphComponent").setLevel(logging.WARNING)
logging.getLogger("sofascrape.football.LineupComonent").setLevel(logging.WARNING)
logging.getLogger("sofascrape.abstract.base").setLevel(logging.WARNING)
"""
2025-08-08 19:00:16,937 - sofascrape.football.statsComponent - INFO - Successfully parsed stats data for event self.matchid =11395689.
2025-08-08 19:00:16,993 - sofascrape.football.lineupComonent - INFO - Successfully parsed lineup data for event self.matchid =11395673.
"""


@pytest.mark.skip()
def test_basic_setup():
    print()
    print("- -" * 50)
    print("basic setup")
    season_quaility_manager = SeasonQualityManager(tournament_id=1, season_id=123456)


@pytest.mark.skip()
def test_comparator():
    print()
    print("- -" * 50)
    print("Test comparator")
    season_quaility_manager = SeasonQualityManager(tournament_id=1, season_id=123456)
    consensus: SeasonConsensusResult = (
        season_quaility_manager.build_consensus_analysis()
    )
    print(consensus.season_summary)

    print(" .. " * 50)
    print("retry dict")
    print(len(consensus.get_retry_dict()))
    print(" .. " * 50)
    print("golden dict ")


#    print(consensus.get_golden_dataset_dict())


@pytest.mark.skip()
def test_golden():
    print()
    print("- -" * 50)
    print("Test golden")
    season_quaility_manager = SeasonQualityManager(tournament_id=1, season_id=123456)
    consensus = season_quaility_manager.storage.load_most_current_consensus()
    print(consensus.season_summary)

    data = season_quaility_manager.build_golden_seaosn_dataset(
        golden_dict=consensus.get_golden_dataset_dict()
    )

    print(f" {len(data.keys())}")
    # NOTE: before retry scrape we had 172 items


@pytest.mark.skip()
def test_retry_scrape():
    print()
    print("- -" * 50)
    print("Test golden")
    season_quaility_manager = SeasonQualityManager(tournament_id=1, season_id=123456)
    consensus = season_quaility_manager.storage.load_most_current_consensus()
    retry = consensus.get_retry_dict()

    season_quaility_manager.execute_scraping_retry(retry)


@pytest.mark.skip()
def test_scot_champ():
    """
    name='Championship 24/25' id=62411 year='24/25'
    name='Championship 23/24' id=52606 year='23/24'
    name='Championship 22/23' id=41958 year='22/23'
    name='Championship 21/22' id=37030 year='21/22'
    name='Championship 20/21' id=29268 year='20/21'
    name='Championship 19/20' id=23988 year='19/20'
    name='Championship 18/19' id=17366 year='18/19'
    name='Championship 17/18' id=13458 year='17/18'
    name='Championship 16/17' id=11765 year='16/17'
    """
    season_quaility_manager = SeasonQualityManager(tournament_id=55, season_id=52606)
    season_quaility_manager.execute_scraping_run()


# @pytest.mark.skip()
def test_comparator_scot_champ():
    print()
    print("- -" * 50)
    print("Test comparator")
    season_quaility_manager = SeasonQualityManager(tournament_id=55, season_id=52606)
    consensus: SeasonConsensusResult = (
        season_quaility_manager.build_consensus_analysis()
    )
    print(consensus.season_summary)

    print(" .. " * 50)
    print("retry dict")
    #    print(len(consensus.get_retry_dict()))
    print(" .. " * 50)
    print("golden dict ")
