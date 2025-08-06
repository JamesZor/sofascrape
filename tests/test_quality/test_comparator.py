import json
import logging
from dataclasses import asdict
from typing import Any, Dict, List, Optional, Union

import pytest
from omegaconf import DictConfig, OmegaConf

import sofascrape.schemas.general as sofaschema
from sofascrape.quality.core.comparator import Comparator
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


@pytest.fixture
def run_data() -> dict[str, sofaschema.SeasonScrapingResult]:
    storage_handler = StorageHandler(tournament_id=1, season_id=123456)
    return storage_handler.load_avaiable_runs()


@pytest.mark.skip(reason="Not needed currently")
def test_basic_setup():
    print()
    print("- -" * 50)
    print("basic setup")
    cfg = OmegaConf.load(
        "/home/james/bet_project/sofascrape/src/sofascrape/quality/config/quality_config.yaml"
    )
    print(OmegaConf.to_yaml(cfg))

    comp = Comparator()


@pytest.mark.skip(reason="Not needed currently, needs modifying")
def test_compare_base(run1_data, run2_data):
    print()
    print("- -" * 50)
    print("test compare base")
    match_1_data = sorted(run1_data.matches, key=lambda x: x.match_id)[0].data
    match_2_data = sorted(run2_data.matches, key=lambda x: x.match_id)[0].data

    print(match_1_data.match_id, match_2_data.match_id)
    c = Comparator()

    print(
        "result of compare base:",
        c.compare_base(match1=match_1_data, match2=match_2_data),
    )

    match_2_data.base.event.awayScore = 10

    print(
        "result of compare base modified m2:",
        c.compare_base(match1=match_1_data, match2=match_2_data),
    )


@pytest.mark.skip(reason="Not needed currently")
def test_compare_all_match(run_data):
    print()
    print("- -" * 50)
    print("test compare all compents")
    match_1_data = sorted(run_data["1"].matches, key=lambda x: x.match_id)[0].data
    match_2_data = sorted(run_data["2"].matches, key=lambda x: x.match_id)[0].data
    match_3_data = sorted(run_data["3"].matches, key=lambda x: x.match_id)[0].data

    print(match_1_data.match_id, match_2_data.match_id)
    c = Comparator()
    print(
        "result of compare base r1 -r2:",
        c.compare_all_components(match1=match_1_data, match2=match_2_data),
    )
    print(
        "result of compare base r1 -r3:",
        c.compare_all_components(match1=match_1_data, match2=match_3_data),
    )
    print(
        "result of compare base r2 -r3:",
        c.compare_all_components(match1=match_2_data, match2=match_3_data),
    )
    m1_d = match_1_data.lineup.model_dump()
    m2_d = match_2_data.lineup.model_dump()
    m3_d = match_3_data.lineup.model_dump()
    print(m1_d["home"].keys())

    print(json.dumps(c.find_dict_differences(m3_d["home"], m2_d["home"]), indent=8))


def test_matchs_consensus(run_data):
    print()
    print("- -" * 50)
    print("test compare all compents")
    match_1_data = sorted(run_data["1"].matches, key=lambda x: x.match_id)[0].data
    match_2_data = sorted(run_data["2"].matches, key=lambda x: x.match_id)[0].data
    match_3_data = sorted(run_data["3"].matches, key=lambda x: x.match_id)[0].data

    match_run_dict = {"1": match_1_data, "2": match_2_data, "3": match_3_data}

    print(match_1_data.match_id, match_2_data.match_id)
    c = Comparator()
    results = c.build_match_consensus(
        match_id=match_1_data.match_id, matches_from_runs=match_run_dict
    )

    print(json.dumps(asdict(results), indent=3))

    match_1_data_fail = sorted(run_data["1"].matches, key=lambda x: x.match_id)[0].data
    match_2_data_fail = sorted(run_data["2"].matches, key=lambda x: x.match_id)[1].data
    match_3_data_fail = sorted(run_data["3"].matches, key=lambda x: x.match_id)[2].data

    match_run_dict_fail = {
        "1": match_1_data_fail,
        "2": match_2_data_fail,
        "3": match_3_data_fail,
    }
    results_fail = c.build_match_consensus(
        match_id=match_1_data.match_id, matches_from_runs=match_run_dict_fail
    )

    print(json.dumps(asdict(results_fail), indent=3))
