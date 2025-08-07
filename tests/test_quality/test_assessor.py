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


# miss parts
# {
#     12476988: MatchConsensusResult(
#         match_id=12476988,
#         run_ids=["2", "3"],
#         has_consensus=False,
#         retry_components=["incidents"],
#         component_results={
#             "graph": ComponentConsensusResult(
#                 name="graph",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "stats": ComponentConsensusResult(
#                 name="stats",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "lineup": ComponentConsensusResult(
#                 name="lineup",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "base": ComponentConsensusResult(
#                 name="base",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "incidents": ComponentConsensusResult(
#                 name="incidents",
#                 has_consensus=False,
#                 agreed_pairs=[],
#                 disagreed_pairs=[("2", "3")],
#             ),
#         },
#     ),
#     12476993: MatchConsensusResult(
#         match_id=12476993,
#         run_ids=["2", "3"],
#         has_consensus=False,
#         retry_components=["incidents"],
#         component_results={
#             "graph": ComponentConsensusResult(
#                 name="graph",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "stats": ComponentConsensusResult(
#                 name="stats",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "lineup": ComponentConsensusResult(
#                 name="lineup",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "base": ComponentConsensusResult(
#                 name="base",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "incidents": ComponentConsensusResult(
#                 name="incidents",
#                 has_consensus=False,
#                 agreed_pairs=[],
#                 disagreed_pairs=[("2", "3")],
#             ),
#         },
#     ),
#     12476994: MatchConsensusResult(
#         match_id=12476994,
#         run_ids=["2", "3"],
#         has_consensus=False,
#         retry_components=["incidents"],
#         component_results={
#             "graph": ComponentConsensusResult(
#                 name="graph",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "stats": ComponentConsensusResult(
#                 name="stats",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "lineup": ComponentConsensusResult(
#                 name="lineup",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "base": ComponentConsensusResult(
#                 name="base",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "incidents": ComponentConsensusResult(
#                 name="incidents",
#                 has_consensus=False,
#                 agreed_pairs=[],
#                 disagreed_pairs=[("2", "3")],
#             ),
#         },
#     ),
#     12476998: MatchConsensusResult(
#         match_id=12476998,
#         run_ids=["1", "2", "3"],
#         has_consensus=False,
#         retry_components=["incidents"],
#         component_results={
#             "graph": ComponentConsensusResult(
#                 name="graph",
#                 has_consensus=True,
#                 agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "stats": ComponentConsensusResult(
#                 name="stats",
#                 has_consensus=True,
#                 agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "lineup": ComponentConsensusResult(
#                 name="lineup",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[("1", "2"), ("1", "3")],
#             ),
#             "base": ComponentConsensusResult(
#                 name="base",
#                 has_consensus=True,
#                 agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "incidents": ComponentConsensusResult(
#                 name="incidents",
#                 has_consensus=False,
#                 agreed_pairs=[],
#                 disagreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#             ),
#         },
#     ),
#     12477008: MatchConsensusResult(
#         match_id=12477008,
#         run_ids=["2", "3"],
#         has_consensus=False,
#         retry_components=["incidents"],
#         component_results={
#             "graph": ComponentConsensusResult(
#                 name="graph",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "stats": ComponentConsensusResult(
#                 name="stats",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "lineup": ComponentConsensusResult(
#                 name="lineup",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "base": ComponentConsensusResult(
#                 name="base",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "incidents": ComponentConsensusResult(
#                 name="incidents",
#                 has_consensus=False,
#                 agreed_pairs=[],
#                 disagreed_pairs=[("2", "3")],
#             ),
#         },
#     ),
#     12477033: MatchConsensusResult(
#         match_id=12477033,
#         run_ids=["1", "2", "3"],
#         has_consensus=False,
#         retry_components=["incidents"],
#         component_results={
#             "graph": ComponentConsensusResult(
#                 name="graph",
#                 has_consensus=True,
#                 agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "stats": ComponentConsensusResult(
#                 name="stats",
#                 has_consensus=True,
#                 agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "lineup": ComponentConsensusResult(
#                 name="lineup",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[("1", "2"), ("1", "3")],
#             ),
#             "base": ComponentConsensusResult(
#                 name="base",
#                 has_consensus=True,
#                 agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "incidents": ComponentConsensusResult(
#                 name="incidents",
#                 has_consensus=False,
#                 agreed_pairs=[],
#                 disagreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#             ),
#         },
#     ),
#     12477053: MatchConsensusResult(
#         match_id=12477053,
#         run_ids=["2", "3"],
#         has_consensus=False,
#         retry_components=["incidents"],
#         component_results={
#             "graph": ComponentConsensusResult(
#                 name="graph",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "stats": ComponentConsensusResult(
#                 name="stats",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "lineup": ComponentConsensusResult(
#                 name="lineup",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "base": ComponentConsensusResult(
#                 name="base",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "incidents": ComponentConsensusResult(
#                 name="incidents",
#                 has_consensus=False,
#                 agreed_pairs=[],
#                 disagreed_pairs=[("2", "3")],
#             ),
#         },
#     ),
#     12477063: MatchConsensusResult(
#         match_id=12477063,
#         run_ids=["2", "3"],
#         has_consensus=False,
#         retry_components=["incidents"],
#         component_results={
#             "graph": ComponentConsensusResult(
#                 name="graph",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "stats": ComponentConsensusResult(
#                 name="stats",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "lineup": ComponentConsensusResult(
#                 name="lineup",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "base": ComponentConsensusResult(
#                 name="base",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "incidents": ComponentConsensusResult(
#                 name="incidents",
#                 has_consensus=False,
#                 agreed_pairs=[],
#                 disagreed_pairs=[("2", "3")],
#             ),
#         },
#     ),
#     12477065: MatchConsensusResult(
#         match_id=12477065,
#         run_ids=["1", "2", "3"],
#         has_consensus=False,
#         retry_components=["incidents"],
#         component_results={
#             "graph": ComponentConsensusResult(
#                 name="graph",
#                 has_consensus=True,
#                 agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "stats": ComponentConsensusResult(
#                 name="stats",
#                 has_consensus=True,
#                 agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "lineup": ComponentConsensusResult(
#                 name="lineup",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[("1", "2"), ("1", "3")],
#             ),
#             "base": ComponentConsensusResult(
#                 name="base",
#                 has_consensus=True,
#                 agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "incidents": ComponentConsensusResult(
#                 name="incidents",
#                 has_consensus=False,
#                 agreed_pairs=[],
#                 disagreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#             ),
#         },
#     ),
#     12477066: MatchConsensusResult(
#         match_id=12477066,
#         run_ids=["2", "3"],
#         has_consensus=False,
#         retry_components=["incidents"],
#         component_results={
#             "graph": ComponentConsensusResult(
#                 name="graph",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "stats": ComponentConsensusResult(
#                 name="stats",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "lineup": ComponentConsensusResult(
#                 name="lineup",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "base": ComponentConsensusResult(
#                 name="base",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "incidents": ComponentConsensusResult(
#                 name="incidents",
#                 has_consensus=False,
#                 agreed_pairs=[],
#                 disagreed_pairs=[("2", "3")],
#             ),
#         },
#     ),
#     12477075: MatchConsensusResult(
#         match_id=12477075,
#         run_ids=["1", "2", "3"],
#         has_consensus=False,
#         retry_components=["incidents"],
#         component_results={
#             "graph": ComponentConsensusResult(
#                 name="graph",
#                 has_consensus=True,
#                 agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "stats": ComponentConsensusResult(
#                 name="stats",
#                 has_consensus=True,
#                 agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "lineup": ComponentConsensusResult(
#                 name="lineup",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[("1", "2"), ("1", "3")],
#             ),
#             "base": ComponentConsensusResult(
#                 name="base",
#                 has_consensus=True,
#                 agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "incidents": ComponentConsensusResult(
#                 name="incidents",
#                 has_consensus=False,
#                 agreed_pairs=[],
#                 disagreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#             ),
#         },
#     ),
#     12477096: MatchConsensusResult(
#         match_id=12477096,
#         run_ids=["1", "2", "3"],
#         has_consensus=False,
#         retry_components=["incidents"],
#         component_results={
#             "graph": ComponentConsensusResult(
#                 name="graph",
#                 has_consensus=True,
#                 agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "stats": ComponentConsensusResult(
#                 name="stats",
#                 has_consensus=True,
#                 agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "lineup": ComponentConsensusResult(
#                 name="lineup",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[("1", "2"), ("1", "3")],
#             ),
#             "base": ComponentConsensusResult(
#                 name="base",
#                 has_consensus=True,
#                 agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "incidents": ComponentConsensusResult(
#                 name="incidents",
#                 has_consensus=False,
#                 agreed_pairs=[],
#                 disagreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#             ),
#         },
#     ),
#     12477098: MatchConsensusResult(
#         match_id=12477098,
#         run_ids=["2", "3"],
#         has_consensus=False,
#         retry_components=["incidents"],
#         component_results={
#             "graph": ComponentConsensusResult(
#                 name="graph",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "stats": ComponentConsensusResult(
#                 name="stats",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "lineup": ComponentConsensusResult(
#                 name="lineup",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "base": ComponentConsensusResult(
#                 name="base",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "incidents": ComponentConsensusResult(
#                 name="incidents",
#                 has_consensus=False,
#                 agreed_pairs=[],
#                 disagreed_pairs=[("2", "3")],
#             ),
#         },
#     ),
#     12477108: MatchConsensusResult(
#         match_id=12477108,
#         run_ids=["1", "2", "3"],
#         has_consensus=False,
#         retry_components=["graph"],
#         component_results={
#             "graph": ComponentConsensusResult(
#                 name="graph",
#                 has_consensus=False,
#                 agreed_pairs=[],
#                 disagreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#             ),
#             "stats": ComponentConsensusResult(
#                 name="stats",
#                 has_consensus=True,
#                 agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "lineup": ComponentConsensusResult(
#                 name="lineup",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[("1", "2"), ("1", "3")],
#             ),
#             "base": ComponentConsensusResult(
#                 name="base",
#                 has_consensus=True,
#                 agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "incidents": ComponentConsensusResult(
#                 name="incidents",
#                 has_consensus=True,
#                 agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#                 disagreed_pairs=[],
#             ),
#         },
#     ),
#     12477110: MatchConsensusResult(
#         match_id=12477110,
#         run_ids=["1", "2", "3"],
#         has_consensus=False,
#         retry_components=["incidents"],
#         component_results={
#             "graph": ComponentConsensusResult(
#                 name="graph",
#                 has_consensus=True,
#                 agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "stats": ComponentConsensusResult(
#                 name="stats",
#                 has_consensus=True,
#                 agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "lineup": ComponentConsensusResult(
#                 name="lineup",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[("1", "2"), ("1", "3")],
#             ),
#             "base": ComponentConsensusResult(
#                 name="base",
#                 has_consensus=True,
#                 agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "incidents": ComponentConsensusResult(
#                 name="incidents",
#                 has_consensus=False,
#                 agreed_pairs=[],
#                 disagreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#             ),
#         },
#     ),
#     12477122: MatchConsensusResult(
#         match_id=12477122,
#         run_ids=["1", "2", "3"],
#         has_consensus=False,
#         retry_components=["incidents"],
#         component_results={
#             "graph": ComponentConsensusResult(
#                 name="graph",
#                 has_consensus=True,
#                 agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "stats": ComponentConsensusResult(
#                 name="stats",
#                 has_consensus=True,
#                 agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "lineup": ComponentConsensusResult(
#                 name="lineup",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[("1", "2"), ("1", "3")],
#             ),
#             "base": ComponentConsensusResult(
#                 name="base",
#                 has_consensus=True,
#                 agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "incidents": ComponentConsensusResult(
#                 name="incidents",
#                 has_consensus=False,
#                 agreed_pairs=[],
#                 disagreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
#             ),
#         },
#     ),
#     12477112: MatchConsensusResult(
#         match_id=12477112,
#         run_ids=["2", "3"],
#         has_consensus=False,
#         retry_components=["graph"],
#         component_results={
#             "graph": ComponentConsensusResult(
#                 name="graph",
#                 has_consensus=False,
#                 agreed_pairs=[],
#                 disagreed_pairs=[("2", "3")],
#             ),
#             "stats": ComponentConsensusResult(
#                 name="stats",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "lineup": ComponentConsensusResult(
#                 name="lineup",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "base": ComponentConsensusResult(
#                 name="base",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#             "incidents": ComponentConsensusResult(
#                 name="incidents",
#                 has_consensus=True,
#                 agreed_pairs=[("2", "3")],
#                 disagreed_pairs=[],
#             ),
#         },
#     ),
# }
#

test_match_dict = {
    12476988: MatchConsensusResult(
        match_id=12476988,
        run_ids=["2", "3"],
        has_consensus=False,
        retry_components=["incidents"],
        component_results={
            "graph": ComponentConsensusResult(
                name="graph",
                has_consensus=True,
                agreed_pairs=[("2", "3")],
                disagreed_pairs=[],
            ),
            "stats": ComponentConsensusResult(
                name="stats",
                has_consensus=True,
                agreed_pairs=[("2", "3")],
                disagreed_pairs=[],
            ),
            "lineup": ComponentConsensusResult(
                name="lineup",
                has_consensus=True,
                agreed_pairs=[("2", "3")],
                disagreed_pairs=[],
            ),
            "base": ComponentConsensusResult(
                name="base",
                has_consensus=True,
                agreed_pairs=[("2", "3")],
                disagreed_pairs=[],
            ),
            "incidents": ComponentConsensusResult(
                name="incidents",
                has_consensus=False,
                agreed_pairs=[],
                disagreed_pairs=[("2", "3")],
            ),
        },
    ),
    12476980: MatchConsensusResult(
        match_id=12476980,
        run_ids=["2", "3"],
        has_consensus=True,
        retry_components=[],
        component_results={
            "graph": ComponentConsensusResult(
                name="graph",
                has_consensus=True,
                agreed_pairs=[("2", "3")],
                disagreed_pairs=[],
            ),
            "stats": ComponentConsensusResult(
                name="stats",
                has_consensus=True,
                agreed_pairs=[("2", "3")],
                disagreed_pairs=[],
            ),
            "lineup": ComponentConsensusResult(
                name="lineup",
                has_consensus=True,
                agreed_pairs=[("2", "3")],
                disagreed_pairs=[],
            ),
            "base": ComponentConsensusResult(
                name="base",
                has_consensus=True,
                agreed_pairs=[("2", "3")],
                disagreed_pairs=[],
            ),
            "incidents": ComponentConsensusResult(
                name="incidents",
                has_consensus=True,
                agreed_pairs=[("2", "3")],
                disagreed_pairs=[],
            ),
        },
    ),
    12476946: MatchConsensusResult(
        match_id=12476946,
        run_ids=["1", "2", "3"],
        has_consensus=True,
        retry_components=[],
        component_results={
            "graph": ComponentConsensusResult(
                name="graph",
                has_consensus=True,
                agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
                disagreed_pairs=[],
            ),
            "stats": ComponentConsensusResult(
                name="stats",
                has_consensus=True,
                agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
                disagreed_pairs=[],
            ),
            "lineup": ComponentConsensusResult(
                name="lineup",
                has_consensus=True,
                agreed_pairs=[("2", "3")],
                disagreed_pairs=[("1", "2"), ("1", "3")],
            ),
            "base": ComponentConsensusResult(
                name="base",
                has_consensus=True,
                agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
                disagreed_pairs=[],
            ),
            "incidents": ComponentConsensusResult(
                name="incidents",
                has_consensus=True,
                agreed_pairs=[("1", "2"), ("1", "3"), ("2", "3")],
                disagreed_pairs=[],
            ),
        },
    ),
}

test_consensus = SeasonConsensusResult(
    tournament_id=1,
    season_id=123456,
    run_ids=["1", "2", "3"],
    match_results=test_match_dict,
    matches_in_single_run_only=[12476947],
)


@pytest.fixture
def load_consensus() -> SeasonConsensusResult:
    sh = StorageHandler(1, 123456)
    return sh.load_most_current_consensus()


def test_basic_setup():
    print()
    print("- -" * 50)
    print("basic setup")
    print(test_consensus.season_summary)

    print(test_consensus.get_retry_dict())

    print(test_consensus.get_golden_dataset_dict())
    # print(
    #     {
    #         key: item
    #         for key, item in list(load_consensus.match_results.items())
    #         if item.has_consensus is False
    #     }
    # )
    #
    # dict with has_ consensus is false
    #    print({key: item for key, item in list(load_consensus.match_results.items())[0:3]})


def test_details(load_consensus):
    print()
    print("- -" * 50)
    print("basic setup")
    print(load_consensus.season_summary)

    print(".." * 50)
    print(json.dumps(load_consensus.get_retry_dict(), indent=6))
    print(".." * 50)
    print(json.dumps(load_consensus.get_golden_dataset_dict(), indent=6))
