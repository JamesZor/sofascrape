import json
import logging
from typing import Any, Dict, List, Optional, Union

import pytest
from omegaconf import DictConfig, OmegaConf

import sofascrape.schemas.general as sofaschema
from sofascrape.loader.footballloader import FootballLoader

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force=True,
)


def test_basic_setup():
    print()
    print("- -" * 50)
    print("basic setup")
    loader = FootballLoader()
    print(loader.discover_available_data())


def test_basic_load():
    print()
    print("- -" * 50)
    print("basic setup")
    loader = FootballLoader()
    example_load = {55: {62411}}
    data = loader.load_tournament_seasons(example_load)

    for t_id, item in data.items():
        for s_id, match_data in item.items():
            # Convert dict.items() to list before slicing
            for m_id, m in list(match_data.items())[0:20]:
                base: sofaschema.footballeventschema = m.base.event
                print(
                    f"t {t_id} | s{s_id} | m {m_id}  = {base.homeTeam.slug} : {base.homeScore.normaltime} - {base.awayTeam.slug} : {base.awayScore.normaltime} ."
                )


def test_basic_load_two():
    print()
    print("- -" * 50)
    print("basic setup")
    loader = FootballLoader()
    example_load = {55: {62411, 52606}}
    data = loader.load_tournament_seasons(example_load)

    for t_id, item in data.items():
        for s_id, match_data in item.items():
            # Convert dict.items() to list before slicing
            for m_id, m in list(match_data.items())[0:20]:
                base: sofaschema.footballeventschema = m.base.event
                print(
                    f"t {t_id} | s{s_id} | m {m_id}  = {base.homeTeam.slug} : {base.homeScore.normaltime} - {base.awayTeam.slug} : {base.awayScore.normaltime} ."
                )
            print("-" * 50)
