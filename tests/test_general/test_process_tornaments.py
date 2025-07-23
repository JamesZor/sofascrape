import json
import logging

import pytest
from omegaconf import DictConfig, OmegaConf
from webdriver import ManagerWebdriver

from sofascrape.general import TournamentProcessScraper


def test_basic_run():
    s = TournamentProcessScraper()
    s.process()

    for t in s.tournaments:
        print(t.data)
