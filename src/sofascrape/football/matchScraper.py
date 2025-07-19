import logging
from typing import Dict

from webdriver import MyWebDriver

import sofascrape.schemas.general as schemas
from sofascrape.abstract.base import BaseMatchScraper

logger = logging.getLogger(__name__)


class MatchFootballScraper(BaseMatchScraper):
    def __init__(self, webdriver: MyWebDriver, matchid: int) -> None:
        super().__init__(webdriver=webdriver, matchid=matchid)

    def get_base(self):
        pass

    def scrape(self):
        pass
