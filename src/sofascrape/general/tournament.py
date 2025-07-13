import json
import logging
from typing import Dict

from hydra import compose, initialize
from omegaconf import DictConfig
from webdriver import MyWebDriver

from sofascrape.abstract.base import BaseComponentScraper

from pydantic import



logger = logging.getLogger(__name__)


class TournamentComponentScraper(BaseComponentScraper):

    def __init__(self, tournamentid: int, webdriver: MyWebDriver) -> None:

        if not isinstance(webdriver, MyWebDriver):
            raise ValueError("Correct webdrive needs to be passed.")

        if (not isinstance(tournamentid, int)) or (tournamentid < 0):
            raise ValueError(
                f"tournamentid must be an positive integer {tournamentid =}, {type(tournamentid) =}."
            )
        self.tournamentid: int = tournamentid
        self.webdrive: MyWebDriver = webdriver
        self.cfg: DictConfig = self._get_cfg()
        self.page_url: str = self.cfg.links.tournament_empty.format(
            tournamentID=tournamentid
        )

    def _get_cfg(self) -> DictConfig:
        with initialize(config_path="../conf/", version_base="1.3"):
            cfg = compose(config_name="general")
        return cfg

    def get_data(self) -> Dict:
        try:
            tournament_page_data = self.webdrive.get_page(self.page_url)

            if not isinstance(tournament_page_data, Dict):
                error_tournament_data: str = (
                    f"Tournamaent page data is incorrect. {tournament_page_data =}, {type(tournament_page_data)=}, for {self.webdrive.set_proxy =}, {self.tournamentid =}."
                )
                logger.warning(error_tournament_data)
                raise ValueError(error_tournament_data)

        except Exception as e:
            raise e
        return tournament_page_data

    def parse_data(self) -> Dict:
        return {}

    def process(self):
        pass
