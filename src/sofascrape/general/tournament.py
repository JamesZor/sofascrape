import json

from hydra import compose, initialize
from omegaconf import DictConfig
from webdriver import MyWebDriver

from sofascrape.abstract.base import BaseComponentScraper


class Tournament(BaseComponentScraper):

    def __init__(self, webdriver: MyWebDriver) -> None:

        if not isinstance(webdriver, MyWebDriver):
            raise ValueError("Correct webdrive needs to be passed.")

        self.webdrive: MyWebDriver = webdriver
        self.cfg: DictConfig = self._get_cfg()

    def _get_cfg(self) -> DictConfig:
        with initialize(config_path="../conf/", version_base="1.3"):
            cfg = compose(config_name="general")
        return cfg

    def get_data(self):
        pass

    def parse_data(self):
        pass

    def process(self):
        pass
