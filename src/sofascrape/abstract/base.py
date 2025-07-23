import logging
from abc import ABC, abstractmethod
from typing import Dict, Generic, Optional, TypeVar

from hydra import compose, initialize
from omegaconf import DictConfig
from pydantic import BaseModel
from webdriver import ManagerWebdriver, MyWebDriver

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class BaseScraperModel:
    def __init__(self, cfg: Optional[DictConfig]) -> None:
        self.cfg: DictConfig = cfg if cfg is not None else self._get_cfg()

    def _get_cfg(self) -> DictConfig:
        with initialize(config_path="../conf/", version_base="1.3"):
            cfg = compose(config_name="general")
        return cfg


class BaseComponentScraper(BaseScraperModel, ABC, Generic[T]):
    """
    Lower level componet. Gets the data and parse it.
    """

    def __init__(self, webdriver: MyWebDriver, cfg: Optional[DictConfig] = None):
        super().__init__(cfg=cfg)
        self.webdriver: MyWebDriver = webdriver

        self.raw_data: Optional[Dict] = None
        self.data: Optional[T] = None

    @abstractmethod
    def get_data(self) -> None:
        """Fetch raw data from a source (e.g., API, web page)."""
        pass

    @abstractmethod
    def parse_data(self) -> None:
        """Parse the fetched data into structured form."""
        pass

    def process(self) -> T:
        """High-level workflow: fetch, parse, and return data."""
        try:
            logger.info(f"Starting processing...")
            self.get_data()

            # Moved validation to parent
            if self.raw_data is None:
                raise ValueError(
                    "No raw data available. get_data() failed to set raw_data."
                )

            self.parse_data()
            logger.info(f"Successfully processed data")
            return self.data
        except Exception as e:
            logger.error(f"Processing failed: {str(e)}")
            raise


class BaseMatchScraper(BaseScraperModel, ABC):
    def __init__(
        self, webdriver: MyWebDriver, matchid: int, cfg: Optional[DictConfig] = None
    ) -> None:
        super().__init__(cfg=cfg)
        self.webdriver: MyWebDriver = webdriver
        self.matchid: int = matchid

    @abstractmethod
    def scrape(self):
        """Orchestrate scraping of all components for a match."""
        pass


class BaseSeasonScraper(BaseScraperModel, ABC):
    def __init__(
        self,
        tournamentid: int,
        seasonid: int,
        managerwebdriver: Optional[ManagerWebdriver] = None,
        cfg: Optional[DictConfig] = None,
    ) -> None:
        super().__init__(cfg=cfg)
        self.tournamentid: int = tournamentid
        self.seasonid: int = seasonid
        self.mw: ManagerWebdriver = (
            managerwebdriver if managerwebdriver is not None else ManagerWebdriver()
        )
        self.data: Optional[T] = None

    @abstractmethod
    def scrape(self):
        """Scrape all matches in a season."""
        pass


class BaseLeagueScraper(BaseScraperModel, ABC):
    def __init__(
        self,
        tournamentid: int,
        managerwebdriver: Optional[ManagerWebdriver] = None,
        cfg: Optional[DictConfig] = None,
    ) -> None:
        super().__init__(cfg=cfg)
        self.tournamentid: int = tournamentid
        self.mw: ManagerWebdriver = (
            managerwebdriver if managerwebdriver is not None else ManagerWebdriver()
        )
        self.cfg: DictConfig = cfg if cfg is not None else self._get_cfg()

    @abstractmethod
    def scrape(self):
        """Scrape all seasons (and thus all matches) in a league."""
        pass


class BaseTournamentProcessor(BaseScraperModel, ABC):
    def __init__(
        self,
        managerwebdriver: Optional[ManagerWebdriver] = None,
        cfg: Optional[DictConfig] = None,
    ) -> None:
        super().__init__(cfg=cfg)
        self.mw: ManagerWebdriver = (
            managerwebdriver if managerwebdriver is not None else ManagerWebdriver()
        )
