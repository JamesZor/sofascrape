import logging
from abc import ABC, abstractmethod
from typing import Dict, Generic, Optional, TypeVar

from hydra import compose, initialize
from omegaconf import DictConfig
from pydantic import BaseModel
from webdriver import ManagerWebdriver, MyWebDriver

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class BaseComponentScraper(ABC, Generic[T]):
    """
    Lower level componet. Gets the data and parse it.
    """

    def __init__(self, webdriver: MyWebDriver, cfg: Optional[DictConfig] = None):
        self.webdriver: MyWebDriver = webdriver

        self.cfg: DictConfig = cfg if cfg is not None else self._get_cfg()
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

    def _get_cfg(self) -> DictConfig:
        with initialize(config_path="../conf/", version_base="1.3"):
            cfg = compose(config_name="general")
        return cfg

    # TODO
    def _validate_inputs(self, tournamentid: int, webdriver: MyWebDriver) -> None:
        """Validate constructor inputs."""
        if not isinstance(webdriver, MyWebDriver):
            raise ValueError("Correct webdriver needs to be passed.")

        if not isinstance(tournamentid, int) or tournamentid < 0:
            raise ValueError(
                f"tournamentid must be a positive integer. "
                f"Got: {tournamentid=}, {type(tournamentid)=}"
            )


class BaseMatchScraper(ABC):
    def __init__(
        self, webdriver: MyWebDriver, matchid: int, cfg: Optional[DictConfig] = None
    ):
        self.webdriver: MyWebDriver = webdriver
        self.matchid: int = matchid
        self.cfg: DictConfig = cfg if cfg is not None else self._get_cfg()

    def _get_cfg(self) -> DictConfig:
        with initialize(config_path="../conf/", version_base="1.3"):
            cfg = compose(config_name="general")
        return cfg

    @abstractmethod
    def scrape(self):
        """Orchestrate scraping of all components for a match."""
        pass


class BaseSeasonScraper(ABC):
    def __init__(
        self,
        tournamentid: int,
        seasonid: int,
        managerwebdriver: Optional[ManagerWebdriver] = None,
        cfg: Optional[DictConfig] = None,
    ) -> None:
        self.tournamentid: int = tournamentid
        self.seasonid: int = seasonid
        self.mw: ManagerWebdriver = (
            managerwebdriver if managerwebdriver is not None else ManagerWebdriver()
        )
        self.cfg: DictConfig = cfg if cfg is not None else self._get_cfg()

    def _get_cfg(self) -> DictConfig:
        with initialize(config_path="../conf/", version_base="1.3"):
            cfg = compose(config_name="general")
        return cfg

    @abstractmethod
    def scrape(self):
        """Scrape all matches in a season."""
        pass


class BaseLeagueScraper(ABC):
    def __init__(
        self,
        tournamentid: int,
        managerwebdriver: Optional[ManagerWebdriver] = None,
        cfg: Optional[DictConfig] = None,
    ) -> None:
        self.tournamentid: int = tournamentid
        self.mw: ManagerWebdriver = (
            managerwebdriver if managerwebdriver is not None else ManagerWebdriver()
        )
        self.cfg: DictConfig = cfg if cfg is not None else self._get_cfg()

    def _get_cfg(self) -> DictConfig:
        with initialize(config_path="../conf/", version_base="1.3"):
            cfg = compose(config_name="general")
        return cfg

    @abstractmethod
    def scrape(self):
        """Scrape all seasons (and thus all matches) in a league."""
        pass
