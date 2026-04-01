import logging
from typing import Dict, Optional

from hydra import compose, initialize
from omegaconf import DictConfig
from pydantic import BaseModel, ValidationError
from webdriver import MyWebDriver

from sofascrape.abstract.base import BaseComponentScraper
from sofascrape.schemas.general import SeasonsListSchema

logger = logging.getLogger(__name__)


class SeasonsComponentScraper(BaseComponentScraper):

    def __init__(
        self,
        tournamentid: int,
        webdriver: MyWebDriver,
        cfg: Optional[DictConfig] = None,
    ) -> None:
        """Initialize the tournament scraper.

        Args:
            tournamentid: Positive integer ID of the tournament
            webdriver: WebDriver instance for scraping

        Raises:
            ValueError: If tournamentid is invalid or webdriver is wrong type
        """
        super().__init__(webdriver=webdriver, cfg=cfg)
        self.tournamentid: int = tournamentid
        self.webdriver: MyWebDriver = webdriver

        self.page_url: str = self.cfg.links.seasons_empty.format(
            tournamentID=tournamentid
        )

    def get_data(self):
        """Fetch the raw seasons lsit data from api.

        Raise:
            ValueError: If data is not in correct format.
            Exception: If getting data fails.
        """
        try:
            seaons_page_data = self.webdriver.get_page(self.page_url)

            if not isinstance(seaons_page_data, dict):
                raise ValueError("Data is not a dict")

            self.raw_data = seaons_page_data

        except Exception as e:
            logger.error(
                f"failed to get data for seasons, for tournament id {self.tournamentid}. {str(e)}."
            )

    def parse_data(self):
        """Parse raw data into structured format using Pydantic.

        Raises:
            ValueError: If raw_data is None
            ValidationError: If data doesn't match expected schema
        """
        try:
            self.data = SeasonsListSchema.model_validate(self.raw_data)
            logger.info(
                f"Successfully parsed data for seasons for tournament {self.tournamentid}."
            )
        except ValidationError as e:
            logger.warning(
                f"validation failed for seasons {self.tournamentid =}: {str(e)}."
            )
            raise

        except Exception as e:
            logger.error(
                f"Unexpected error parsing seasons, {self.tournamentid=}: {str(e)}."
            )
            raise


# %%
# --- IPython Playground ---
if __name__ == "__main__":
    import os

    from webdriver import ManagerWebdriver

    os.environ["DISPLAY"] = ":0"

    from sofascrape.conf.config import load_config
    from sofascrape.db.manager import DatabaseManager
    from sofascrape.general.seasons import SeasonsComponentScraper

    # 1. Load our new infrastructure
    config = load_config()
    db = DatabaseManager(config)

    # 2. Spin up your custom proxy-rotating webdriver
    print("Spawning Webdriver...")
    mw = ManagerWebdriver()
    driver = mw.spawn_webdriver()

    try:
        # 3. Let's try to scrape the Scottish Premiership (Assuming ID is 36, change if needed!)
        # Actually, let's use Tournament ID 17 (Premier League) or whatever you know works.
        target_id = 56

        print(f"Instantiating Scraper for Tournament {target_id}...")
        scraper = SeasonsComponentScraper(
            tournamentid=target_id, webdriver=driver, cfg=config
        )

        # 4. Execute the scrape steps
        scraper.get_data()
        print("Raw Data Fetched!")

        scraper.parse_data()
        print("Data Parsed into Pydantic successfully!")

        # Let's look at what we got:
        print(f"\n--- Result ---")
        print(scraper.data.model_dump_json(indent=6))  # Pretty print the JSON

        # 5. Let's test saving it to our brand new Postgres table!
        # FIXME: need to ensure that we have tournamentid in db before, otherwise throws an error.
        db.upsert_seasons(
            scraper.tournamentid,
            scraper.data.seasons,
            scraper.raw_data.get("seasons", []),
        )

        print("Saved successfully!")

    finally:
        # Always clean up the browser!
        print("Closing webdriver...")
        driver.close()
