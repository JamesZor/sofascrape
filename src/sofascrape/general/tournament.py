import logging
from typing import Dict, Optional

from pydantic import ValidationError
from webdriver import MyWebDriver

from sofascrape.abstract.base import BaseComponentScraper
from sofascrape.conf.config import AppConfig
from sofascrape.schemas.general import TournamentData

logger = logging.getLogger(__name__)


class TournamentComponentScraper(BaseComponentScraper):

    def __init__(
        self,
        tournamentid: int,
        webdriver: MyWebDriver,
        cfg: Optional[AppConfig] = None,
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

        self.page_url: str = self.cfg.links.tournament_empty.format(
            tournamentID=tournamentid
        )

    def get_data(self) -> None:
        """Fetch raw tournament data from the web page.

        Raises:
            ValueError: If page data is not a dictionary
            Exception: If web scraping fails
        """
        try:
            tournament_page_data = self.webdriver.get_page(self.page_url)

            if not isinstance(tournament_page_data, dict):
                error_msg = (
                    f"Tournament page data is incorrect. "
                    f"Expected dict, got {type(tournament_page_data)}. "
                    f"Data: {tournament_page_data} "
                    f"Proxy: {self.webdriver.set_proxy}, "
                    f"Tournament ID: {self.tournamentid}"
                )
                logger.warning(error_msg)
                raise ValueError(error_msg)

            self.raw_data = tournament_page_data
            logger.info(f"Successfully fetched data for tournament {self.tournamentid}")

        except Exception as e:
            logger.error(
                f"Failed to get data for tournament {self.tournamentid}: {str(e)}"
            )
            raise

    def parse_data(self) -> None:
        """Parse raw data into structured format using Pydantic.

        Raises:
            ValueError: If raw_data is None
            ValidationError: If data doesn't match expected schema
        """
        if self.raw_data is None:
            raise ValueError("No raw data available. Call get_data() first.")

        try:
            # Pass dict directly to Pydantic (not JSON string)
            self.data = TournamentData.model_validate(self.raw_data)
            logger.info(f"Successfully parsed data for tournament {self.tournamentid}")

        except ValidationError as e:
            logger.warning(
                f"Validation failed for tournament {self.tournamentid}: {str(e)}"
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error parsing tournament {self.tournamentid}: {str(e)}"
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
    from sofascrape.general.tournament import TournamentComponentScraper

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
        scraper = TournamentComponentScraper(
            tournamentid=target_id, webdriver=driver, cfg=config
        )

        # 4. Execute the scrape steps
        scraper.get_data()
        print("Raw Data Fetched!")

        scraper.parse_data()
        print("Data Parsed into Pydantic successfully!")

        # Let's look at what we got:
        tournament_name = scraper.data.tournament.name
        print(f"\n--- Result ---")
        print(f"Found Tournament: {tournament_name}")
        print(scraper.data.model_dump_json(indent=6))  # Pretty print the JSON

        # 5. Let's test saving it to our brand new Postgres table!
        print(f"\nSaving {tournament_name} to Postgres database...")
        db.upsert_tournament(scraper.data, scraper.raw_data)
        print("Saved successfully!")

    finally:
        # Always clean up the browser!
        print("Closing webdriver...")
        driver.close()
