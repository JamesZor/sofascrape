import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel
from tqdm import tqdm
from webdriver import ManagerWebdriver, MyWebDriver

import sofascrape.schemas.general as schemas
from sofascrape.abstract.base import BaseSeasonScraper
from sofascrape.football import FootballMatchScraper
from sofascrape.general import EventsComponentScraper

logger = logging.getLogger(__name__)


class SeasonEventSchema(BaseModel):
    """Individual match result in a season"""

    tournament_id: int
    season_id: int
    match_id: int
    scraped_at: str
    success: bool
    data: Optional[schemas.FootballMatchResultDetailed] = None  # Use the simple version
    error: Optional[str] = None


class SeasonScrapingResult(BaseModel):
    """Complete season scraping result"""

    tournament_id: int
    season_id: int
    total_matches: int
    successful_matches: int
    failed_matches: int
    matches: List[SeasonEventSchema]
    scraping_duration: float  # seconds
    errors_summary: List[str] = []

    @property
    def success_rate(self) -> str:
        """Get success rate as percentage"""
        if self.total_matches == 0:
            return "0%"
        percentage = (self.successful_matches / self.total_matches) * 100
        return f"{percentage:.1f}%"


@dataclass
class MatchJob:
    """Individual match scraping job"""

    match_id: int
    tournament_id: int
    season_id: int


class SeasonFootballScraper(BaseSeasonScraper):
    """Scraper for all matches in a football season"""

    def __init__(self, tournament_id: int, season_id: int) -> None:
        super().__init__(tournamentid=tournament_id, seasonid=season_id)
        self.tournament_id = tournament_id
        self.season_id = season_id
        self.matches: List[SeasonEventSchema] = []
        self.valid_match_ids: List[int] = []

    def _get_events(self) -> bool:
        """Get all events/matches for the season"""
        try:
            driver = self.mw.spawn_webdriver()

            events_scraper = EventsComponentScraper(
                webdriver=driver,
                tournamentid=self.tournament_id,
                seasonid=self.season_id,
                cfg=self.cfg,
            )

            events_data = events_scraper.process()

            if events_data and events_data.events:
                # Filter for completed matches (status code 100)
                self.valid_match_ids = [
                    event.id for event in events_data.events if event.status.code == 100
                ]

                logger.info(
                    f"Found {len(self.valid_match_ids)} completed matches in season {self.season_id}"
                )
                return True
            else:
                logger.error(f"No events found for season {self.season_id}")
                return False

        except Exception as e:
            logger.error(f"Failed to get events for season {self.season_id}: {str(e)}")
            return False
        finally:
            driver.close()

    def _scrape_single_match(
        self, match_id: int, driver: MyWebDriver
    ) -> SeasonEventSchema:
        """Scrape a single match"""
        start_time = datetime.now()

        try:
            match_scraper = FootballMatchScraper(
                webdriver=driver, matchid=match_id, cfg=self.cfg
            )
            result = match_scraper.scrape()

            # Consider it successful if we at least got base data
            success = result.has_base_data
            error_msg = str(result.errors) if result.errors else None

            logger.info(
                f"Match {match_id}: {'✓' if success else '✗'} ({result.success_rate})"
            )

            return SeasonEventSchema(
                tournament_id=self.tournament_id,
                season_id=self.season_id,
                match_id=match_id,
                scraped_at=str(datetime.now()),
                success=success,
                data=result if success else None,
                error=error_msg,
            )

        except Exception as e:
            error_msg = f"Exception scraping match {match_id}: {str(e)}"
            logger.error(error_msg)

            return SeasonEventSchema(
                tournament_id=self.tournament_id,
                season_id=self.season_id,
                match_id=match_id,
                scraped_at=str(datetime.now()),
                success=False,
                error=error_msg,
            )

    def process_events_sequential(self) -> SeasonScrapingResult:
        """Process all events sequentially (safer but slower)"""
        start_time = datetime.now()

        driver = self.mw.spawn_webdriver()
        matches = []

        try:
            # Progress bar for sequential processing
            with tqdm(
                total=len(self.valid_match_ids), desc="Processing matches", unit="match"
            ) as pbar:
                for match_id in self.valid_match_ids:
                    match_result = self._scrape_single_match(match_id, driver)
                    matches.append(match_result)

                    # Update progress bar with success/failure info
                    status = "✓" if match_result.success else "✗"
                    pbar.set_postfix(
                        {
                            "Last": f"{match_id} {status}",
                            "Success": f"{sum(1 for m in matches if m.success)}/{len(matches)}",
                        }
                    )
                    pbar.update(1)

        finally:
            driver.close()

        # Calculate results
        successful = sum(1 for m in matches if m.success)
        failed = len(matches) - successful
        duration = (datetime.now() - start_time).total_seconds()

        return SeasonScrapingResult(
            tournament_id=self.tournament_id,
            season_id=self.season_id,
            total_matches=len(matches),
            successful_matches=successful,
            failed_matches=failed,
            matches=matches,
            scraping_duration=duration,
        )

    def process_events_threaded(self, max_workers: int = 10) -> SeasonScrapingResult:
        """Process events using threading (faster but more resource intensive)"""
        start_time = datetime.now()

        # Create driver pool
        drivers = [self.mw.spawn_webdriver() for _ in range(max_workers)]

        # MANUAL SPLITTING: Split matches among workers
        # This is REQUIRED - ThreadPoolExecutor doesn't auto-split
        match_chunks = self._chunk_matches(self.valid_match_ids, max_workers)

        matches = []
        errors = []

        # Progress tracking for threading
        total_matches = len(self.valid_match_ids)
        completed_matches = 0
        progress_lock = threading.Lock()

        try:
            with tqdm(
                total=total_matches, desc="Processing matches (threaded)", unit="match"
            ) as pbar:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # Submit jobs - one per chunk, not per match
                    future_to_chunk = {}
                    for i, (driver, chunk) in enumerate(zip(drivers, match_chunks)):
                        future = executor.submit(
                            self._process_match_chunk_with_progress,
                            driver,
                            chunk,
                            i,
                            pbar,
                            progress_lock,
                        )
                        future_to_chunk[future] = i

                    # Collect results
                    for future in as_completed(future_to_chunk):
                        chunk_id = future_to_chunk[future]
                        try:
                            chunk_results = future.result()
                            matches.extend(chunk_results)
                            logger.info(
                                f"Completed chunk {chunk_id}: {len(chunk_results)} matches"
                            )
                        except Exception as e:
                            error_msg = f"Chunk {chunk_id} failed: {str(e)}"
                            logger.error(error_msg)
                            errors.append(error_msg)

        finally:
            # Close all drivers
            for driver in drivers:
                try:
                    driver.close()
                except:
                    pass

        # Calculate results
        successful = sum(1 for m in matches if m.success)
        failed = len(matches) - successful
        duration = (datetime.now() - start_time).total_seconds()

        return SeasonScrapingResult(
            tournament_id=self.tournament_id,
            season_id=self.season_id,
            total_matches=len(matches),
            successful_matches=successful,
            failed_matches=failed,
            matches=matches,
            scraping_duration=duration,
            errors_summary=errors,
        )

    def _chunk_matches(self, match_ids: List[int], num_chunks: int) -> List[List[int]]:
        """Split match IDs into chunks for threading"""
        chunk_size = len(match_ids) // num_chunks
        remainder = len(match_ids) % num_chunks

        chunks = []
        start = 0

        for i in range(num_chunks):
            # Add one extra item to first 'remainder' chunks
            end = start + chunk_size + (1 if i < remainder else 0)
            if start < len(match_ids):
                chunks.append(match_ids[start:end])
            start = end

        return [chunk for chunk in chunks if chunk]  # Remove empty chunks

    def _process_match_chunk_with_progress(
        self,
        driver: MyWebDriver,
        match_ids: List[int],
        chunk_id: int,
        pbar: tqdm,
        progress_lock: threading.Lock,
    ) -> List[SeasonEventSchema]:
        """Process a chunk of matches with progress updates"""
        results = []

        for i, match_id in enumerate(match_ids):
            try:
                result = self._scrape_single_match(match_id, driver)
                results.append(result)

                # Thread-safe progress update
                with progress_lock:
                    status = "✓" if result.success else "✗"
                    pbar.set_postfix(
                        {
                            "Last": f"Chunk {chunk_id}: {match_id} {status}",
                            "Success": f"{sum(1 for r in results if r.success)}",
                        }
                    )
                    pbar.update(1)

            except Exception as e:
                logger.error(
                    f"Chunk {chunk_id}: Failed to process match {match_id}: {str(e)}"
                )
                results.append(
                    SeasonEventSchema(
                        tournament_id=self.tournament_id,
                        season_id=self.season_id,
                        match_id=match_id,
                        scraped_at=str(datetime.now()),
                        success=False,
                        error=str(e),
                    )
                )

                # Update progress even for failures
                with progress_lock:
                    pbar.set_postfix(
                        {
                            "Last": f"Chunk {chunk_id}: {match_id} ✗",
                            "Success": f"{sum(1 for r in results if r.success)}",
                        }
                    )
                    pbar.update(1)

        return results

    def scrape(
        self, use_threading: bool = True, max_workers: int = 5
    ) -> SeasonScrapingResult:
        """Main scraping method"""
        if use_threading:
            return self.process_events_threaded(max_workers)
        else:
            return self.process_events_sequential()


# Convenience functions
def scrape_season(
    tournament_id: int, season_id: int, use_threading: bool = True, max_workers: int = 5
) -> SeasonScrapingResult:
    """Scrape an entire season"""
    scraper = SeasonFootballScraper(tournament_id, season_id)
    return scraper.scrape(use_threading, max_workers)


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Premier League 24/25 season
    tournament_id = 1
    season_id = 61627

    try:
        # Quick test with just a few matches (sequential)
        logger.info("Testing sequential processing...")
        scraper = SeasonFootballScraper(tournament_id, season_id)

        # Override for testing - just get first 5 completed matches
        if scraper._get_events():
            scraper.valid_match_ids = scraper.valid_match_ids[:5]  # Test with 5 matches
            print(f"{scraper.valid_match_ids=}")
            result = scraper.process_events_sequential()

            print(f"\n=== Sequential Results ===")
            print(f"Processed: {result.total_matches} matches")
            print(f"Success rate: {result.success_rate}")
            print(f"Duration: {result.scraping_duration:.1f} seconds")

            # Show successful matches
            for match in result.matches:
                if match.success and match.data:
                    info = match.data.get_match_info()
                    if info:
                        print(
                            f"✓ {match.match_id}: {info['home_team']} vs {info['away_team']} ({info['score']})"
                        )
                    else:
                        print(f"✓ {match.match_id}: Data available but no match info")
                else:
                    print(f"✗ {match.match_id}: {match.error}")

        # Test threading (with same 5 matches)
        logger.info("\nTesting threaded processing...")
        scraper2 = SeasonFootballScraper(tournament_id, season_id)
        if scraper2._get_events():
            scraper2.valid_match_ids = scraper2.valid_match_ids[
                :5
            ]  # Test with 5 matches
            result2 = scraper2.process_events_threaded(max_workers=3)

            print(f"\n=== Threaded Results ===")
            print(f"Processed: {result2.total_matches} matches")
            print(f"Success rate: {result2.success_rate}")
            print(f"Duration: {result2.scraping_duration:.1f} seconds")
            print(
                f"Speedup: {result.scraping_duration / result2.scraping_duration:.1f}x"
            )

    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
