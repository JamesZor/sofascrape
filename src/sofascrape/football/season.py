import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Optional

from tqdm import tqdm
from webdriver import ManagerWebdriver, MyWebDriver

import sofascrape.schemas.general as schemas
from sofascrape.abstract.base import BaseSeasonScraper
from sofascrape.football import FootballMatchScraper
from sofascrape.general import EventsComponentScraper

logger = logging.getLogger(__name__)


class SeasonFootballScraper(BaseSeasonScraper):
    """Scraper for all matches in a football season"""

    def __init__(self, tournamentid: int, seasonid: int) -> None:
        super().__init__(tournamentid=tournamentid, seasonid=seasonid)

        self.events_scraper: Optional[EventsComponentScraper] = None

        self.matches: List[schemas.SeasonEventSchema] = []
        self.valid_matchids: List[int] = []

    def _get_events(self) -> None:
        """Get all events/matches for the season"""
        try:
            driver = self.mw.spawn_webdriver()

            self.events_scraper = EventsComponentScraper(
                webdriver=driver,
                tournamentid=self.tournamentid,
                seasonid=self.seasonid,
                cfg=self.cfg,
            )

            self.events_scraper.process()  # data in events_scraper.data. # eventslistschema -> for event in events

            if self.events_scraper is not None:
                # Filter for completed matches (status code 100)
                self.valid_matchids = [
                    event.id
                    for event in self.events_scraper.data.events
                    if event.status.code == 100
                ]

                logger.info(
                    f"Found {len(self.valid_matchids)} completed matches in season {self.seasonid}"
                )
        except Exception as e:
            logger.error(f"Failed to get events for season {self.seasonid}: {str(e)}")
        finally:
            driver.close()

    def _set_debug_limit_valid_matches(self, limit: int = 3) -> None:
        """in place transformation on valid matches, to reduce the number to process, (saves time)"""
        if self.events_scraper is not None:
            self.valid_matchids = self.valid_matchids[0:limit]

    def _scrape_single_match(
        self, matchid: int, driver: MyWebDriver
    ) -> schemas.SeasonEventSchema:
        """Scrape a single match"""
        try:
            match_scraper = FootballMatchScraper(
                webdriver=driver, matchid=matchid, cfg=self.cfg
            )
            result = match_scraper.scrape()

            # Consider it successful if we at least got base data
            success = result.has_base_data
            error_msg = str(result.errors) if result.errors else None

            logger.info(
                f"Match {matchid}: {'✓' if success else '✗'} ({result.success_rate})"
            )

            return schemas.SeasonEventSchema(
                tournament_id=self.tournamentid,
                season_id=self.seasonid,
                match_id=matchid,
                scraped_at=str(datetime.now()),
                success=success,
                data=result if success else None,
                error=error_msg,
            )

        except Exception as e:
            error_msg = f"Exception scraping match {matchid}: {str(e)}"
            logger.error(error_msg)

            return schemas.SeasonEventSchema(
                tournament_id=self.tournamentid,
                season_id=self.seasonid,
                match_id=matchid,
                scraped_at=str(datetime.now()),
                success=False,
                error=error_msg,
            )

    def process_events_sequential(self) -> schemas.SeasonScrapingResult:
        """Process all events sequentially (safer but slower)"""
        start_time = datetime.now()

        driver = self.mw.spawn_webdriver()
        matches = []

        try:
            # Progress bar for sequential processing
            with tqdm(
                total=len(self.valid_matchids), desc="Processing matches", unit="match"
            ) as pbar:
                for matchid in self.valid_matchids:
                    match_result = self._scrape_single_match(matchid, driver)
                    matches.append(match_result)

                    # Update progress bar with success/failure info
                    status = "✓" if match_result.success else "✗"
                    pbar.set_postfix(
                        {
                            "Last": f"{matchid} {status}",
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

        return schemas.SeasonScrapingResult(
            tournament_id=self.tournamentid,
            season_id=self.seasonid,
            total_matches=len(matches),
            successful_matches=successful,
            failed_matches=failed,
            matches=matches,
            scraping_duration=duration,
        )

    def process_events_threaded(
        self, max_workers: int = 10
    ) -> schemas.SeasonScrapingResult:
        """Process events using threading (faster but more resource intensive)"""
        start_time = datetime.now()

        # Create driver pool
        drivers = [self.mw.spawn_webdriver() for _ in range(max_workers)]

        # MANUAL SPLITTING: Split matches among workers
        # This is REQUIRED - ThreadPoolExecutor doesn't auto-split
        match_chunks = self._chunk_matches(self.valid_matchids, max_workers)

        matches = []
        errors = []

        # Progress tracking for threading
        total_matches = len(self.valid_matchids)
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

        return schemas.SeasonScrapingResult(
            tournament_id=self.tournamentid,
            season_id=self.seasonid,
            total_matches=len(matches),
            successful_matches=successful,
            failed_matches=failed,
            matches=matches,
            scraping_duration=duration,
            errors_summary=errors,
        )

    def _chunk_matches(self, matchids: List[int], num_chunks: int) -> List[List[int]]:
        """Split match IDs into chunks for threading"""
        chunk_size = len(matchids) // num_chunks
        remainder = len(matchids) % num_chunks

        chunks = []
        start = 0

        for i in range(num_chunks):
            # Add one extra item to first 'remainder' chunks
            end = start + chunk_size + (1 if i < remainder else 0)
            if start < len(matchids):
                chunks.append(matchids[start:end])
            start = end

        return [chunk for chunk in chunks if chunk]  # Remove empty chunks

    def _process_match_chunk_with_progress(
        self,
        driver: MyWebDriver,
        matchids: List[int],
        chunkid: int,
        pbar: tqdm,
        progress_lock: threading.Lock,
    ) -> List[schemas.SeasonEventSchema]:
        """Process a chunk of matches with progress updates"""
        results = []

        for i, matchid in enumerate(matchids):
            try:
                result = self._scrape_single_match(matchid, driver)
                results.append(result)

                # Thread-safe progress update
                with progress_lock:
                    status = "✓" if result.success else "✗"
                    pbar.set_postfix(
                        {
                            "Last": f"Chunk {chunkid}: {matchid} {status}",
                            "Success": f"{sum(1 for r in results if r.success)}",
                        }
                    )
                    pbar.update(1)

            except Exception as e:
                logger.error(
                    f"Chunk {chunkid}: Failed to process match {matchid}: {str(e)}"
                )
                results.append(
                    schemas.SeasonEventSchema(
                        tournament_id=self.tournamentid,
                        season_id=self.seasonid,
                        match_id=matchid,
                        scraped_at=str(datetime.now()),
                        success=False,
                        error=str(e),
                    )
                )

                # Update progress even for failures
                with progress_lock:
                    pbar.set_postfix(
                        {
                            "Last": f"Chunk {chunkid}: {matchid} ✗",
                            "Success": f"{sum(1 for r in results if r.success)}",
                        }
                    )
                    pbar.update(1)

        return results

    def scrape(
        self, use_threading: bool = True, max_workers: int = 5
    ) -> schemas.SeasonScrapingResult:
        """Main scraping method"""
        self._get_events()
        if use_threading:
            return self.process_events_threaded(max_workers)
        else:
            return self.process_events_sequential()
