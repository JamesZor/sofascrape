import logging
from typing import List, Optional, Union

from omegaconf import DictConfig
from tqdm import tqdm
from webdriver import ManagerWebdriver

from sofascrape.abstract.base import BaseTournamentProcessor

from .tournament import TournamentComponentScraper


class TournamentProcessScraper(BaseTournamentProcessor):

    def __init__(
        self,
        managerwebdriver: Optional[ManagerWebdriver] = None,
        cfg: Optional[DictConfig] = None,
    ) -> None:

        super().__init__(managerwebdriver=managerwebdriver, cfg=cfg)

        self.tournament_ids_list: List[int] = []

    def _get_list_of_tournamentids(self) -> None:
        self.tournament_ids_list = list(range(1, 11, 1))

    def process(self):

        if not self.tournament_ids_list:
            self._get_list_of_tournamentids()

        driver = self.mw.spawn_webdriver()

        self.tournaments: List[Union[None, TournamentComponentScraper]] = [None] * len(
            self.tournament_ids_list
        )

        with tqdm(
            total=len(self.tournament_ids_list),
            desc="Processing tournaments",
            unit="Tournament",
        ) as pbar:
            for idx, tournament_id in enumerate(self.tournament_ids_list):
                try:
                    self.tournaments[idx] = TournamentComponentScraper(
                        tournamentid=tournament_id, webdriver=driver, cfg=self.cfg
                    )
                    self.tournaments[idx].process()
                    status = "✓"
                    Details = f"{self.tournaments[idx].data.tournament.name}"

                except Exception as e:
                    logging.warning(f"Error {str(e)}, on tournaments: {tournament_id}")
                    status = "✗"

                    Details = f"ID {idx}, no Details"

                pbar.set_postfix(
                    ordered_dict={
                        "Last": f"{tournament_id}, {status}",
                        "Details": Details,
                    },
                    refresh=True,
                )
                pbar.update(1)
        driver.close()
