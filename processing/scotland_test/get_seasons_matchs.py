import json
import logging

from omegaconf import OmegaConf

from sofascrape.football import LeagueFootballScraper
from sofascrape.utils import FootballLeague, ProcessingUtils

scotland_pl_tour_id = 54


logger = logging.getLogger(__name__)
logger.setLevel(level=logging.WARNING)


if __name__ == "__main__":
    pu = ProcessingUtils(type=FootballLeague.SCOTLAND, web_on=False)

    league_scraper = LeagueFootballScraper(tournamentid=scotland_pl_tour_id)
    # example
    # results_dict = league_scraper._wip_scrape()
    # pu.save_pickle("scot_pl_example", results_dict)
    # load_results = pu.load_pickle(file_name="scot_pl_example")

    # season 24 - 20
    results_dict = league_scraper.scrape()
    pu.save_pickle("scot_pl_20_24", results_dict)
    load_results = pu.load_pickle(file_name="scot_pl_example")

    # for seasonid, season_scrape in load_results.items():
    #     print(seasonid)
    #     for event in season_scrape.data.matches[0:2]:
    #         print(event.data.base.model_dump_json(indent=5))
