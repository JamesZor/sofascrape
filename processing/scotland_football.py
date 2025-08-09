import logging
import time
from dataclasses import asdict

from webdriver import ManagerWebdriver

import sofascrape.schemas.general as sofaschema
from sofascrape.general import SeasonsComponentScraper
from sofascrape.loader.footballDataManager import FootballDataManager
from sofascrape.quality.core.dataclasses import SeasonConsensusResult
from sofascrape.quality.manager import SeasonQualityManager

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force=True,
)
# Use the CORRECT logger name from the debug output
# manager_logger =logging.getLogger("sofascrape.quality.manager")
# manager_logger.setLevel(logging.w)
logging.getLogger("sofascrape.quality.manager").setLevel(logging.WARNING)
# Silence the noisy ones
logging.getLogger("webdriver").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("selenium").setLevel(logging.WARNING)
logging.getLogger("webdriver.core").setLevel(logging.WARNING)
logging.getLogger("webdriver.core.mywebdriver").setLevel(logging.WARNING)
logging.getLogger("webdriver.core.options").setLevel(logging.WARNING)
logging.getLogger("webdriver.core.proxy_manager").setLevel(logging.WARNING)

logging.getLogger("sofascrape.football.season").setLevel(logging.WARNING)
logging.getLogger("sofascrape.football.LineupComonent").setLevel(logging.WARNING)
logging.getLogger("sofascrape.football.base").setLevel(logging.WARNING)
logging.getLogger("sofascrape.football.matchScraper").setLevel(logging.WARNING)
logging.getLogger("sofascrape.football.incidentsComponent").setLevel(logging.WARNING)
logging.getLogger("sofascrape.football.statsComponent").setLevel(logging.WARNING)
logging.getLogger("sofascrape.football.graphComponent").setLevel(logging.WARNING)
logging.getLogger("sofascrape.football.lineupComonent").setLevel(logging.WARNING)
logging.getLogger("sofascrape.football.LineupComonent").setLevel(logging.WARNING)
logging.getLogger("sofascrape.football.eventComponent").setLevel(logging.WARNING)
logging.getLogger("sofascrape.abstract.base").setLevel(logging.WARNING)
"""
scot_prem_id = 54
scot_champ_id = 55 
"""

"""
name='Championship 24/25' id=62411 year='24/25'
name='Championship 23/24' id=52606 year='23/24'
name='Championship 22/23' id=41958 year='22/23'
name='Championship 21/22' id=37030 year='21/22'
name='Championship 20/21' id=29268 year='20/21'
name='Championship 19/20' id=23988 year='19/20'
name='Championship 18/19' id=17366 year='18/19'
name='Championship 17/18' id=13458 year='17/18'
name='Championship 16/17' id=11765 year='16/17'
"""
"""
name='Premiership 25/26' id=77128 year='25/26'
name='Premiership 24/25' id=62408 year='24/25'
name='Premiership 23/24' id=52588 year='23/24'
name='Premiership 22/23' id=41957 year='22/23'
name='Premiership 21/22' id=37029 year='21/22'
name='Premiership 20/21' id=28212 year='20/21'
name='Premiership 19/20' id=23987 year='19/20'
name='Premiership 18/19' id=17364 year='18/19'
name='Premiership 17/18' id=13448 year='17/18'
name='Premiership 16/17' id=11743 year='16/17'
"""

data = {
    54: [
        # 62408,
        52588,
        41957,
        37029,
        28212,
        23987,
        17364,
        13448,
        11743,
    ],  # Premiership
    55: [
        # 62411,
        # 52606,
        41958,
        37030,
        29268,
        23988,
        17366,
        13458,
        11765,
    ],  # Championshi
}


def get_seaons_ids(tournament_id: int):
    mw = ManagerWebdriver()
    tournamentscraper = SeasonsComponentScraper(
        tournamentid=tournament_id, webdriver=mw.spawn_webdriver()
    )
    results = tournamentscraper.process()
    assert results is not None, "Error getting the data."
    for season in results.seasons[:10]:
        print(season)


def run_two_scrapes(tournament_id: int, season_id: int):
    season_quaility_manager = SeasonQualityManager(
        tournament_id=tournament_id, season_id=season_id
    )
    season_quaility_manager.execute_scraping_run()
    time.sleep(600)
    season_quaility_manager.execute_scraping_run()

    consensus: SeasonConsensusResult = (
        season_quaility_manager.build_consensus_analysis()
    )
    print(consensus.season_summary)


def _run_two_scrapes(tournament_id: int, season_id: int):

    print(f"Processing {tournament_id}, season {season_id}")
    season_quaility_manager = SeasonQualityManager(
        tournament_id=tournament_id, season_id=season_id
    )
    season_quaility_manager.execute_scraping_run()
    time.sleep(60)
    season_quaility_manager.execute_scraping_run()
    time.sleep(60)
    consensus: SeasonConsensusResult = (
        season_quaility_manager.build_consensus_analysis()
    )
    season_quaility_manager.execute_scraping_retry(consensus.get_retry_dict())
    consensus_final: SeasonConsensusResult = (
        season_quaility_manager.build_consensus_analysis()
    )
    print(consensus_final.season_summary)


def loop_over_tournament_and_seasons(data_dict):
    for tour_id, season_list in data_dict.items():
        print(f"Processing {tour_id}")
        [_run_two_scrapes(tour_id, season_id) for season_id in season_list]


def loop_over_tournament_and_seasons_check(data_dict):
    for tour_id, season_list in data_dict.items():
        for season_id in season_list:
            season_quaility_manager = SeasonQualityManager(
                tournament_id=tour_id, season_id=season_id
            )
            consensus = season_quaility_manager.storage.load_most_current_consensus()
            print(f"t {tour_id} | s {season_id} : {consensus.season_summary}.")


"""
t 54 | s 52588 : {'total_matches': 198, 'analyzable_matches': 198, 'single_run_matches': 0, 'perfect_consensus': 198, 'consensus_with_outliers': 0, 'consensus_total': 198, 'failed_matches': 0, 'consensus_rate': 1.0, 'perfect_rate': 1.0}.
t 54 | s 41957 : {'total_matches': 198, 'analyzable_matches': 198, 'single_run_matches': 0, 'perfect_consensus': 198, 'consensus_with_outliers': 0, 'consensus_total': 198, 'failed_matches': 0, 'consensus_rate': 1.0, 'perfect_rate': 1.0}.
t 54 | s 37029 : {'total_matches': 198, 'analyzable_matches': 198, 'single_run_matches': 0, 'perfect_consensus': 198, 'consensus_with_outliers': 0, 'consensus_total': 198, 'failed_matches': 0, 'consensus_rate': 1.0, 'perfect_rate': 1.0}.

t 54 | s 28212 : {'total_matches': 0, 'analyzable_matches': 0, 'single_run_matches': 0, 'perfect_consensus': 0, 'consensus_with_outliers': 0, 'consensus_total': 0, 'failed_matches': 0, 'consensus_rate': 0.0, 'perfect_rate': 0.0}.
t 54 | s 23987 : {'total_matches': 0, 'analyzable_matches': 0, 'single_run_matches': 0, 'perfect_consensus': 0, 'consensus_with_outliers': 0, 'consensus_total': 0, 'failed_matches': 0, 'consensus_rate': 0.0, 'perfect_rate': 0.0}.
t 54 | s 17364 : {'total_matches': 0, 'analyzable_matches': 0, 'single_run_matches': 0, 'perfect_consensus': 0, 'consensus_with_outliers': 0, 'consensus_total': 0, 'failed_matches': 0, 'consensus_rate': 0.0, 'perfect_rate': 0.0}.
t 54 | s 13448 : {'total_matches': 0, 'analyzable_matches': 0, 'single_run_matches': 0, 'perfect_consensus': 0, 'consensus_with_outliers': 0, 'consensus_total': 0, 'failed_matches': 0, 'consensus_rate': 0.0, 'perfect_rate': 0.0}.
t 54 | s 11743 : {'total_matches': 0, 'analyzable_matches': 0, 'single_run_matches': 0, 'perfect_consensus': 0, 'consensus_with_outliers': 0, 'consensus_total': 0, 'failed_matches': 0, 'consensus_rate': 0.0, 'perfect_rate': 0.0}.


t 55 | s 41958 : {'total_matches': 180, 'analyzable_matches': 180, 'single_run_matches': 0, 'perfect_consensus': 70, 'consensus_with_outliers': 0, 'consensus_total': 70, 'failed_matches': 110, 'consensus_rate': 0.3888888888888889, 'perfect_rate': 0.3888888888888889}.
t 55 | s 37030 : {'total_matches': 180, 'analyzable_matches': 180, 'single_run_matches': 0, 'perfect_consensus': 2, 'consensus_with_outliers': 0, 'consensus_total': 2, 'failed_matches': 178, 'consensus_rate': 0.011111111111111112, 'perfect_rate': 0.011111111111111112}.
t 55 | s 29268 : {'total_matches': 0, 'analyzable_matches': 0, 'single_run_matches': 0, 'perfect_consensus': 0, 'consensus_with_outliers': 0, 'consensus_total': 0, 'failed_matches': 0, 'consensus_rate': 0.0, 'perfect_rate': 0.0}.
t 55 | s 23988 : {'total_matches': 0, 'analyzable_matches': 0, 'single_run_matches': 0, 'perfect_consensus': 0, 'consensus_with_outliers': 0, 'consensus_total': 0, 'failed_matches': 0, 'consensus_rate': 0.0, 'perfect_rate': 0.0}.
"""

sub_set = {
    54: [62408, 52588, 41957, 37029],  # Premiership
    55: [
        62411,
        52606,
    ],  # Championshi
}
#
ava = {54: {62408, 52588, 41957, 37029}, 55: {62411, 52606}}


def build_golden(subdata):
    for tour_id, season_list in subdata.items():
        for season_id in season_list:
            season_quaility_manager = SeasonQualityManager(
                tournament_id=tour_id, season_id=season_id
            )
            consensus = season_quaility_manager.storage.load_most_current_consensus()
            print(f"t {tour_id} | s {season_id} : {consensus.season_summary}.")
            golden_data_set = season_quaility_manager.build_golden_seaosn_dataset(
                consensus.get_golden_dataset_dict()
            )
            season_quaility_manager.storage.save_golden_dataset(
                golden_data=golden_data_set
            )


def processing(data):
    manager = FootballDataManager()

    # available = manager.loader.discover_available_data()
    # print(f"Available data: {available}")
    #
    # if not available:
    #     print("No data found. Check your data directory structure.")
    #     return

    manager.process_tournament_seasons(
        tournament_seasons=data,
        output_dir="scot_test_mixed",
    )


if __name__ == "__main__":
    # NOTE:
    # - running:
    #       54 - 62408 - 24/25
    # TODO:
    # - run for 54

    # loop_over_tournament_and_seasons(data)
    # loop_over_tournament_and_seasons_check(data)
    # build_golden(sub_set)

    processing(ava)  # {54: {62408, 52588, 41957, 37029}, 55: {62411, 52606}}

    # run_two_scrapes(54, season_id=62408)

    # get_seaons_ids(54)
