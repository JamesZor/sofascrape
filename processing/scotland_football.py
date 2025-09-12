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
    level=logging.WARNING,
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
scot_league_cup_ = 73
"""

"""
25/26 = 77037
name='Championship 24/25' id=62411 year='24/25' - yes f:2
name='Championship 23/24' id=52606 year='23/24' - yes
name='Championship 22/23' id=41958 year='22/23' - yes # no stats 
name='Championship 21/22' id=37030 year='21/22' - yes f:178 # no stats
name='Championship 20/21' id=29268 year='20/21' - yes
name='Championship 19/20' id=23988 year='19/20' - wip
name='Championship 18/19' id=17366 year='18/19'
name='Championship 17/18' id=13458 year='17/18'
name='Championship 16/17' id=11765 year='16/17'
"""
"""
name='Premiership 25/26' id=77128 year='25/26'
name='Premiership 24/25' id=62408 year='24/25' - yes 
name='Premiership 23/24' id=52588 year='23/24' - yes 
name='Premiership 22/23' id=41957 year='22/23' - yes 
name='Premiership 21/22' id=37029 year='21/22' - yes
name='Premiership 20/21' id=28212 year='20/21' - yes 
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
        52606,
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
    time.sleep(60)
    season_quaility_manager.execute_scraping_run()

    consensus: SeasonConsensusResult = (
        season_quaility_manager.build_consensus_analysis()
    )
    print(consensus.season_summary)


def run_one_scrape(tournament_id: int, season_id: int):
    season_quaility_manager = SeasonQualityManager(
        tournament_id=tournament_id, season_id=season_id
    )
    season_quaility_manager.execute_scraping_run()
    consensus = season_quaility_manager.build_consensus_analysis()
    print(consensus.season_summary)


def get_consensus(tournament_id: int, season_id: int):
    season_quaility_manager = SeasonQualityManager(
        tournament_id=tournament_id, season_id=season_id
    )

    consensus: SeasonConsensusResult = (
        season_quaility_manager.build_consensus_analysis()
    )
    print(consensus.season_summary)


def rerun_and_consensus(tournament_id: int, season_id: int):
    season_quaility_manager = SeasonQualityManager(
        tournament_id=tournament_id, season_id=season_id
    )

    consensus = season_quaility_manager.storage.load_most_current_consensus()

    season_quaility_manager.execute_scraping_retry(consensus.get_retry_dict())

    consensus_final: SeasonConsensusResult = (
        season_quaility_manager.build_consensus_analysis()
    )
    print(consensus_final.season_summary)


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


def repair(tournament_id: int, season_id: int):
    season_quaility_manager = SeasonQualityManager(
        tournament_id=tournament_id, season_id=season_id
    )
    consensus = season_quaility_manager.storage.load_most_current_consensus()
    season_quaility_manager.execute_scraping_retry(consensus.get_retry_dict())


def loop_over_tournament_and_seasons_repair(data_dict):
    for tour_id, season_list in data_dict.items():
        print(f"tour_id: {tour_id}")
        [repair(tour_id, season_id) for season_id in season_list]


def loop_over_tournament_and_seasons_check(data_dict):
    for tour_id, season_list in data_dict.items():
        for season_id in season_list:
            season_quaility_manager = SeasonQualityManager(
                tournament_id=tour_id, season_id=season_id
            )
            consensus = season_quaility_manager.storage.load_most_current_consensus()
            print(f"t {tour_id} | s {season_id} : {consensus.season_summary}.")


def loop_over_tournament_and_seasons_new_consensus(data_dict):
    for tour_id, season_list in data_dict.items():
        for season_id in season_list:
            season_quaility_manager = SeasonQualityManager(tour_id, season_id)
            consensus = season_quaility_manager.build_consensus_analysis()
            print(consensus.season_summary)


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


def processing(data, out_dir):
    manager = FootballDataManager()

    # available = manager.loader.discover_available_data()
    # print(f"Available data: {available}")
    #
    # if not available:
    #     print("No data found. Check your data directory structure.")
    #     return

    manager.process_tournament_seasons(
        tournament_seasons=data,
        output_dir=out_dir,
    )


no_stats = {
    54: {
        62408,
        52588,
        41957,
        37029,
        28212,
    },  # Premiership
    55: {
        62411,
        52606,
        41958,
        37030,
        29268,
        23988,
    },  # Championship
}
check_data = {55: [41958, 37030]}


def whats_going_on_scot_20_to_24(data):
    """
    Confused on what data points i have

    in the quality active [base, lineup, incidents]
    """
    # loop_over_tournament_and_seasons_check(data)

    """
    yields 
    t 54 | s 28212 : {'total_matches': 198, 'analyzable_matches': 198, 'single_run_matches': 0, 'perfect_consensus': 198, 'consensus_with_outliers': 0, 'consensus_total': 198, 'failed_matches': 0, 'consensus_rate': 1.0, 'perfect_rate': 1.0}.
    t 54 | s 41957 : {'total_matches': 198, 'analyzable_matches': 198, 'single_run_matches': 0, 'perfect_consensus': 198, 'consensus_with_outliers': 0, 'consensus_total': 198, 'failed_matches': 0, 'consensus_rate': 1.0, 'perfect_rate': 1.0}.
    t 54 | s 37029 : {'total_matches': 198, 'analyzable_matches': 198, 'single_run_matches': 0, 'perfect_consensus': 198, 'consensus_with_outliers': 0, 'consensus_total': 198, 'failed_matches': 0, 'consensus_rate': 1.0, 'perfect_rate': 1.0}.
    t 54 | s 62408 : {'total_matches': 198, 'analyzable_matches': 198, 'single_run_matches': 0, 'perfect_consensus': 195, 'consensus_with_outliers': 0, 'consensus_total': 195, 'failed_matches': 3, 'consensus_rate': 0.9848484848484849, 'perfect_rate': 0.9848484848484849}.
    t 54 | s 52588 : {'total_matches': 198, 'analyzable_matches': 198, 'single_run_matches': 0, 'perfect_consensus': 198, 'consensus_with_outliers': 0, 'consensus_total': 198, 'failed_matches': 0, 'consensus_rate': 1.0, 'perfect_rate': 1.0}.
        t 55 | s 23988 : {'total_matches': 137, 'analyzable_matches': 137, 'single_run_matches': 0, 'perfect_consensus': 66, 'consensus_with_outliers': 1, 'consensus_total': 67, 'failed_matches': 70, 'consensus_rate': 0.48905109489051096, 'perfect_rate': 0.48175182481751827}.
    t 55 | s 29268 : {'total_matches': 135, 'analyzable_matches': 134, 'single_run_matches': 1, 'perfect_consensus': 134, 'consensus_with_outliers': 0, 'consensus_total': 134, 'failed_matches': 0, 'consensus_rate': 1.0, 'perfect_rate': 1.0}.
    t 55 | s 37030 : {'total_matches': 180, 'analyzable_matches': 180, 'single_run_matches': 0, 'perfect_consensus': 176, 'consensus_with_outliers': 4, 'consensus_total': 180, 'failed_matches': 0, 'consensus_rate': 1.0, 'perfect_rate': 0.9777777777777777}.
    t 55 | s 41958 : {'total_matches': 180, 'analyzable_matches': 180, 'single_run_matches': 0, 'perfect_consensus': 177, 'consensus_with_outliers': 3, 'consensus_total': 180, 'failed_matches': 0, 'consensus_rate': 1.0, 'perfect_rate': 0.9833333333333333}.
    t 55 | s 62411 : {'total_matches': 180, 'analyzable_matches': 180, 'single_run_matches': 0, 'perfect_consensus': 177, 'consensus_with_outliers': 1, 'consensus_total': 178, 'failed_matches': 2, 'consensus_rate': 0.9888888888888889, 'perfect_rate': 0.9833333333333333}.
    t 55 | s 52606 : {'total_matches': 180, 'analyzable_matches': 180, 'single_run_matches': 0, 'perfect_consensus': 113, 'consensus_with_outliers': 67, 'consensus_total': 180, 'failed_matches': 0, 'consensus_rate': 1.0, 'perfect_rate': 0.6277777777777778}.


    Unsure why s29268 has 135 matches? 
    to have seasons each 
    """

    no_stats = {
        54: {
            62408,
            52588,
            41957,
            37029,
            28212,
        },  # Premiership
        55: {
            62411,
            52606,
            41958,
            37030,
            29268,
        },  # Championship
    }
    manager = FootballDataManager()
    manager.process_tournament_seasons(
        tournament_seasons=no_stats,
        output_dir="scot_nostats_20_to_24",
    )


if __name__ == "__main__":
    #    whats_going_on_scot_20_to_24(no_stats)

    # loop_over_tournament_and_seasons(data)
    # loop_over_tournament_and_seasons_check(check_data)
    # build_golden(no_stats)

    # processing(ava)  # {54: {62408, 52588, 41957, 37029}, 55: {62411, 52606}}
    # id = 23988
    # run_two_scrapes(55, season_id=23988)

    # get_consensus(55, season_id=41958)
    # rerun_and_consensus(55, season_id=id)

    # used to get the data into the output folder
    # processing(no_stats)

    """
    running for some season 25/26
    """
    #  run_two_scrapes(54, 77128)
    #    run_two_scrapes(55, 77037)
    """missing a game 
    """
    #    rerun_and_consensus(54, 77128)

    no_stats_20_25 = {
        54: {
            77128,
            62408,
            52588,
            41957,
            37029,
            28212,
        },  # Premiership
        55: {
            77037,
            62411,
            52606,
            41958,
            37030,
            29268,
        },  # Championship
    }
    # loop_over_tournament_and_seasons_repair(no_stats_20_25)
    # loop_over_tournament_and_seasons_check(no_stats_20_25)
    # loop_over_tournament_and_seasons_new_consensus(no_stats_20_25)
    # build_golden(no_stats_20_25)
    # processing(no_stats_20_25, "scot_20_25_f")

    """
t 54 | s 28212 : {'total_matches': 198, 'analyzable_matches': 198, 'single_run_matches': 0, 'perfect_consensus': 198, 'consensus_with_outliers': 0, 'consensus_total': 198, 'failed_matches': 0, 'consensus_rate': 1.0, 'perfect_rate': 1.0}.
t 54 | s 41957 : {'total_matches': 198, 'analyzable_matches': 198, 'single_run_matches': 0, 'perfect_consensus': 198, 'consensus_with_outliers': 0, 'consensus_total': 198, 'failed_matches': 0, 'consensus_rate': 1.0, 'perfect_rate': 1.0}.
t 54 | s 37029 : {'total_matches': 198, 'analyzable_matches': 198, 'single_run_matches': 0, 'perfect_consensus': 198, 'consensus_with_outliers': 0, 'consensus_total': 198, 'failed_matches': 0, 'consensus_rate': 1.0, 'perfect_rate': 1.0}.
t 54 | s 62408 : {'total_matches': 198, 'analyzable_matches': 198, 'single_run_matches': 0, 'perfect_consensus': 195, 'consensus_with_outliers': 0, 'consensus_total': 195, 'failed_matches': 3, 'consensus_rate': 0.9848484848484849, 'perfect_rate': 0.9848484848484849}.
t 54 | s 77128 : {'total_matches': 18, 'analyzable_matches': 18, 'single_run_matches': 0, 'perfect_consensus': 17, 'consensus_with_outliers': 1, 'consensus_total': 18, 'failed_matches': 0, 'consensus_rate': 1.0, 'perfect_rate': 0.9444444444444444}.
t 54 | s 52588 : {'total_matches': 198, 'analyzable_matches': 198, 'single_run_matches': 0, 'perfect_consensus': 198, 'consensus_with_outliers': 0, 'consensus_total': 198, 'failed_matches': 0, 'consensus_rate': 1.0, 'perfect_rate': 1.0}.
t 55 | s 29268 : {'total_matches': 135, 'analyzable_matches': 134, 'single_run_matches': 1, 'perfect_consensus': 134, 'consensus_with_outliers': 0, 'consensus_total': 134, 'failed_matches': 0, 'consensus_rate': 1.0, 'perfect_rate': 1.0}.
t 55 | s 37030 : {'total_matches': 180, 'analyzable_matches': 180, 'single_run_matches': 0, 'perfect_consensus': 176, 'consensus_with_outliers': 4, 'consensus_total': 180, 'failed_matches': 0, 'consensus_rate': 1.0, 'perfect_rate': 0.9777777777777777}.
t 55 | s 41958 : {'total_matches': 180, 'analyzable_matches': 180, 'single_run_matches': 0, 'perfect_consensus': 177, 'consensus_with_outliers': 3, 'consensus_total': 180, 'failed_matches': 0, 'consensus_rate': 1.0, 'perfect_rate': 0.9833333333333333}.
t 55 | s 62411 : {'total_matches': 180, 'analyzable_matches': 180, 'single_run_matches': 0, 'perfect_consensus': 177, 'consensus_with_outliers': 1, 'consensus_total': 178, 'failed_matches': 2, 'consensus_rate': 0.9888888888888889, 'perfect_rate': 0.9833333333333333}.
t 55 | s 77037 : {'total_matches': 20, 'analyzable_matches': 20, 'single_run_matches': 0, 'perfect_consensus': 20, 'consensus_with_outliers': 0, 'consensus_total': 20, 'failed_matches': 0, 'consensus_rate': 1.0, 'perfect_rate': 1.0}.
t 55 | s 52606 : {'total_matches': 180, 'analyzable_matches': 180, 'single_run_matches': 0, 'perfect_consensus': 113, 'consensus_with_outliers': 67, 'consensus_total': 180, 'failed_matches': 0, 'consensus_rate': 1.0, 'perfect_rate': 0.6277777777777778}.
    """

    # scotland cup
    # get_seaons_ids(73)
    scot_cup = {73: {81893, 66406, 54956}}

    loop_over_tournament_and_seasons(scot_cup)
