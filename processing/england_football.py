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
  - Premier League (ID: 1, Slug: premier-league)
  - Championship (ID: 2, Slug: championship)
  - League One (ID: 3, Slug: league-one)
  - League Two (ID: 84, Slug: league-two)

"""

"""
 No stats 
name='Premier League 25/26' id=76986 year='25/26'
name='Premier League 24/25' id=61627 year='24/25' - missing 18
name='Premier League 23/24' id=52186 year='23/24' -missing  0 
name='Premier League 22/23' id=41886 year='22/23' - missing 42 
name='Premier League 21/22' id=37036 year='21/22' - missing 19 
name='Premier League 20/21' id=29415 year='20/21' - missing 2
name='Premier League 19/20' id=23776 year='19/20'
name='Premier League 18/19' id=17359 year='18/19'
name='Premier League 17/18' id=13380 year='17/18'
name='Premier League 16/17' id=11733 year='16/17'

"""

"""
no stats 
name='Championship 25/26' id=77347 year='25/26'
name='Championship 24/25' id=61961 year='24/25' - missing 0, 
name='Championship 23/24' id=52367 year='23/24' - missing 0 matches 
name='Championship 22/23' id=42401 year='22/23' - missing 0 
name='Championship 21/22' id=37154 year='21/22' - missing 0
name='Championship 20/21' id=29438 year='20/21' - missing 0 
name='Championship 19/20' id=23976 year='19/20'
name='Championship 18/19' id=17473 year='18/19'
name='Championship 17/18' id=13429 year='17/18'
name='Championship 16/17' id=11784 year='16/17'

"""


"""
no stats 
name='League One 25/26' id=77352 year='25/26'
name='League One 24/25' id=61959 year='24/25' - 12 missing
name='League One 23/24' id=52370 year='23/24' - 13 missing 
name='League One 22/23' id=42402 year='22/23' - 4 missing
name='League One 21/22' id=37155 year='21/22' - 8 missing
name='League One 20/21' id=29439 year='20/21' - All missing 
name='League One 19/20' id=23977 year='19/20'
name='League One 18/19' id=17474 year='18/19'
name='League One 17/18' id=13430 year='17/18'
name='League One 16/17' id=11785 year='16/17'
"""

"""
name='League Two 25/26' id=77351 year='25/26'
name='League Two 24/25' id=61960 year='24/25'
name='League Two 23/24' id=52368 year='23/24'
name='League Two 22/23' id=42403 year='22/23'
name='League Two 21/22' id=37156 year='21/22'
name='League Two 20/21' id=29441 year='20/21'
name='League Two 19/20' id=23980 year='19/20'
name='League Two 18/19' id=17476 year='18/19'
name='League Two 17/18' id=13431 year='17/18'
name='League Two 16/17' id=11783 year='16/17'

"""


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
    print(
        f"Running two scrapes for tournament id {tournament_id} - season {season_id}."
    )
    season_quaility_manager = SeasonQualityManager(
        tournament_id=tournament_id, season_id=season_id
    )
    season_quaility_manager.execute_scraping_run()
    time.sleep(120)
    season_quaility_manager.execute_scraping_run()

    consensus: SeasonConsensusResult = (
        season_quaility_manager.build_consensus_analysis()
    )
    print(consensus.season_summary)


def display_consensus(tournament_id: int, season_id: int):
    season_quaility_manager = SeasonQualityManager(
        tournament_id=tournament_id, season_id=season_id
    )

    consensus = season_quaility_manager.storage.load_most_current_consensus()
    print(consensus.season_summary)
    print()
    print(consensus.get_retry_dict())


# def rerun(tournament_id: int, season_id: int):
#    season_quaility_manager = SeasonQualityManager(
#        tournament_id=tournament_id, season_id=season_id
#    )
#
#    cons


def rerun_and_consensus(tournament_id: int, season_id: int):
    season_quaility_manager = SeasonQualityManager(
        tournament_id=tournament_id, season_id=season_id
    )

    consensus = season_quaility_manager.storage.load_most_current_consensus()

    season_quaility_manager.execute_scraping_retry(consensus.get_retry_dict())
    consensus_final = season_quaility_manager.build_consensus_analysis()

    print(consensus_final.season_summary)


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


def process_to_df(data_dict, out_dir):
    """
    need to run golden before this
    """
    print("running")
    manager = FootballDataManager()
    manager.process_tournament_seasons(tournament_seasons=data_dict, output_dir=out_dir)
    print("finished")


main_data = {
    1: {61627, 52186, 41886, 37036, 29415},
    2: {61961, 52367, 42401, 37154, 29438},
    3: {61959, 52370, 42402, 37155, 29439},
    84: {61960, 52368, 42403, 37156, 29441},
}

pl_rerun = [61627, 52186, 41886, 37036, 29415]
champ_l = [61961, 52367, 42401, 37154, 29438]
league_one = [61959, 52370, 42402, 37155, 29439]  # 3
league_two = [61960, 52368, 42403, 37156, 29441]  # 84
if __name__ == "__main__":
    # t_id = 84
    # get_seaons_ids(t_id)
    # s_id = 29439
    # run_two_scrapes(tournament_id=t_id, season_id=s_id)

    # to process to df
    # build_golden(main_data)
    process_to_df(main_data, "england_20_25")

    # for season_id in league_two:
    #     print(f"season :{season_id}")
    #     # run_two_scrapes(t_id, season_id)
    #     # time.sleep(120)
    #     # display_consensus(tournament_id=t_id, season_id=season_id)
    #     rerun_and_consensus(tournament_id=t_id, season_id=season_id)


"""
{'total_matches': 552, 'analyzable_matches': 552, 'single_run_matches': 0, 'perfect_consensus': 540, 'consensus_with_outliers': 5, 'consensus_total': 545, 'failed_matches': 7, 'consensus_rate': 0.9873188405797102, 'perfect_rate': 0.9782608695652174}
season :52370

{'total_matches': 552, 'analyzable_matches': 552, 'single_run_matches': 0, 'perfect_consensus': 535, 'consensus_with_outliers': 8, 'consensus_total': 543, 'failed_matches': 9, 'consensus_rate': 0.9836956521739131, 'perfect_rate': 0.9692028985507246}
season :42402

{'total_matches': 552, 'analyzable_matches': 552, 'single_run_matches': 0, 'perfect_consensus': 543, 'consensus_with_outliers': 1, 'consensus_total': 544, 'failed_matches': 8, 'consensus_rate': 0.9855072463768116, 'perfect_rate': 0.9836956521739131}
season :37155

{'total_matches': 552, 'analyzable_matches': 552, 'single_run_matches': 0, 'perfect_consensus': 544, 'consensus_with_outliers': 6, 'consensus_total': 550, 'failed_matches': 2, 'consensus_rate': 0.9963768115942029, 'perfect_rate': 0.9855072463768116}

"""

#   display_consensus(tournament_id=t_id, season_id=s_id)
"""
{'total_matches': 552, 'analyzable_matches': 552, 'single_run_matches': 0, 'perfect_consensus': 548, 'consensus_with_outliers': 0, 'consensus_total': 548, 'failed_matches': 4, 'consensus_rate': 0.9927536231884058, 'perfect_rate': 0.9927536231884058}

{12468423: ['incidents'], 12468426: ['incidents'], 12468618: ['incidents'], 12469050: ['incidents']}
season :52367
{'total_matches': 552, 'analyzable_matches': 551, 'single_run_matches': 1, 'perfect_consensus': 541, 'consensus_with_outliers': 0, 'consensus_total': 541, 'failed_matches': 10, 'consensus_rate': 0.9818511796733213, 'perfect_rate': 0.9818511796733213}

{11372708: ['incidents'], 11372799: ['incidents'], 11371418: ['incidents'], 11371429: ['incidents'], 11371433: ['incidents'], 11371440: ['incidents'], 11371550: ['lineup'], 11371619: ['incidents'], 11368239: ['incidents'], 11368258: ['lineup'], 11372729: ['base', 'stats', 'lineup', 'incidents', 'graph']}
season :42401
{'total_matches': 552, 'analyzable_matches': 552, 'single_run_matches': 0, 'perfect_consensus': 551, 'consensus_with_outliers': 0, 'consensus_total': 551, 'failed_matches': 1, 'consensus_rate': 0.9981884057971014, 'perfect_rate': 0.9981884057971014}

{10406075: ['base']}
season :37154
{'total_matches': 552, 'analyzable_matches': 552, 'single_run_matches': 0, 'perfect_consensus': 542, 'consensus_with_outliers': 0, 'consensus_total': 542, 'failed_matches': 10, 'consensus_rate': 0.9818840579710145, 'perfect_rate': 0.9818840579710145}

{9590823: ['base'], 9590858: ['lineup'], 9591333: ['lineup'], 9591709: ['base'], 9591943: ['incidents'], 9591957: ['incidents'], 10057379: ['incidents'], 9592394: ['base'], 9592429: ['base'], 9592452: ['incidents']}
season :29438
{'total_matches': 552, 'analyzable_matches': 552, 'single_run_matches': 0, 'perfect_consensus': 545, 'consensus_with_outliers': 0, 'consensus_total': 545, 'failed_matches': 7, 'consensus_rate': 0.9873188405797102, 'perfect_rate': 0.9873188405797102}

{8900796: ['incidents'], 8900822: ['lineup'], 8901163: ['incidents'], 8902001: ['incidents'], 8902343: ['incidents'], 8902465: ['incidents'], 8902517: ['incidents']}
"""
