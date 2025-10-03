import json
from pathlib import Path
from typing import List

from omegaconf import OmegaConf
from webdriver import ManagerWebdriver, MyWebDriver

import sofascrape.schemas.general as schemas
from sofascrape.football import FootballMatchScraper
from sofascrape.general import (
    EventsComponentScraper,
    SeasonsComponentScraper,
    TournamentComponentScraper,
)

"""
    Scotland:
  - Premiership (ID: 54, Slug: premiership)
  - Championship (ID: 55, Slug: championship)
  - League One (ID: 56, Slug: league-one)
  - League Two (ID: 57, Slug: league-two)
  - Scottish Cup (ID: 73, Slug: scottish-cup)
"""


"""

    League One ( League 1 ) - id 56 
name='League 1 25/26' id=77129 year='25/26'
name='League 1 24/25' id=62416 year='24/25'
name='League 1 23/24' id=52605 year='23/24'
name='League One 22/23' id=41959 year='22/23'
name='League One 21/22' id=37031 year='21/22'
name='League One 20/21' id=29270 year='20/21'
name='League One 19/20' id=23989 year='19/20'
name='League One 18/19' id=17368 year='18/19'
name='League One 17/18' id=13636 year='17/18'
name='League One 16/17' id=11766 year='16/17'



    League Two ( League 2) - id 57 
###
name='League 2 25/26' id=77045 year='25/26'
name='League 2 24/25' id=62487 year='24/25'
name='League 2 23/24' id=52604 year='23/24'
name='League Two 22/23' id=41960 year='22/23'
name='League Two 21/22' id=37032 year='21/22'
name='League Two 20/21' id=29269 year='20/21'
name='League Two 19/20' id=23990 year='19/20'
name='League Two 18/19' id=17369 year='18/19'
name='League Two 17/18' id=13827 year='17/18'
name='League Two 16/17' id=11767 year='16/17'

# get matches 
[14035666, 14035662, 14035664, 14035663, 14035665, 14035670, 14035668, 14035671, 14035669, 14035667, 14035672, 14035675, 14035676, 14035674, 14035673, 14035678, 14035677, 14035681, 14035683, 14035679, 14035680, 14035686, 14035682, 14035684, 14035685, 14035687, 14035695, 14035692, 14035689, 14035688, 14035694, 14035691, 14035693, 14035690, 14035697, 14035696, 14035700, 14035703, 14035698, 14035699, 14035706, 14035704, 14035702, 14035701, 14035705, 14035708, 14035709, 14035710, 14035707, 14035711, 14035713, 14035714, 14035716, 14035712, 14035715, 14035717, 14035718, 14035720, 14035719, 14035735, 14035722, 14035721, 14035725, 14035723, 14035724, 14035729, 14035728, 14035727, 14035726, 14035730, 14035732, 14035733, 14035731, 14035734, 14035736, 14035740, 14035739, 14035741, 14035738, 14035737, 14035747, 14035744, 14035742, 14035745, 14035743, 14035750, 14035751, 14035748, 14035749, 14035746, 14035753, 14035755, 14035756, 14035754, 14035752, 14035757, 14035760, 14035758, 14035759, 14035761, 14035763, 14035762, 14035767, 14035764, 14035765, 14035768, 14035771, 14035766, 14035770, 14035769, 14035774, 14035773, 14035772, 14035775, 14035776, 14035782, 14035779, 14035777, 14035780, 14035781, 14035787, 14035784, 14035778, 14035783, 14035785, 14035788, 14035789, 14035786, 14035790, 14035792, 14035793, 14035794, 14035796, 14035795, 14035791, 14035799, 14035798, 14035800, 14035797, 14035801, 14035803, 14035804, 14035806, 14035802, 14035805, 14035809, 14035808, 14035811, 14035810, 14035807, 14035813, 14035815, 14035814, 14035816, 14035812, 14035819, 14035827, 14035818, 14035820, 14035817, 14035824, 14035826, 14035821, 14035823, 14035822, 14032707, 14032717, 14032715, 14032721, 14032718, 14032714, 14032719, 14032708, 14032710, 14032720, 14032722, 14032713, 14032725, 14032712, 14032716]


"""


def get_tournament_scraper(
    webdriver: MyWebDriver, tournamentid: int
) -> TournamentComponentScraper:
    """
    Get the tournamentScraper for the associated tournamentid and print the loaded config.

    """
    tournamentScraper = TournamentComponentScraper(
        tournamentid=tournamentid, webdriver=webdriver
    )
    print(OmegaConf.to_yaml(tournamentScraper.cfg))

    return tournamentScraper


def get_seasons_last_10(
    webdriver: MyWebDriver, tournamentid: int
) -> SeasonsComponentScraper:
    tournamentscraper = SeasonsComponentScraper(
        tournamentid=tournamentid, webdriver=web
    )
    results = tournamentscraper.process()
    assert results is not None, "Error getting the data."
    for season in results.seasons[:10]:
        print(season)
    return tournamentscraper


def get_matchs_last_20(
    webdriver: MyWebDriver, tournamentid: int, seasonid: int
) -> List:

    season_events_scraper = EventsComponentScraper(
        webdriver=web, tournamentid=tournamentid, seasonid=seasonid
    )
    result: schemas.EventsListSchema = season_events_scraper.process()

    assert result is not None, "Error getting the data"
    if result:
        matchids = [event.id for event in result.events]
        print("+" * 20)
        print(matchids)

    return matchids


if __name__ == "__main__":
    mw = ManagerWebdriver()

    """
        Explore the tournaments and the parts of the page and of the api. 

        tournament 
        -> each season has an id. 
        seaosn 
        -> each match has an id 

    """
    web = mw.spawn_webdriver()

    tour_scraper_L2 = get_tournament_scraper(webdriver=web, tournamentid=57)
    tour_scraper_L2.process()
    seasons_L2 = get_seasons_last_10(webdriver=web, tournamentid=56)

    ##
    matches_id_L2 = get_matchs_last_20(webdriver=web, tournamentid=57, seasonid=77045)

    """
        matches have a number of parts: 
         - event details : main details regarding the event. 

    """
    matches_id_L2[0]

    from sofascrape.football import EventFootallComponentScraper

    eventDetailsScraper = EventFootallComponentScraper(
        webdriver=web, matchid=matches_id_L2[0]
    )
    print(OmegaConf.to_yaml(eventDetailsScraper.cfg))

    eventDetailsScraper.get_data()
    assert eventDetailsScraper.raw_data is not None, "Expected raw data got none."
    assert isinstance(
        eventDetailsScraper.raw_data, dict
    ), f"Expect dict data, got {type(eventDetailsScraper.raw_data)=}."

    print(json.dumps(eventDetailsScraper.raw_data, indent=4))

    #### line up

    from sofascrape.football import FootballLineupComponentScraper

    lineup_scraper = FootballLineupComponentScraper(
        webdriver=web, matchid=matches_id_L2[0]
    )

    lineup_scraper.get_data()
    assert lineup_scraper.raw_data is not None, "Expected raw data got none."
    assert isinstance(
        lineup_scraper.raw_data, dict
    ), f"Expect dict data, got {type(lineup_scraper.raw_data)=}."

    print(json.dumps(lineup_scraper.raw_data, indent=4))

    result = lineup_scraper.process()

    if result:
        print(result.model_dump_json(indent=6))

    ### Incidents

    from sofascrape.football import FootballIncidentsComponentScraper

    incidents_scraper = FootballIncidentsComponentScraper(
        webdriver=web, matchid=matches_id_L2[0]
    )

    incidents_scraper.get_data()
    assert incidents_scraper.raw_data is not None, "Expected raw data got none."
    assert isinstance(
        incidents_scraper.raw_data, dict
    ), f"Expect dict data, got {type(incidents_scraper.raw_data)=}."

    print(json.dumps(incidents_scraper.raw_data, indent=4))

    result = incidents_scraper.process()

    assert result is not None, "Error getting the data"

    if result:
        print(result.model_dump_json(indent=6))

    ### match - main part based on the config

    match_scraper = FootballMatchScraper(webdriver=web, matchid=matches_id_L2[0])
    print(OmegaConf.to_yaml(match_scraper.cfg))

    result = match_scraper.scrape()
    if result:
        print(result.model_dump_json(indent=10))

    web.close()
