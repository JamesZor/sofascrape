import json
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from hydra import compose, initialize
from omegaconf import DictConfig, OmegaConf
from pydantic import BaseModel, Field, ValidationError, model_validator
from webdriver import ManagerWebdriver

import sofascrape.schemas.general as schemas
from sofascrape.utils import NoteBookType, NotebookUtils


class LinkType(Enum):
    BASE = "football_base_match"
    STATS = "football_stats"
    LINEUP = "football_lineup"
    INCIDENTS = "football_incidents"
    GRAPH = "football_graph"
    HEATMAP = "football_heatmap"


def get_match_data(
    link_type: LinkType, matchid: int = 12436870, playerid: Optional[int] = None
) -> Any:
    url_template = getattr(nbu.cfg.links, link_type.value)
    if playerid is None:
        url = url_template.format(match_id=matchid)
    else:
        url = url_template.format(match_id=matchid, player_id=playerid)
    return nbu.driver.get_page(url)


def get_and_save_sample(nbu: NotebookUtils, matchid: int, playerid: int):
    for link_type in LinkType:
        if link_type.value != "football_heatmap":
            nbu.save(
                file_name=f"{link_type.value}_{matchid}",
                data=get_match_data(link_type=link_type, matchid=matchid),
                indent=8,
            )
        else:
            nbu.save(
                file_name=f"{link_type.value}_{matchid}",
                data=get_match_data(
                    link_type=link_type, matchid=matchid, playerid=playerid
                ),
                indent=8,
            )


# pydantic


# base  match
class FootballEventSchema(schemas.EventSchema):
    attendance: Optional[int] = None


class FootballDetailsSchema(BaseModel):
    event: FootballEventSchema


def process_base_match_pydantic(event):
    try:
        data = FootballDetailsSchema.model_validate(event)

        print(data)
    except Exception as e:
        print(f"Failed to process event : {str(e)}.")


if __name__ == "__main__":
    ### Set up for the notebook
    nbu = NotebookUtils(NoteBookType.FOOTBALL)
    nbu_g = NotebookUtils(NoteBookType.GENERAL)
    matchid = 12436870
    playerid = 149380  # Harry Maguire
    #    get_and_save_sample(nbu=nbu, matchid=matchid, playerid=playerid)
    base_match = nbu.load(file_name=f"football_base_match_{matchid}")
    print(f"base match keys: {base_match.get('event').keys()}.")

    event_base = nbu_g.load(file_name="event_0_season_61627")
    print(f"base base keys: {event_base.keys()}.")

    s1 = set(base_match.get("event").keys())
    s2 = set(event_base.keys())
    intersection = s1.intersection(s2)
    diff = s1 - s2

    print(f"{intersection =} .... {diff =}")

"""
intersection ={'finalResultOnly', 'roundInfo', 'varInProgress', 'hasEventPlayerStatistics', 'awayTeam', 'changes', 'time', 'tournament', 'winnerCode', 'hasGlobalHighlights', 'hasEventPlayerHeatMap', 'status', 'awayScore', 'crowdsourcingDataDisplayEnabled', 'homeScore', 'feedLocked', 'slug', 'homeTeam', 'season', 'id', 'hasXg', 'customId', 'isEditor', 'detailId', 'startTimestamp'} .
diff ={'defaultPeriodLength', 'defaultPeriodCount', 'attendance', 'defaultOvertimeLength', 'venue', 'showTotoPromo', 'fanRatingEvent', 'referee', 'seasonStatisticsType', 'currentPeriodStartTimestamp'}
"""
# base match
#    process_base_match_pydantic(base_match)
