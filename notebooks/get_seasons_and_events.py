import json
from typing import Any, Dict, List, Optional, Union

from hydra import compose, initialize
from omegaconf import DictConfig, OmegaConf
from pydantic import BaseModel, Field, ValidationError, model_validator
from webdriver import ManagerWebdriver

import sofascrape.schemas.general as generalSchemas
from sofascrape.utils import NotebookSaveLoad


# ns = NotebookSaveLoad()
# data = ns.load(file_name="tournament_1")
# print(json.dumps(data, indent=4))
#
def create_global_cfg():
    with initialize(config_path="../src/sofascrape/conf", version_base="1.3"):
        cfg = compose(config_name="notebook_general.yaml")
    return cfg


def get_season_json(cfg: DictConfig, tournamentid: int = 1) -> Dict:
    webmanger = ManagerWebdriver()
    d1 = webmanger.spawn_webdriver()
    url: str = cfg.links.seasons_empty.format(tournamentID=tournamentid)
    page_data = d1.get_page(url)
    d1.close()
    return page_data


def process_get():
    ns = NotebookSaveLoad()
    cfg = create_global_cfg()
    print(OmegaConf.to_yaml(cfg))
    tournamentid = 1
    url: str = cfg.links.seasons_empty.format(tournamentID=tournamentid)
    print(f"{url=}")

    data = get_season_json(cfg, 1)
    print(data)
    ns.save("seasons_1", data, indent=2)


## test pydantic


class SeasonSchema(BaseModel):
    name: str
    id: int
    year: str


class SeasonsListSchema(BaseModel):
    seasons: List[SeasonSchema]


def process_season(seaosn: dict):
    try:
        season_data = SeasonSchema.model_validate(seaosn)
        return season_data
    except Exception as e:
        print(f"Processing failed: {str(e)}.")
        raise e


def process_season_list(season_list: List):
    try:
        season_data = SeasonsListSchema.model_validate(season_list)
        return season_data
    except Exception as e:
        print(f"Processing failed: {str(e)}.")
        raise e


def pydantic_example(seaon):
    result = process_season(seaon)
    if result:
        print(result)


def pydantic_example_1(data):
    result = process_season_list(data)
    if result:
        print(f"Found {len(result.seasons)} seasons")
        for season in result.seasons[:3]:  # Show first 3
            print(season)
            # print(f"- {season.name}: {season.year}")


# seasons
def seasons_main_func():
    ns = NotebookSaveLoad()
    data = ns.load(file_name="seasons_1")
    pydantic_example_1(data)


### For events
def get_events_in_season(cfg: DictConfig, tournamentid: int, seasonid: int) -> Dict:
    webmanger = ManagerWebdriver()
    d1 = webmanger.spawn_webdriver()
    url: str = cfg.links.events_season_empty.format(
        tournamentID=tournamentid, seasonID=seasonid
    )
    page_date = d1.get_page(url)

    d1.close()
    return page_date


def main_event_run():
    cfg: DictConfig = create_global_cfg()
    tournamentid: int = 1
    seasonid: int = 61627

    page_date = get_events_in_season(
        cfg=cfg, tournamentid=tournamentid, seasonid=seasonid
    )

    ns = NotebookSaveLoad()
    ns.save(file_name=f"events_season_{seasonid}", data=page_date, indent=4)


def load_eventlist(seasonid: int = 61627):
    ns = NotebookSaveLoad()
    return ns.load(file_name=f"events_season_{seasonid}")


def save_event_from_events(event_id: int = 0):
    ns = NotebookSaveLoad()
    data = ns.load(file_name="events_season_61627")
    #    event_0_data = json.dumps(data.get("events")[event_id], indent=6)
    event_0_data = data.get("events")[event_id]
    file_name: str = f"event_{event_id}_season_61627"
    ns.save(file_name=file_name, data=event_0_data, indent=6)
    print(f"{file_name=}")


def load_event_from_events(event_id: int = 0):
    ns = NotebookSaveLoad()
    file_name: str = f"event_{event_id}_season_61627"
    return ns.load(file_name=file_name)


# event pydantic schema
class TimeFootballSchema(BaseModel):
    """football"""

    injuryTime1: int
    injuryTime2: int
    currentPeriodStartTimestamp: int


class ScoreFootballSchema(BaseModel):
    current: int
    display: int
    period1: int
    period2: int
    normaltime: int


class CountrySchema(BaseModel):
    name: str
    slug: str
    alpha2: str
    alpha3: str


class TeamColorsSchema(BaseModel):
    primary: str
    secondary: str
    text: str


class TeamSchema(BaseModel):
    name: str
    slug: str
    shortName: str
    nameCode: str
    gender: str
    sport: generalSchemas.SportSchema
    country: CountrySchema
    teamColors: TeamColorsSchema


class RoundInfoSchema(BaseModel):
    round: int


class StatusSchema(BaseModel):
    code: int
    description: str
    type: str


class EventSchema(BaseModel):
    slug: str
    id: int
    startTimestamp: int
    status: StatusSchema

    time: Optional[TimeFootballSchema]

    tournament: generalSchemas.TournamentSchema
    season: generalSchemas.SeasonSchema
    roundInfo: RoundInfoSchema

    winnerCode: Optional[int]
    homeScore: Optional[ScoreFootballSchema]
    awayScore: Optional[ScoreFootballSchema]

    homeTeam: TeamSchema
    awayTeam: TeamSchema

    hasGlobalHighlights: bool
    hasXg: bool
    hasEventPlayerStatistics: bool
    hasEventPlayerHeatMap: bool

    @model_validator(mode="before")
    @classmethod
    def handle_match_status(cls, data: Any) -> Any:
        """Handle the cases when the event is postponed etc via the status,
        then set the appropriate field requirements
        """

        if isinstance(data, Dict):
            # get the status details             status_info = data.get('status', {})
            status_info = data.get("status", {})
            status_type = status_info.get("type", "").lower()

            # For postponed, cancelled, or not started matches
            if status_type in ["postponed", "cancelled", "notstarted"]:
                # Convert empty dicts to None
                empty_dict_fields = ["time", "homeScore", "awayScore"]
                for field in empty_dict_fields:
                    if field in data and data[field] == {}:
                        data[field] = None

                # No winner for these statuses
                if "winnerCode" not in data:
                    data["winnerCode"] = None

        return data


class EventsListSchema(BaseModel):
    events: List[EventSchema]


def process_event_pydantic(event: dict):
    try:
        event_data = EventSchema.model_validate(event)
        return event_data
    except Exception as e:
        print(f"Failed to process event: {str(e)}.")
        raise e


def process_eventlist_pydantic(events: dict):
    try:
        event_data = EventsListSchema.model_validate(events)
        return event_data
    except Exception as e:
        print(f"Failed to process event: {str(e)}.")
        raise e


"""
    Some events are postponed and hence currently cause an issue 
    with pydantic due to incorrect value types been seen. 

    in event.status = { code:60, description:postponed, type: postponed}
    # TODO
"""

if __name__ == "__main__":
    ### Handel postponed data
    #    event_140 = load_event_from_events(140)
    #    print(json.dumps(event_140, indent=6))
    #
    #    result = process_event_pydantic(event_140)
    #    if result:
    #        print(result)
    ### list processing
    event_data: dict = load_eventlist()
    result = process_eventlist_pydantic(event_data)

    if result:
        for event in result.events[:2]:
            print(event)

        valid_count: int = 0
        not_valid_count: int = 0
        for event in result.events:
            if event.status.code == 100:
                valid_count += 1
            else:
                not_valid_count += 1

        print("+" * 20)
        print(f" {valid_count =} ..... {not_valid_count=}")
