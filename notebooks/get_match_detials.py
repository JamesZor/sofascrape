import json
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union

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


class VenueCoordinatesSchema(BaseModel):
    latitude: float
    longitude: float


class CitySchema(BaseModel):
    name: str


class StadiumSchema(BaseModel):
    name: str
    capacity: int


class VenueSchema(BaseModel):
    name: str
    slug: str
    capacity: int
    id: int
    city: CitySchema
    venueCoordinates: VenueCoordinatesSchema
    country: schemas.CountrySchema
    stadium: StadiumSchema


class ManagerSchema(BaseModel):
    name: str
    slug: str
    shortName: str
    id: int
    country: schemas.CountrySchema


class RefereeSchema(BaseModel):
    name: str
    slug: str
    id: int
    yellowCards: int
    redCards: int
    yellowRedCards: int
    games: int
    sport: schemas.SportSchema
    country: schemas.CountrySchema


class FootballTeamSchema(schemas.TeamSchema):
    """Extended team schema for football with additional fields"""

    fullName: Optional[str] = None
    manager: Optional[ManagerSchema] = None
    venue: Optional[VenueSchema] = None
    class_: Optional[int] = Field(None, alias="class")


class FootballTournamentSchema(schemas.TournamentSchema):
    """Extended tournament schema for football"""

    competitionType: Optional[int] = None


class FootballEventSchema(schemas.EventSchema):
    """Extended event schema for football with additional fields"""

    attendance: Optional[int] = None
    venue: Optional[VenueSchema] = None
    referee: Optional[RefereeSchema] = None
    defaultPeriodCount: Optional[int] = None
    defaultPeriodLength: Optional[int] = None
    defaultOvertimeLength: Optional[int] = None
    currentPeriodStartTimestamp: Optional[int] = None
    fanRatingEvent: Optional[bool] = None
    seasonStatisticsType: Optional[str] = None
    showTotoPromo: Optional[bool] = None

    # Override team fields to use football-specific team schema
    homeTeam: FootballTeamSchema
    awayTeam: FootballTeamSchema

    # Override tournament to use football-specific tournament schema
    tournament: FootballTournamentSchema


class FootballDetailsSchema(BaseModel):
    event: FootballEventSchema


def process_base_match_pydantic(event):
    try:
        data = FootballDetailsSchema.model_validate(event)

        print(data.model_dump_json(indent=6))
    except Exception as e:
        print(f"Failed to process event : {str(e)}.")


def key_diff(dict1: dict, dict2: dict) -> Tuple[Set[str], Set[str]]:
    s1 = set(dict1.keys())
    s2 = set(dict2.keys())
    intersection = s1.intersection(s2)
    diff = s1 - s2
    return intersection, diff


def notes_on_diff_keys():
    #    s1 = set(base_match.get("event").keys())
    #    s2 = set(event_base.keys())
    #    intersection = s1.intersection(s2)
    #    diff = s1 - s2

    intersection, diff = key_diff(base_match.get("event"), event_base)

    print(f"{intersection =}")
    print("-" * 30)
    print(f"{diff =}")
    print("-" * 30)
    check_event = base_match.get("event")

    for key in intersection:
        print("=" * 30)
        print(key)
        intersection = set()
        diff = set()
        try:
            intersection, diff = key_diff(check_event.get(key), event_base.get(key))
        except Exception as e:
            print(str(e))
        print(f"{intersection =}")
        print("-" * 30)
        print(f"{diff =}")
        print("=" * 30)


def match_event_details():
    nbu_g = NotebookUtils(NoteBookType.GENERAL)
    matchid = 12436870
    playerid = 149380  # Harry Maguire
    #    get_and_save_sample(nbu=nbu, matchid=matchid, playerid=playerid)
    base_match = nbu.load(file_name=f"football_base_match_{matchid}")
    #    print(f"base match keys: {base_match.get('event').keys()}.")
    event_base = nbu_g.load(file_name="event_0_season_61627")
    #    print(f"base base keys: {event_base.keys()}.")

    # base match
    process_base_match_pydantic(base_match)


########################################
#### stats
########################################
class FootballStatisticItemSchema(BaseModel):
    """Individual statistic item within a group"""

    key: str
    name: str
    home: str
    away: str
    compareCode: int
    statisticsType: str  # "positive", "negative"
    valueType: str  # "event", "team"
    homeValue: float  # Can be int or float
    awayValue: float  # Can be int or float
    renderType: int

    # Optional fields for percentage-based stats
    homeTotal: Optional[int] = None
    awayTotal: Optional[int] = None


class StatisticGroupSchema(BaseModel):
    """Group of related statistics (e.g., "Match overview", "Shots", etc.)"""

    groupName: str
    statisticsItems: List[FootballStatisticItemSchema]


class FootballStatisticPeriodSchema(BaseModel):
    """Statistics for a specific period (ALL, 1ST, 2ND, etc.)"""

    period: str  # "ALL", "1ST", "2ND"
    groups: List[StatisticGroupSchema]


class FootballStatsSchema(BaseModel):
    """Complete football statistics data"""

    statistics: List[FootballStatisticPeriodSchema]


def process_stats_pydantic(data):
    try:
        results = FootballStatsSchema.model_validate(data)

        print(results.model_dump_json(indent=6))
    except Exception as e:
        print(f"Failed with error : {str(e)}.")


def stats_main():
    matchid = 12436870
    playerid = 149380  # Harry Maguire
    linkT = LinkType.STATS
    raw_stats = nbu.load(file_name=f"{linkT.value}_{matchid}")
    process_stats_pydantic(raw_stats)


########################################
#### lineup
########################################


class MarketValueSchema(BaseModel):
    """Player market value information"""

    value: int
    currency: str


class PlayerStatisticsSchema(BaseModel):
    """Player match statistics - all fields optional as different players have different stats"""

    totalPass: Optional[int] = None
    accuratePass: Optional[int] = None
    totalLongBalls: Optional[int] = None
    accurateLongBalls: Optional[int] = None
    goalAssist: Optional[int] = None
    totalCross: Optional[int] = None
    accurateCross: Optional[int] = None
    aerialLost: Optional[int] = None
    aerialWon: Optional[int] = None
    duelLost: Optional[int] = None
    duelWon: Optional[int] = None
    challengeLost: Optional[int] = None
    totalContest: Optional[int] = None
    wonContest: Optional[int] = None
    dispossessed: Optional[int] = None
    totalClearance: Optional[int] = None
    outfielderBlock: Optional[int] = None
    interceptionWon: Optional[int] = None
    totalTackle: Optional[int] = None
    wasFouled: Optional[int] = None
    fouls: Optional[int] = None
    totalOffside: Optional[int] = None
    minutesPlayed: Optional[int] = None
    touches: Optional[int] = None
    rating: Optional[float] = None
    possessionLostCtrl: Optional[int] = None
    expectedGoals: Optional[float] = None
    expectedAssists: Optional[float] = None
    keyPass: Optional[int] = None
    ratingVersions: Optional[Dict[str, float]] = None

    # Goalkeeper specific stats
    goodHighClaim: Optional[int] = None
    savedShotsFromInsideTheBox: Optional[int] = None
    saves: Optional[int] = None
    totalKeeperSweeper: Optional[int] = None
    accurateKeeperSweeper: Optional[int] = None
    goalsPrevented: Optional[float] = None
    errorLeadToAShot: Optional[int] = None
    punches: Optional[int] = None

    # Attacking stats
    bigChanceCreated: Optional[int] = None
    bigChanceMissed: Optional[int] = None
    shotOffTarget: Optional[int] = None
    onTargetScoringAttempt: Optional[int] = None
    blockedScoringAttempt: Optional[int] = None
    goals: Optional[int] = None


class LineupPlayerSchema(BaseModel):
    """Player information in lineup"""

    name: str
    firstName: Optional[str] = None
    lastName: Optional[str] = None
    slug: str
    shortName: str
    position: str
    jerseyNumber: Optional[str] = None  # Make optional - some players don't have this
    height: Optional[int] = None
    userCount: Optional[int] = None
    sofascoreId: Optional[str] = None
    id: int
    country: schemas.CountrySchema  # Reuse from general schemas
    marketValueCurrency: Optional[str] = None
    dateOfBirthTimestamp: Optional[int] = None
    proposedMarketValueRaw: Optional[MarketValueSchema] = None


class LineupPlayerEntrySchema(BaseModel):
    """Complete player entry with team info and stats"""

    player: LineupPlayerSchema
    teamId: int
    shirtNumber: int
    jerseyNumber: str
    position: str
    substitute: bool
    captain: Optional[bool] = None
    statistics: PlayerStatisticsSchema


class PlayerColorSchema(BaseModel):
    """Team kit colors"""

    primary: str
    number: str
    outline: str
    fancyNumber: str


class MissingPlayerSchema(BaseModel):
    """Information about missing/unavailable players"""

    player: LineupPlayerSchema
    type: str  # "missing"
    reason: int


class TeamLineupSchema(BaseModel):
    """Complete team lineup information"""

    players: List[LineupPlayerEntrySchema]
    supportStaff: List[Any] = []  # Usually empty, can be more specific if needed
    formation: str
    playerColor: PlayerColorSchema
    goalkeeperColor: PlayerColorSchema
    missingPlayers: List[MissingPlayerSchema] = []


class FootballLineupSchema(BaseModel):
    """Complete football lineup data"""

    confirmed: bool
    home: TeamLineupSchema
    away: TeamLineupSchema


def process_lineup_pydantic(data):
    try:
        results = FootballLineupSchema.model_validate(data)

        print(results.model_dump_json(indent=6))
    except Exception as e:
        print(f"Failed with error : {str(e)}.")


if __name__ == "__main__":
    ### Set up for the notebook
    nbu = NotebookUtils(NoteBookType.FOOTBALL)
    matchid = 12436870
    playerid = 149380  # Harry Maguire
    linkT = LinkType.LINEUP
    raw = nbu.load(file_name=f"{linkT.value}_{matchid}")
    process_lineup_pydantic(raw)
#    print(json.dumps(raw, indent=8))
#    process_stats_pydantic(raw_stats)
