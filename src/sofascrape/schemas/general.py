from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, model_validator

##############################
# tournament
##############################


class SportSchema(BaseModel):
    name: str
    slug: str
    id: int


class CategorySchema(BaseModel):
    name: str
    id: int
    slug: str
    sport: SportSchema


class TournamentSchema(BaseModel):
    id: int
    name: str
    slug: str
    category: CategorySchema


class TournamentData(BaseModel):
    tournament: TournamentSchema


##############################
# seasons
##############################


class SeasonSchema(BaseModel):
    name: str
    id: int
    year: str


class SeasonsListSchema(BaseModel):
    seasons: List[SeasonSchema]


##############################
# Events
##############################


class TimeFootballSchema(BaseModel):
    """football"""

    injuryTime1: int = 0
    injuryTime2: int = 0
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
    sport: SportSchema
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

    tournament: TournamentSchema
    season: SeasonSchema
    roundInfo: RoundInfoSchema

    winnerCode: Optional[int]
    homeScore: Optional[ScoreFootballSchema]
    awayScore: Optional[ScoreFootballSchema]

    homeTeam: TeamSchema
    awayTeam: TeamSchema

    hasGlobalHighlights: bool = False
    hasXg: bool = False
    hasEventPlayerStatistics: bool = False
    hasEventPlayerHeatMap: bool = False

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


##############################
# Football
##############################


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
    venueCoordinates: Optional[VenueCoordinatesSchema] = None
    country: Optional[CountrySchema] = None
    stadium: Optional[StadiumSchema] = None


class ManagerSchema(BaseModel):
    name: str
    slug: str
    shortName: str
    id: int
    country: CountrySchema


class RefereeSchema(BaseModel):
    name: str
    slug: str
    id: int
    yellowCards: int
    redCards: int
    yellowRedCards: int
    games: int
    sport: SportSchema
    country: CountrySchema


class FootballTeamSchema(TeamSchema):
    """Extended team schema for football with additional fields"""

    fullName: Optional[str] = None
    manager: Optional[ManagerSchema] = None
    venue: Optional[VenueSchema] = None
    class_: Optional[int] = Field(None, alias="class")


class FootballTournamentSchema(TournamentSchema):
    """Extended tournament schema for football"""

    competitionType: Optional[int] = None


class FootballEventSchema(EventSchema):
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
