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


##############################
# Football Statistics Schemas
##############################


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


##############################
# Football LineUp Schemas
##############################


class MarketValueSchema(BaseModel):
    """Player market value information"""

    value: Optional[int] = None
    currency: Optional[str] = None


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
    id: int
    firstName: Optional[str] = None
    lastName: Optional[str] = None
    slug: Optional[str] = None
    shortName: Optional[str] = None
    position: Optional[str] = None
    jerseyNumber: Optional[str] = None  # Make optional - some players don't have this
    height: Optional[int] = None
    userCount: Optional[int] = None
    sofascoreId: Optional[str] = None
    country: CountrySchema  # Reuse from general schemas
    marketValueCurrency: Optional[str] = None
    dateOfBirthTimestamp: Optional[int] = None
    proposedMarketValueRaw: Optional[MarketValueSchema] = None


class LineupPlayerEntrySchema(BaseModel):
    """Complete player entry with team info and stats"""

    player: LineupPlayerSchema
    teamId: int
    shirtNumber: Optional[int] = None
    jerseyNumber: Optional[str] = None
    position: Optional[str] = None
    substitute: bool
    captain: Optional[bool] = None
    statistics: Optional[PlayerStatisticsSchema] = None


class PlayerColorSchema(BaseModel):
    """Team kit colors"""

    primary: Optional[str] = None
    number: Optional[str] = None
    outline: Optional[str] = None
    fancyNumber: Optional[str] = None


class MissingPlayerSchema(BaseModel):
    """Information about missing/unavailable players"""

    player: LineupPlayerSchema
    type: Optional[str] = None  # "missing"
    reason: Optional[int] = None


class TeamLineupSchema(BaseModel):
    """Complete team lineup information"""

    players: List[LineupPlayerEntrySchema]
    supportStaff: List[Any] = []  # Usually empty, can be more specific if needed
    formation: Optional[str] = None
    playerColor: Optional[PlayerColorSchema] = None
    goalkeeperColor: Optional[PlayerColorSchema] = None
    missingPlayers: List[MissingPlayerSchema] = []


class FootballLineupSchema(BaseModel):
    """Complete football lineup data"""

    confirmed: bool
    home: TeamLineupSchema
    away: TeamLineupSchema
