from datetime import datetime
from enum import Enum
from typing import Annotated, Any, Dict, List, Literal, Optional, Union

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
    jerseyNumber: Optional[str] = None
    height: Optional[int] = None
    userCount: Optional[int] = None
    sofascoreId: Optional[str] = None
    country: Optional[CountrySchema] = None
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


########################################
#### incidents
########################################
class CoordinatesSchema(BaseModel):
    """X, Y coordinates on the pitch"""

    x: float
    y: float


class PassingNetworkActionSchema(BaseModel):
    """Individual action in passing network leading to goal"""

    player: LineupPlayerSchema  # Reuse from lineup schemas
    eventType: str  # "pass", "ball-movement", "goal"
    time: int
    playerCoordinates: CoordinatesSchema
    passEndCoordinates: Optional[CoordinatesSchema] = None
    gkCoordinates: Optional[CoordinatesSchema] = None
    goalShotCoordinates: Optional[CoordinatesSchema] = None
    goalMouthCoordinates: Optional[CoordinatesSchema] = None
    goalkeeper: Optional[LineupPlayerSchema] = None  # Reuse from lineup schemas
    isHome: bool
    isAssist: Optional[bool] = None
    bodyPart: Optional[str] = None  # "left-foot", "right-foot", "head"
    goalType: Optional[str] = None  # "regular", "penalty", "own-goal"


class PeriodIncidentSchema(BaseModel):
    """Period incidents (HT, FT)"""

    incidentType: Literal["period"] = "period"  # Use Literal instead of str
    text: str  # "HT", "FT"
    homeScore: int
    awayScore: int
    isLive: bool
    time: int
    addedTime: int
    timeSeconds: int
    reversedPeriodTime: int
    reversedPeriodTimeSeconds: int
    periodTimeSeconds: int


class InjuryTimeIncidentSchema(BaseModel):
    """Injury time incidents"""

    incidentType: Literal["injuryTime"] = "injuryTime"  # Use Literal instead of str
    length: int
    time: int
    addedTime: int
    reversedPeriodTime: int


class SubstitutionIncidentSchema(BaseModel):
    """Substitution incidents"""

    incidentType: Literal["substitution"] = "substitution"  # Use Literal instead of str
    playerIn: LineupPlayerSchema  # Reuse from lineup schemas
    playerOut: LineupPlayerSchema  # Reuse from lineup schemas
    id: int
    time: int
    addedTime: Optional[int] = None
    injury: bool
    isHome: bool
    incidentClass: str
    reversedPeriodTime: int


class CardIncidentSchema(BaseModel):
    """Card incidents (yellow, red)"""

    incidentType: Literal["card"] = "card"  # Use Literal instead of str
    # Make fields optional that might not be present in the data
    player: Optional[LineupPlayerSchema] = None  # Reuse from lineup schemas
    playerName: Optional[str] = None
    reason: Optional[str] = None
    rescinded: Optional[bool] = None
    id: Optional[int] = None
    time: Optional[int] = None
    isHome: Optional[bool] = None
    incidentClass: Optional[str] = None  # "yellow", "red"
    reversedPeriodTime: Optional[int] = None


class VarDecisionIncidentSchema(BaseModel):
    """VAR decision incidents"""

    incidentType: Literal["varDecision"] = "varDecision"
    # Based on the error, these incidents have at least 'confirmed' and 'play...' fields
    confirmed: Optional[bool] = None
    # Add other fields as optional since we don't know the exact structure
    player: Optional[LineupPlayerSchema] = None
    time: Optional[int] = None
    isHome: Optional[bool] = None
    incidentClass: Optional[str] = None
    reversedPeriodTime: Optional[int] = None
    decision: Optional[str] = None
    reason: Optional[str] = None


class GoalIncidentSchema(BaseModel):
    """Goal incidents"""

    incidentType: Literal["goal"] = "goal"  # Use Literal instead of str
    homeScore: int
    awayScore: int
    player: LineupPlayerSchema  # Reuse from lineup schemas
    assist1: Optional[LineupPlayerSchema] = None  # Reuse from lineup schemas
    assist2: Optional[LineupPlayerSchema] = None  # Reuse from lineup schemas
    footballPassingNetworkAction: Optional[List[PassingNetworkActionSchema]] = None
    id: int
    time: int
    isHome: bool
    incidentClass: str
    reversedPeriodTime: int


class TeamColorsIncidentSchema(BaseModel):
    """Team colors in incidents"""

    goalkeeperColor: PlayerColorSchema  # Reuse from lineup
    playerColor: PlayerColorSchema


# Add discriminator to the Union using Annotated
IncidentType = Annotated[
    Union[
        PeriodIncidentSchema,
        InjuryTimeIncidentSchema,
        SubstitutionIncidentSchema,
        CardIncidentSchema,
        GoalIncidentSchema,
        VarDecisionIncidentSchema,
    ],
    Field(discriminator="incidentType"),
]


class FootballIncidentsSchema(BaseModel):
    """Complete football incidents data"""

    incidents: List[IncidentType]  # Remove discriminator from here
    home: TeamColorsIncidentSchema
    away: TeamColorsIncidentSchema

    class Config:
        # Allow extra fields for future incident types
        extra = "allow"


##############################
# Football Graph Schemas
##############################


class GraphPointSchema(BaseModel):
    """Individual graph point representing match momentum at a specific minute"""

    minute: float  # Can be decimal like 45.5, 90.5 for added time
    value: int  # Momentum value (positive favors home, negative favors away)


class FootballGraphSchema(BaseModel):
    """Complete football graph/momentum data"""

    graphPoints: List[GraphPointSchema]
    periodTime: int  # Length of each period (usually 45 minutes)
    overtimeLength: int  # Length of overtime periods (usually 15 minutes)
    periodCount: int  # Number of periods (usually 2 for football)


##############################
# football match
##############################


class ComponentStatus(str, Enum):
    """Status of individual component scraping"""

    SUCCESS = "success"
    FAILED = "failed"
    NOT_ATTEMPTED = "not_attempted"


class ComponentError(BaseModel):
    """Individual component error details"""

    component: str
    status: ComponentStatus
    error_message: Optional[str] = None
    attempted_at: Optional[str] = None  # timestamp


class MatchScrapingErrors(BaseModel):
    """Detailed error tracking for all components"""

    base: ComponentError
    stats: ComponentError
    lineup: ComponentError
    incidents: ComponentError
    graph: ComponentError

    @property
    def has_errors(self) -> bool:
        """Check if any component failed"""
        return any(
            error.status == ComponentStatus.FAILED
            for error in [
                self.base,
                self.stats,
                self.lineup,
                self.incidents,
                self.graph,
            ]
        )

    @property
    def successful_components(self) -> List[str]:
        """Get list of successful component names"""
        return [
            error.component
            for error in [
                self.base,
                self.stats,
                self.lineup,
                self.incidents,
                self.graph,
            ]
            if error.status == ComponentStatus.SUCCESS
        ]

    @property
    def failed_components(self) -> List[str]:
        """Get list of failed component names"""
        return [
            error.component
            for error in [
                self.base,
                self.stats,
                self.lineup,
                self.incidents,
                self.graph,
            ]
            if error.status == ComponentStatus.FAILED
        ]


class FootballMatchResultDetailed(BaseModel):
    """Complete football match data with detailed error tracking"""

    # Match metadata
    match_id: int
    scraped_at: str = Field(default_factory=lambda: str(datetime.now()))

    # Component data (None if failed/not attempted)
    base: Optional[FootballDetailsSchema] = None
    stats: Optional[FootballStatsSchema] = None
    lineup: Optional[FootballLineupSchema] = None
    incidents: Optional[FootballIncidentsSchema] = None
    graph: Optional[FootballGraphSchema] = None

    # Detailed error tracking
    errors: MatchScrapingErrors

    @property
    def success_rate(self) -> str:
        """Get success rate as string"""
        successful = len(self.errors.successful_components)
        total = successful + len(self.errors.failed_components)
        return f"{successful}/{total}"

    @property
    def has_base_data(self) -> bool:
        """Check if base data is available"""
        return (
            self.base is not None and self.errors.base.status == ComponentStatus.SUCCESS
        )

    def get_match_info(self) -> Optional[Dict]:
        """Get basic match info from base data"""
        if not self.base:
            return None

        event = self.base.event
        return {
            "home_team": event.homeTeam.name,
            "away_team": event.awayTeam.name,
            "score": f"{event.homeScore.current if event.homeScore else 0}-{event.awayScore.current if event.awayScore else 0}",
            "status": event.status.description,
            "venue": event.venue.name if event.venue else None,
        }

    def get_error_summary(self) -> str:
        """Get a summary of all errors"""
        if not self.errors.has_errors:
            return "No errors"

        failed = self.errors.failed_components
        return f"Failed components: {', '.join(failed)}"
