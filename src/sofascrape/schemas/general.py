from datetime import datetime
from enum import Enum
from typing import Annotated, Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field, model_validator

##############################
# Enhanced Base Models with Conversion Methods
##############################


class ConvertibleBaseModel(BaseModel):
    """Base class that adds conversion capabilities to all Pydantic models."""

    def to_sql_dict(self, exclude_relations: bool = True) -> Dict[str, Any]:
        """
        Convert to dictionary suitable for SQLModel creation.

        Args:
            exclude_relations: If True, exclude nested objects (default behavior)
        """
        data = self.model_dump()

        if exclude_relations:
            # Remove nested objects by default
            nested_fields = self._get_nested_fields()
            for field in nested_fields:
                data.pop(field, None)

        # Add foreign key mappings if defined
        foreign_keys = self._get_foreign_key_mappings()
        for fk_name, source_path in foreign_keys.items():
            try:
                value = self._get_nested_value(source_path)
                if value is not None:
                    data[fk_name] = value
            except (AttributeError, KeyError):
                # Skip if nested value doesn't exist
                pass

        # Remove None values
        return {k: v for k, v in data.items() if v is not None}

    def _get_nested_fields(self) -> List[str]:
        """Override in subclasses to define which fields are nested objects."""
        return []

    def _get_foreign_key_mappings(self) -> Dict[str, str]:
        """Override in subclasses to define foreign key mappings."""
        return {}

    def _get_nested_value(self, path: str) -> Any:
        """Get value from nested object using dot notation."""
        obj = self
        for attr in path.split("."):
            if hasattr(obj, attr):
                obj = getattr(obj, attr)
            else:
                return None
        return obj


##############################
# tournament
##############################


class SportSchema(ConvertibleBaseModel):
    name: str
    slug: str
    id: int


class CategorySchema(ConvertibleBaseModel):
    name: str
    id: int
    slug: str
    sport: SportSchema

    def _get_nested_fields(self) -> List[str]:
        return ["sport"]

    def _get_foreign_key_mappings(self) -> Dict[str, str]:
        return {"sport_id": "sport.id"}


class TournamentSchema(ConvertibleBaseModel):
    id: int
    name: str
    slug: str
    category: CategorySchema

    def _get_nested_fields(self) -> List[str]:
        return ["category"]

    def _get_foreign_key_mappings(self) -> Dict[str, str]:
        return {"category_id": "category.id"}


class TournamentData(ConvertibleBaseModel):
    tournament: TournamentSchema

    def get_info_string(self) -> str:
        """Return formatted string with tournament info."""
        return f"ID: {self.tournament.id}, Name: {self.tournament.name} ({self.tournament.slug}), Sport: {self.tournament.category.sport.name}"

    def __str__(self) -> str:
        """String representation of the tournament."""
        return self.get_info_string()


##############################
# seasons
##############################


class SeasonSchema(ConvertibleBaseModel):
    name: str
    id: int
    year: str


class SeasonsListSchema(ConvertibleBaseModel):
    seasons: List[SeasonSchema]


##############################
# Events
##############################


class TimeFootballSchema(ConvertibleBaseModel):
    """football"""

    injuryTime1: int = 0
    injuryTime2: int = 0
    currentPeriodStartTimestamp: Optional[int] = None


class ScoreFootballSchema(ConvertibleBaseModel):
    current: Optional[int] = None
    display: Optional[int] = None
    period1: Optional[int] = None
    period2: Optional[int] = None
    normaltime: Optional[int] = None


class CountrySchema(ConvertibleBaseModel):
    name: Optional[str] = None
    slug: Optional[str] = None
    alpha2: Optional[str] = None
    alpha3: Optional[str] = None


class TeamColorsSchema(ConvertibleBaseModel):
    primary: str
    secondary: str
    text: str


class TeamSchema(ConvertibleBaseModel):
    name: str
    slug: str
    shortName: Optional[str] = None
    nameCode: str
    gender: str
    sport: SportSchema
    country: Optional[CountrySchema]
    teamColors: TeamColorsSchema

    def _get_nested_fields(self) -> List[str]:
        return ["sport", "country", "teamColors"]

    def _get_foreign_key_mappings(self) -> Dict[str, str]:
        return {
            "sport_id": "sport.id",
            # Note: country needs special handling since it uses name as key
            # 'country_id': 'country.name',  # Handle this in converter
            # 'team_colors_id': handled after saving teamColors
        }


class RoundInfoSchema(ConvertibleBaseModel):
    round: Optional[int] = None


# TODO: sort this out, optional for all
class StatusSchema(ConvertibleBaseModel):
    code: int
    description: str
    type: str


class EventSchema(ConvertibleBaseModel):
    slug: str
    id: int
    startTimestamp: Optional[int] = None
    status: StatusSchema

    time: Optional[TimeFootballSchema] = None

    tournament: TournamentSchema
    season: SeasonSchema
    roundInfo: Optional[RoundInfoSchema] = None

    winnerCode: Optional[int] = None
    homeScore: Optional[ScoreFootballSchema] = None
    awayScore: Optional[ScoreFootballSchema] = None

    homeTeam: TeamSchema
    awayTeam: TeamSchema

    hasGlobalHighlights: bool = False
    hasXg: bool = False
    hasEventPlayerStatistics: bool = False
    hasEventPlayerHeatMap: bool = False

    def _get_nested_fields(self) -> List[str]:
        return [
            "status",
            "time",
            "tournament",
            "season",
            "roundInfo",
            "homeScore",
            "awayScore",
            "homeTeam",
            "awayTeam",
        ]

    def _get_foreign_key_mappings(self) -> Dict[str, str]:
        return {
            "tournament_id": "tournament.id",
            "season_id": "season.id",
            # Add these after implementing related object saving:
            # 'status_id': 'status.code',  # or handle by code lookup
            # 'round_info_id': handled after saving roundInfo
            # 'home_team_id': handled after saving homeTeam
            # 'away_team_id': handled after saving awayTeam
            # 'home_score_id': handled after saving homeScore
            # 'away_score_id': handled after saving awayScore
            # 'time_id': handled after saving time
        }

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


class EventsListSchema(ConvertibleBaseModel):
    events: List[EventSchema]


##############################
# Football
##############################


class VenueCoordinatesSchema(ConvertibleBaseModel):
    latitude: float
    longitude: float


class CitySchema(ConvertibleBaseModel):
    name: str


class StadiumSchema(ConvertibleBaseModel):
    name: str
    capacity: Optional[int] = None


class VenueSchema(ConvertibleBaseModel):
    name: str
    slug: str
    capacity: Optional[int] = None
    id: int
    city: CitySchema
    venueCoordinates: Optional[VenueCoordinatesSchema] = None
    country: Optional[CountrySchema] = None
    stadium: Optional[StadiumSchema] = None


class ManagerSchema(ConvertibleBaseModel):
    name: str
    slug: str
    shortName: str
    id: int
    country: Optional[CountrySchema] = None

    def _get_nested_fields(self) -> List[str]:
        return ["country"]

    def _get_foreign_key_mappings(self) -> Dict[str, str]:
        return {
            "country_slug": "country.slug",
        }


class RefereeSchema(ConvertibleBaseModel):
    name: str
    slug: str
    id: int
    yellowCards: int
    redCards: int
    yellowRedCards: int
    games: int
    sport: SportSchema
    country: Optional[CountrySchema] = None

    def _get_nested_fields(self) -> List[str]:
        return ["sport", "country"]

    def _get_foreign_key_mappings(self) -> Dict[str, str]:
        return {
            "sport_id": "sport.id",
            "country_id": "country.id",
        }


class FootballTeamSchema(TeamSchema):
    """Extended team schema for football with additional fields"""

    fullName: Optional[str] = None
    manager: Optional[ManagerSchema] = None
    venue: Optional[VenueSchema] = None
    class_: Optional[int] = Field(None, alias="class")

    def _get_nested_fields(self) -> List[str]:
        return ["sport", "country", "teamColors", "manager", "venue"]

    def _get_foreign_key_mappings(self) -> Dict[str, str]:
        return {
            "sport_id": "sport.id",
            # Note: country needs special handling since it uses name as key
            # 'country_id': 'country.name',  # Handle this in converter
            # 'team_colors_id': handled after saving teamColors
        }


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


class FootballDetailsSchema(ConvertibleBaseModel):
    event: FootballEventSchema


##############################
# Football Statistics Schemas
##############################


class FootballStatisticItemSchema(ConvertibleBaseModel):
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

    def _get_nested_fields(self) -> List[str]:
        return []  # No nested objects

    def _get_foreign_key_mappings(self) -> Dict[str, str]:
        return {}  # Foreign keys will be set when saving


class StatisticGroupSchema(ConvertibleBaseModel):
    """Group of related statistics (e.g., "Match overview", "Shots", etc.)"""

    groupName: str
    statisticsItems: List[FootballStatisticItemSchema]

    def _get_nested_fields(self) -> List[str]:
        return ["statisticsItems"]

    def _get_foreign_key_mappings(self) -> Dict[str, str]:
        return {}


class FootballStatisticPeriodSchema(ConvertibleBaseModel):
    """Statistics for a specific period (ALL, 1ST, 2ND, etc.)"""

    period: str  # "ALL", "1ST", "2ND"
    groups: List[StatisticGroupSchema]

    def _get_nested_fields(self) -> List[str]:
        return ["groups"]

    def _get_foreign_key_mappings(self) -> Dict[str, str]:
        return {}


class FootballStatsSchema(ConvertibleBaseModel):
    """Complete football statistics data"""

    statistics: List[FootballStatisticPeriodSchema]

    def _get_nested_fields(self) -> List[str]:
        return ["statistics"]

    def _get_foreign_key_mappings(self) -> Dict[str, str]:
        return {}


##############################
# Football LineUp Schemas
##############################


class MarketValueSchema(ConvertibleBaseModel):
    """Player market value information"""

    value: Optional[int] = None
    currency: Optional[str] = None


class PlayerStatisticsSchema(ConvertibleBaseModel):
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
    ratingVersions: Optional[Dict[str, Optional[float]]] = None

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


class LineupPlayerSchema(ConvertibleBaseModel):
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


class LineupPlayerEntrySchema(ConvertibleBaseModel):
    """Complete player entry with team info and stats"""

    player: LineupPlayerSchema
    teamId: int
    shirtNumber: Optional[int] = None
    jerseyNumber: Optional[str] = None
    position: Optional[str] = None
    substitute: Optional[bool] = None
    captain: Optional[bool] = None
    statistics: Optional[PlayerStatisticsSchema] = None


class PlayerColorSchema(ConvertibleBaseModel):
    """Team kit colors"""

    primary: Optional[str] = None
    number: Optional[str] = None
    outline: Optional[str] = None
    fancyNumber: Optional[str] = None


class MissingPlayerSchema(ConvertibleBaseModel):
    """Information about missing/unavailable players"""

    player: LineupPlayerSchema
    type: Optional[str] = None  # "missing"
    reason: Optional[int] = None


class TeamLineupSchema(ConvertibleBaseModel):
    """Complete team lineup information"""

    players: List[LineupPlayerEntrySchema]
    supportStaff: List[Any] = []  # Usually empty, can be more specific if needed
    formation: Optional[str] = None
    playerColor: Optional[PlayerColorSchema] = None
    goalkeeperColor: Optional[PlayerColorSchema] = None
    missingPlayers: List[MissingPlayerSchema] = []


class FootballLineupSchema(ConvertibleBaseModel):
    """Complete football lineup data"""

    confirmed: bool
    home: TeamLineupSchema
    away: TeamLineupSchema


########################################
#### incidents
########################################


class CoordinatesSchema(ConvertibleBaseModel):
    """X, Y coordinates on the pitch"""

    x: float
    y: float


class PassingNetworkActionSchema(ConvertibleBaseModel):
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


class PeriodIncidentSchema(ConvertibleBaseModel):
    """Period incidents (HT, FT)"""

    incidentType: Literal["period"] = "period"  # Use Literal instead of str
    text: str  # "HT", "FT"
    homeScore: Optional[int]
    awayScore: Optional[int]
    isLive: bool
    time: int
    addedTime: int
    timeSeconds: int
    reversedPeriodTime: int
    reversedPeriodTimeSeconds: int
    periodTimeSeconds: int


class InjuryTimeIncidentSchema(ConvertibleBaseModel):
    """Injury time incidents"""

    incidentType: Literal["injuryTime"] = "injuryTime"  # Use Literal instead of str
    length: int
    time: int
    addedTime: int
    reversedPeriodTime: int


class SubstitutionIncidentSchema(ConvertibleBaseModel):
    """Substitution incidents"""

    incidentType: Literal["substitution"] = "substitution"  # Use Literal instead of str
    playerIn: Optional[LineupPlayerSchema] = None  # Reuse from lineup schemas
    playerOut: Optional[LineupPlayerSchema] = None  # Reuse from lineup schemas
    id: int
    time: int
    addedTime: Optional[int] = None
    injury: bool
    isHome: bool
    incidentClass: str
    reversedPeriodTime: int


class InGamePenaltySchema(ConvertibleBaseModel):
    incidentType: Literal["inGamePenalty"] = (
        "inGamePenalty"  # Use Literal instead of str
    )
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


class CardIncidentSchema(ConvertibleBaseModel):
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


class VarDecisionIncidentSchema(ConvertibleBaseModel):
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


class GoalIncidentSchema(ConvertibleBaseModel):
    """Goal incidents"""

    incidentType: Literal["goal"] = "goal"  # Use Literal instead of str
    homeScore: Optional[int] = None
    awayScore: Optional[int] = None
    player: Optional[LineupPlayerSchema] = None  # Reuse from lineup schemas
    assist1: Optional[LineupPlayerSchema] = None  # Reuse from lineup schemas
    assist2: Optional[LineupPlayerSchema] = None  # Reuse from lineup schemas
    footballPassingNetworkAction: Optional[List[PassingNetworkActionSchema]] = None
    id: int
    time: int
    isHome: bool
    incidentClass: str
    reversedPeriodTime: int


class TeamColorsIncidentSchema(ConvertibleBaseModel):
    """Team colors in incidents"""

    goalkeeperColor: PlayerColorSchema  # Reuse from lineup
    playerColor: PlayerColorSchema


# Add discriminator to the Union using Annotated
IncidentType = Annotated[
    Union[
        InGamePenaltySchema,
        PeriodIncidentSchema,
        InjuryTimeIncidentSchema,
        SubstitutionIncidentSchema,
        CardIncidentSchema,
        GoalIncidentSchema,
        VarDecisionIncidentSchema,
    ],
    Field(discriminator="incidentType"),
]


class FootballIncidentsSchema(ConvertibleBaseModel):
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


class GraphPointSchema(ConvertibleBaseModel):
    """Individual graph point representing match momentum at a specific minute"""

    minute: float  # Can be decimal like 45.5, 90.5 for added time
    value: int  # Momentum value (positive favors home, negative favors away)


class FootballGraphSchema(ConvertibleBaseModel):
    """Complete football graph/momentum data"""

    graphPoints: Optional[List[GraphPointSchema]] = None
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


class ComponentError(ConvertibleBaseModel):
    """Individual component error details"""

    component: str
    status: ComponentStatus
    error_message: Optional[str] = None
    attempted_at: Optional[str] = None  # timestamp


class MatchScrapingErrors(ConvertibleBaseModel):
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


class FootballMatchResultDetailed(ConvertibleBaseModel):
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
    errors: Optional[MatchScrapingErrors] = None

    # Convenience properties for checking data availability
    @property
    def has_base_data(self) -> bool:
        """Check if base component data is available"""
        return self.base is not None

    @property
    def has_stats_data(self) -> bool:
        """Check if stats component data is available"""
        return self.stats is not None

    @property
    def has_lineup_data(self) -> bool:
        """Check if lineup component data is available"""
        return self.lineup is not None

    @property
    def has_incidents_data(self) -> bool:
        """Check if incidents component data is available"""
        return self.incidents is not None

    @property
    def has_graph_data(self) -> bool:
        """Check if graph component data is available"""
        return self.graph is not None

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


##############################
# football season
##############################
class SeasonEventSchema(ConvertibleBaseModel):
    """Individual match result in a season"""

    tournament_id: int
    season_id: int
    match_id: int
    scraped_at: str
    success: bool
    data: Optional[FootballMatchResultDetailed] = None  # Use the simple version
    error: Optional[str] = None


class SeasonScrapingResult(ConvertibleBaseModel):
    """Complete season scraping result"""

    tournament_id: int
    season_id: int
    total_matches: int
    successful_matches: int
    failed_matches: int
    matches: List[SeasonEventSchema]
    scraping_duration: float  # seconds
    errors_summary: List[str] = []

    @property
    def success_rate(self) -> str:
        """Get success rate as percentage"""
        if self.total_matches == 0:
            return "0%"
        percentage = (self.successful_matches / self.total_matches) * 100
        return f"{percentage:.1f}%"
