FootballMatchResultDetailed
├── match_id: int
├── scraped_at: str
├── base: FootballDetailsSchema (optional)
│   └── event: FootballEventSchema
│       ├── Basic Match Info
│       │   ├── slug: str
│       │   ├── id: int
│       │   ├── startTimestamp: int
│       │   ├── winnerCode: int (optional)
│       │   ├── hasGlobalHighlights: bool
│       │   ├── hasXg: bool
│       │   ├── hasEventPlayerStatistics: bool
│       │   └── hasEventPlayerHeatMap: bool
│       ├── Match Status & Time
│       │   ├── status: StatusSchema
│       │   │   ├── code: int
│       │   │   ├── description: str
│       │   │   └── type: str
│       │   └── time: TimeFootballSchema (optional)
│       │       ├── injuryTime1: int
│       │       ├── injuryTime2: int
│       │       └── currentPeriodStartTimestamp: int
│       ├── Competition Info
│       │   ├── tournament: FootballTournamentSchema
│       │   │   ├── id: int
│       │   │   ├── name: str
│       │   │   ├── slug: str
│       │   │   ├── competitionType: int (optional)
│       │   │   └── category: CategorySchema
│       │   │       ├── name: str
│       │   │       ├── id: int
│       │   │       ├── slug: str
│       │   │       └── sport: SportSchema
│       │   │           ├── name: str
│       │   │           ├── slug: str
│       │   │           └── id: int
│       │   ├── season: SeasonSchema
│       │   │   ├── name: str
│       │   │   ├── id: int
│       │   │   └── year: str
│       │   └── roundInfo: RoundInfoSchema
│       │       └── round: int
│       ├── Scores
│       │   ├── homeScore: ScoreFootballSchema (optional)
│       │   │   ├── current: int (optional)
│       │   │   ├── display: int (optional)
│       │   │   ├── period1: int (optional)
│       │   │   ├── period2: int (optional)
│       │   │   └── normaltime: int (optional)
│       │   └── awayScore: ScoreFootballSchema (optional)
│       │       └── [same structure as homeScore]
│       ├── Teams
│       │   ├── homeTeam: FootballTeamSchema
│       │   │   ├── Basic Team Info
│       │   │   │   ├── name: str
│       │   │   │   ├── slug: str
│       │   │   │   ├── shortName: str (optional)
│       │   │   │   ├── nameCode: str
│       │   │   │   ├── fullName: str (optional)
│       │   │   │   ├── gender: str
│       │   │   │   └── class_: int (optional)
│       │   │   ├── Location & Identity
│       │   │   │   ├── sport: SportSchema
│       │   │   │   ├── country: CountrySchema
│       │   │   │   │   ├── name: str
│       │   │   │   │   ├── slug: str
│       │   │   │   │   ├── alpha2: str
│       │   │   │   │   └── alpha3: str
│       │   │   │   └── teamColors: TeamColorsSchema
│       │   │   │       ├── primary: str
│       │   │   │       ├── secondary: str
│       │   │   │       └── text: str
│       │   │   ├── Staff & Venue
│       │   │   │   ├── manager: ManagerSchema (optional)
│       │   │   │   │   ├── name: str
│       │   │   │   │   ├── slug: str
│       │   │   │   │   ├── shortName: str
│       │   │   │   │   ├── id: int
│       │   │   │   │   └── country: CountrySchema (optional)
│       │   │   │   └── venue: VenueSchema (optional)
│       │   │   │       ├── name: str
│       │   │   │       ├── slug: str
│       │   │   │       ├── capacity: int (optional)
│       │   │   │       ├── id: int
│       │   │   │       ├── city: CitySchema
│       │   │   │       ├── venueCoordinates: VenueCoordinatesSchema (optional)
│       │   │   │       ├── country: CountrySchema (optional)
│       │   │   │       └── stadium: StadiumSchema (optional)
│       │   └── awayTeam: FootballTeamSchema
│       │       └── [same structure as homeTeam]
│       └── Additional Match Details
│           ├── attendance: int (optional)
│           ├── venue: VenueSchema (optional)
│           ├── referee: RefereeSchema (optional)
│           │   ├── name: str
│           │   ├── slug: str
│           │   ├── id: int
│           │   ├── yellowCards: int
│           │   ├── redCards: int
│           │   ├── yellowRedCards: int
│           │   ├── games: int
│           │   ├── sport: SportSchema
│           │   └── country: CountrySchema
│           ├── defaultPeriodCount: int (optional)
│           ├── defaultPeriodLength: int (optional)
│           ├── defaultOvertimeLength: int (optional)
│           ├── currentPeriodStartTimestamp: int (optional)
│           ├── fanRatingEvent: bool (optional)
│           ├── seasonStatisticsType: str (optional)
│           └── showTotoPromo: bool (optional)
├── stats: FootballStatsSchema (optional)
│   └── statistics: List[FootballStatisticPeriodSchema]
│       └── [Each period contains groups of statistics with home/away values]
├── lineup: FootballLineupSchema (optional)
│   ├── confirmed: bool
│   ├── home: TeamLineupSchema
│   │   ├── players: List[LineupPlayerEntrySchema]
│   │   │   └── [Player details, position, substitute status, and match statistics]
│   │   ├── formation: str (optional)
│   │   ├── playerColor: PlayerColorSchema (optional)
│   │   ├── goalkeeperColor: PlayerColorSchema (optional)
│   │   └── missingPlayers: List[MissingPlayerSchema]
│   └── away: TeamLineupSchema
│       └── [same structure as home]
├── incidents: FootballIncidentsSchema (optional)
│   ├── incidents: List[IncidentType]
│   │   └── [Goals, cards, substitutions, VAR decisions, periods, injury time]
│   ├── home: TeamColorsIncidentSchema
│   └── away: TeamColorsIncidentSchema
├── graph: FootballGraphSchema (optional)
│   ├── graphPoints: List[GraphPointSchema]
│   │   └── [Minute-by-minute match momentum data]
│   ├── periodTime: int
│   ├── overtimeLength: int
│   └── periodCount: int
└── errors: MatchScrapingErrors
    ├── base: ComponentError
    │   ├── component: str
    │   ├── status: ComponentStatus
    │   ├── error_message: str (optional)
    │   └── attempted_at: str (optional)
    ├── stats: ComponentError
    ├── lineup: ComponentError  
    ├── incidents: ComponentError
    ├── graph: ComponentError
    └── Properties:
        ├── has_errors: bool
        ├── successful_components: List[str]
        └── failed_components: List[str]
