SeasonScrapingResult
├── Season Metadata
│   ├── tournament_id: int
│   ├── season_id: int
│   └── scraping_duration: float  # seconds
├── Summary Statistics  
│   ├── total_matches: int
│   ├── successful_matches: int
│   ├── failed_matches: int
│   └── errors_summary: List[str]
├── matches: List[SeasonEventSchema]
│   └── SeasonEventSchema (for each match)
│       ├── Match Identifiers
│       │   ├── tournament_id: int
│       │   ├── season_id: int
│       │   ├── match_id: int
│       │   └── scraped_at: str
│       ├── Result Status
│       │   ├── success: bool
│       │   └── error: str (optional)
│       └── data: FootballMatchResultDetailed (optional)
│           └── [See FootballMatchResultDetailed tree - contains base, stats, lineup, incidents, graph, errors]
└── Properties
    └── success_rate: str  # "85.2%" format
