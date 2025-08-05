# Code Examples 

## Core Data Models

### data_models.py
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from enum import Enum
from datetime import datetime

class ComponentStatus(Enum):
    VALID = "valid"
    INVALID = "invalid"
    MISSING = "missing"
    ERROR = "error"

@dataclass
class ComponentQuality:
    """Quality assessment for a single component of a match"""
    component_name: str
    status: ComponentStatus
    hash_value: Optional[str] = None
    error_message: Optional[str] = None
    data_present: bool = False
    
    @property 
    def is_valid(self) -> bool:
        return self.status == ComponentStatus.VALID

@dataclass
class MatchQuality:
    """Quality assessment for all components of a single match"""
    match_id: int
    run_id: str
    timestamp: datetime
    components: Dict[str, ComponentQuality] = field(default_factory=dict)
    
    @property
    def valid_components(self) -> List[str]:
        # TODO: return list of component names that are valid
        pass
    
    @property 
    def invalid_components(self) -> List[str]:
        # TODO: return list of component names that are invalid
        pass
    
    @property
    def is_complete(self) -> bool:
        # TODO: check if all required components are valid
        pass

@dataclass
class ComponentConsensus:
    """Consensus analysis for a single component across multiple runs"""
    component_name: str
    match_id: int
    consensus_hash: Optional[str] = None
    consensus_data: Optional[Any] = None
    agreeing_runs: List[str] = field(default_factory=list)
    disagreeing_runs: List[str] = field(default_factory=list)
    
    @property
    def has_consensus(self) -> bool:
        # TODO: check if this component has consensus across runs
        pass
    
    @property
    def needs_retry(self) -> bool:
        # TODO: determine if this component needs to be re-scraped
        pass

@dataclass
class MatchConsensus:
    """Consensus analysis for all components of a single match"""
    match_id: int
    components: Dict[str, ComponentConsensus] = field(default_factory=dict)
    
    @property
    def components_needing_retry(self) -> List[str]:
        # TODO: return list of component names that need retry
        pass
    
    @property
    def is_ready_for_golden(self) -> bool:
        # TODO: check if this match can be included in golden dataset
        pass

@dataclass
class SeasonConsensus:
    """Consensus analysis for entire season across all runs"""
    tournament_id: int
    season_id: int
    run_ids: List[str]
    matches: Dict[int, MatchConsensus] = field(default_factory=dict)
    analysis_timestamp: datetime = field(default_factory=datetime.now)
    
    @property
    def total_matches(self) -> int:
        return len(self.matches)
    
    @property
    def matches_with_full_consensus(self) -> List[int]:
        # TODO: return match_ids that have consensus on all components
        pass
    
    @property
    def matches_needing_retry(self) -> List[int]:
        # TODO: return match_ids that need some components retried
        pass

@dataclass
class RetryItem:
    """Single component retry task"""
    match_id: int
    component_name: str
    reason: str
    attempts: int = 0

@dataclass
class RetryPlan:
    """Collection of retry tasks"""
    season_id: int
    tournament_id: int
    items: List[RetryItem] = field(default_factory=list)
    
    def add_retry(self, match_id: int, component_name: str, reason: str):
        # TODO: add a retry item to the plan
        pass
    
    def get_matches_to_retry(self) -> List[int]:
        # TODO: return unique list of match_ids that need retries
        pass

@dataclass
class GoldenMatch:
    """A single match in the golden dataset"""
    match_id: int
    source_runs: Dict[str, str]  # component_name -> run_id it came from
    data: Any  # The actual match data
    quality_score: float
    
@dataclass 
class GoldenDataset:
    """Complete golden dataset for a season"""
    tournament_id: int
    season_id: int
    matches: Dict[int, GoldenMatch] = field(default_factory=dict)
    creation_timestamp: datetime = field(default_factory=datetime.now)
    completeness_stats: Dict[str, Any] = field(default_factory=dict)



## Core Classes Structure
import hashlib
import json
from typing import Dict, Any, Optional, List
from omegaconf import DictConfig

class ComponentHasher:
    """Generates consistent hashes for match components"""
    
    def __init__(self, config: DictConfig):
        self.config = config
        self.hash_exclusions = config.quality.hash_exclusions
    
    def hash_match_components(self, match_data) -> Dict[str, str]:
        """
        Generate hash for each available component in match data
        
        Args:
            match_data: FootballMatchResultDetailed object
            
        Returns:
            Dict mapping component_name -> hash_string
        """
        # TODO: 
        # 1. Extract each component (base, stats, lineup, incidents, graph)
        # 2. Normalize each component (remove excluded fields)
        # 3. Generate consistent hash for each
        # 4. Return dict of component_name -> hash
        pass
    
    def _normalize_component(self, component_name: str, component_data: Any) -> Dict:
        """
        Remove fields that should be excluded from hashing
        
        Args:
            component_name: Name of component (base, stats, etc.)
            component_data: Raw component data
            
        Returns:
            Normalized dict ready for hashing
        """
        # TODO:
        # 1. Convert component data to dict
        # 2. Remove fields listed in hash_exclusions for this component
        # 3. Sort dict keys for consistency
        # 4. Handle nested objects and lists consistently
        pass
    
    def _generate_hash(self, normalized_data: Dict) -> str:
        """Generate consistent hash from normalized data"""
        # TODO:
        # 1. Convert dict to JSON string with sorted keys
        # 2. Generate SHA-256 hash
        # 3. Return hex digest
        pass

### quality_assessor.py
from typing import Dict
from omegaconf import DictConfig
from .data_models import MatchQuality, ComponentQuality, ComponentStatus

class ComponentQualityAssessor:
    """Assesses binary quality (valid/invalid) for each component"""
    
    def __init__(self, config: DictConfig):
        self.config = config
        self.component_rules = config.quality.component_rules
        self.active_components = config.quality.active_components
    
    def assess_match_quality(self, run_id: str, match_data) -> MatchQuality:
        """
        Assess quality of all components in a match
        
        Args:
            run_id: Identifier for this scraping run
            match_data: FootballMatchResultDetailed object
            
        Returns:
            MatchQuality with assessment of each component
        """
        # TODO:
        # 1. Create MatchQuality object
        # 2. For each active component, assess its quality
        # 3. Create ComponentQuality for each component
        # 4. Add to MatchQuality.components dict
        pass
    
    def _assess_base_component(self, match_data) -> ComponentQuality:
        """Assess base component quality"""
        # TODO:
        # 1. Check if base data exists
        # 2. Validate critical fields are present
        # 3. Check data makes sense (teams, scores, etc.)
        # 4. Return ComponentQuality with VALID/INVALID status
        pass
    
    def _assess_stats_component(self, match_data) -> ComponentQuality:
        """Assess stats component quality"""
        # TODO:
        # 1. Check if stats data exists
        # 2. Validate minimum number of stat groups
        # 3. Check for proper data structure
        # 4. Return ComponentQuality
        pass
    
    def _assess_lineup_component(self, match_data) -> ComponentQuality:
        """Assess lineup component quality""" 
        # TODO: Similar pattern for lineup validation
        pass
    
    def _assess_incidents_component(self, match_data) -> ComponentQuality:
        """Assess incidents component quality"""
        # TODO: Similar pattern for incidents validation
        pass
    
    def _assess_graph_component(self, match_data) -> ComponentQuality:
        """Assess graph component quality"""
        # TODO: Similar pattern for graph validation
        pass

### consensus_builder.py
from typing import Dict, List
from collections import Counter
from omegaconf import DictConfig
from .data_models import MatchQuality, SeasonConsensus, MatchConsensus, ComponentConsensus

class ConsensusBuilder:
    """Builds consensus across multiple scraping runs"""
    
    def __init__(self, config: DictConfig):
        self.config = config
        self.consensus_threshold = config.quality.consensus.threshold
        self.min_runs = config.quality.consensus.min_runs_required
    
    def build_season_consensus(self, run_qualities: Dict[str, List[MatchQuality]]) -> SeasonConsensus:
        """
        Build consensus for entire season across all runs
        
        Args:
            run_qualities: Dict mapping run_id -> List[MatchQuality]
            
        Returns:
            SeasonConsensus with analysis of all matches
        """
        # TODO:
        # 1. Create SeasonConsensus object
        # 2. Get all unique match_ids across runs
        # 3. For each match_id, build MatchConsensus
        # 4. Add to SeasonConsensus.matches
        pass
    
    def _build_match_consensus(self, match_id: int, 
                              match_qualities: List[MatchQuality]) -> MatchConsensus:
        """Build consensus for single match across runs"""
        # TODO:
        # 1. Create MatchConsensus object
        # 2. Get all components that appear in any run for this match
        # 3. For each component, build ComponentConsensus
        # 4. Add to MatchConsensus.components
        pass
    
    def _build_component_consensus(self, component_name: str, match_id: int,
                                  component_qualities: List[ComponentQuality]) -> ComponentConsensus:
        """Build consensus for single component across runs"""
        # TODO:
        # 1. Count hash occurrences across runs
        # 2. Check if any hash meets consensus threshold (100%)
        # 3. If yes, set consensus_hash and find consensus_data
        # 4. Track which runs agree/disagree
        # 5. Return ComponentConsensus
        pass

### retry_manager.py
from typing import List, Dict
from omegaconf import DictConfig
from webdriver import ManagerWebdriver
from .data_models import RetryPlan, RetryItem
from ..football.matchScraper import FootballMatchScraper

class SelectiveRetryManager:
    """Handles retrying specific components of specific matches"""
    
    def __init__(self, config: DictConfig, webdriver_manager: ManagerWebdriver):
        self.config = config
        self.webdriver_manager = webdriver_manager
        self.max_attempts = config.quality.retry.max_attempts
        self.delay = config.quality.retry.delay_seconds
    
    def execute_retry_plan(self, retry_plan: RetryPlan) -> Dict[str, Any]:
        """
        Execute all retries in the plan
        
        Args:
            retry_plan: Plan containing all retry items
            
        Returns:
            Dict with retry results and updated match data
        """
        # TODO:
        # 1. Group retry items by match_id for efficiency
        # 2. For each match, retry the needed components
        # 3. Track success/failure of each retry
        # 4. Return results for consensus re-analysis
        pass
    
    def _retry_match_components(self, match_id: int, 
                               component_names: List[str]) -> Dict[str, Any]:
        """Retry specific components for a single match"""
        # TODO:
        # 1. Create new webdriver
        # 2. Create FootballMatchScraper for this match
        # 3. Scrape only the specified components (modify scraper to be selective)
        # 4. Return component data
        pass
    
    def _create_selective_scraper(self, match_id: int, 
                                 components: List[str]) -> 'FootballMatchScraper':
        """Create a scraper that only scrapes specified components"""
        # TODO:
        # 1. Create FootballMatchScraper
        # 2. Configure it to only scrape specified components
        # 3. Return configured scraper
        pass

## storage/run_storage.py
import pickle
from pathlib import Path
from typing import Dict, List
from datetime import datetime
from omegaconf import DictConfig

class RunStorage:
    """Handles saving and loading of scraping runs and analysis results"""
    
    def __init__(self, config: DictConfig, tournament_id: int, season_id: int):
        self.config = config
        self.tournament_id = tournament_id
        self.season_id = season_id
        self.base_dir = self._setup_directories()
    
    def _setup_directories(self) -> Path:
        """Create directory structure for this tournament/season"""
        # TODO:
        # 1. Create base directory path
        # 2. Create subdirectories (runs, analysis, golden, logs)
        # 3. Return base path
        pass
    
    def save_scraping_run(self, run_data, run_name: str) -> str:
        """Save a scraping run with custom name"""
        # TODO:
        # 1. Generate filename with timestamp
        # 2. Save to runs subdirectory
        # 3. Return full file path
        pass
    
    def load_available_runs(self) -> Dict[str, Any]:
        """Load all available runs for this season"""
        # TODO:
        # 1. List all .pkl files in runs directory
        # 2. Load each one
        # 3. Return dict of run_name -> run_data
        pass
    
    def save_consensus_analysis(self, consensus_data, analysis_name: str) -> str:
        """Save consensus analysis results"""
        # TODO: Save to analysis subdirectory
        pass
    
    def save_golden_dataset(self, golden_data, dataset_name: str) -> str:
        """Save golden dataset"""
        # TODO: Save to golden subdirectory  
        pass
    
    def get_run_summary(self) -> Dict[str, Any]:
        """Get summary of available runs and their basic stats"""
        # TODO:
        # 1. List available runs
        # 2. Extract basic stats (match count, success rate, etc.)
        # 3. Return summary dict
        pass

## Main Orchestrator

from omegaconf import DictConfig
from webdriver import ManagerWebdriver
from .core.hash_generator import ComponentHasher
from .core.quality_assessor import ComponentQualityAssessor
from .core.consensus_builder import ConsensusBuilder
from .core.retry_manager import SelectiveRetryManager
from .storage.run_storage import RunStorage
from .reports.review_reports import ReviewReportGenerator

class SeasonQualityManager:
    """Main orchestrator for the quality assurance system"""
    
    def __init__(self, tournament_id: int, season_id: int, config: DictConfig):
        self.tournament_id = tournament_id
        self.season_id = season_id
        self.config = config
        
        # Initialize core components
        self.hasher = ComponentHasher(config)
        self.quality_assessor = ComponentQualityAssessor(config)
        self.consensus_builder = ConsensusBuilder(config)
        self.storage = RunStorage(config, tournament_id, season_id)
        
        # Lazy initialization (only when needed)
        self._retry_manager = None
        self._report_generator = None
    
    # === SCRAPING OPERATIONS ===
    def execute_scraping_run(self, run_name: str = None) -> str:
        """Execute a single scraping run and save results"""
        # TODO:
        # 1. Create SeasonFootballScraper
        # 2. Execute scraping
        # 3. Assess quality of all matches
        # 4. Save run data
        # 5. Return run identifier
        pass
    
    def load_available_runs(self) -> Dict[str, Any]:
        """Load all available runs for analysis"""
        return self.storage.load_available_runs()
    
    # === ANALYSIS OPERATIONS ===
    def build_consensus_analysis(self, run_ids: List[str] = None) -> 'SeasonConsensus':
        """Build consensus analysis across specified runs"""
        # TODO:
        # 1. Load specified runs (or all if none specified)
        # 2. Extract quality assessments for each run
        # 3. Use consensus_builder to analyze
        # 4. Save analysis results
        # 5. Return SeasonConsensus
        pass
    
    def generate_review_report(self, consensus: 'SeasonConsensus') -> 'ReviewReport':
        """Generate manual review report from consensus analysis"""
        # TODO:
        # 1. Create ReviewReportGenerator if needed
        # 2. Generate report from consensus
        # 3. Return report for manual inspection
        pass
    
    # === RETRY OPERATIONS ===  
    def execute_retry_plan(self, retry_plan: 'RetryPlan') -> Dict[str, Any]:
        """Execute selective retries based on plan"""
        # TODO:
        # 1. Create SelectiveRetryManager if needed
        # 2. Execute retry plan
        # 3. Save retry results
        # 4. Return results for re-analysis
        pass
    
    # === GOLDEN DATASET OPERATIONS ===
    def create_golden_dataset(self, consensus: 'SeasonConsensus') -> 'GoldenDataset':
        """Create final golden dataset from consensus"""
        # TODO:
        # 1. Use consensus to select best data for each match
        # 2. Validate completeness
        # 3. Save golden dataset
        # 4. Return GoldenDataset
        pass
    
    # === UTILITY METHODS ===
    def get_season_summary(self) -> Dict[str, Any]:
        """Get summary of current state of this season's data"""
        # TODO: Return overview of runs, consensus, golden dataset status
        pass
