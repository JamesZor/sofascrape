import logging
from typing import Any, Dict, List, Set, Tuple

from .dataclasses import (
    ComponentConsensusResult,
    MatchConsensusResult,
    SeasonConsensusResult,
)

logger = logging.getLogger(__name__)


class SeasonAssessor:
    """Handles analysis and planning for season consensus results"""

    def __init__(self) -> None:
        pass

    def get_total_matches(self, consensus: SeasonConsensusResult) -> int:
        """Get the total number of matches in Consensus"""
        return len(consensus.match_results) + len(consensus.matches_in_single_run_only)

    def debug_process_consensus(self, season_consensus: SeasonConsensusResult):

        logger.debug(self.get_total_matches(season_consensus))
