import logging
from itertools import combinations
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from omegaconf import DictConfig, OmegaConf

import sofascrape.schemas.general as sofaschema

from .dataclasses import (
    ComponentConsensusResult,
    MatchConsensusResult,
    SeasonConsensusResult,
)

logger = logging.getLogger(__name__)


class Comparator:
    """
    Class to handle the comparison of matches across multiple runs.
    Uses Pydantic model equality with configurable field exclusions.
    """

    def __init__(self, config: Optional[DictConfig] = None) -> None:
        logger.debug("Hasher init starting...")
        if config is None:
            logger.debug("No config given, getting default.")
            self.config = self._get_config()
        else:
            self.config = config

        self.active_components = set(self.config.quality.active_components)
        logger.debug("Hasher init finished.")

    def _get_config(self) -> DictConfig:
        config_path = (Path(__file__).parent.parent) / "config" / "quality_config.yaml"
        return OmegaConf.load(config_path)  # type: ignore

    # ========================================================================
    # CORE COMPARISON METHODS
    # ========================================================================

    def _get_exclusion_fields(self, component_name: str) -> set[str]:
        return set(getattr(self.config.quality.comparator_exclusions, component_name))

    def compare_component(
        self,
        match1: sofaschema.FootballMatchResultDetailed,
        match2: sofaschema.FootballMatchResultDetailed,
        component_name: str,
    ) -> bool:
        """Generic component comparison method."""
        from typing import Dict

        excluded_fields = self._get_exclusion_fields(component_name=component_name)

        # Get component from match1
        if (match1_component := getattr(match1, component_name)) is not None:
            match1_dict: Dict[str, Union[int, float, str]] = (
                match1_component.model_dump(exclude=excluded_fields)
            )
        else:
            logger.warning(
                f"Failed to get {component_name} component for match 1, {match1.match_id}."
            )
            return False

        # Get component from match2
        if (match2_component := getattr(match2, component_name)) is not None:
            match2_dict: Dict[str, Union[int, float, str]] = (
                match2_component.model_dump(exclude=excluded_fields)
            )
        else:
            logger.warning(
                f"Failed to get {component_name} component for match 2, {match2.match_id}."
            )
            return False
        return bool(match1_dict == match2_dict)

    def compare_all_components(
        self,
        match1: sofaschema.FootballMatchResultDetailed,
        match2: sofaschema.FootballMatchResultDetailed,
    ) -> Dict[str, bool]:
        """Compare all components set in config and return results as a dictionary."""
        results = {}
        for component in self.config.quality.active_components:
            results[component] = self.compare_component(match1, match2, component)

        return results

    # ========================================================================
    # CONSENSUS BUILDING METHODS
    # ========================================================================

    def _generate_run_pairs(self, run_ids: List[str]) -> List[Tuple[str, str]]:
        """build all pairs, rotations doesn't matter"""
        PAIRS = 2
        return list(combinations(run_ids, PAIRS))  # type: ignore

    def compare_all_pairs(
        self, matches_from_runs: Dict[str, Any]
    ) -> Dict[Tuple[str, str], Dict[str, bool]]:
        """
        Compare all pairs of runs for a single match.

        Args:
            matches_from_runs: Dict mapping run_id -> match_data

        Returns:
            Dict mapping (run_id1, run_id2) -> comparison_results

        Example return:
            {
                ('r1', 'r2'): {'base': True, 'stats': True, 'lineup': False, 'incidents': True, 'graph': True},
                ('r1', 'r3'): {'base': True, 'stats': True, 'lineup': False, 'incidents': True, 'graph': True},
                ('r2', 'r3'): {'base': True, 'stats': True, 'lineup': True, 'incidents': True, 'graph': True}
            }
        """
        run_ids = list(matches_from_runs.keys())
        pairs = self._generate_run_pairs(run_ids)

        pair_results = {}

        for pair in pairs:
            run1_id, run2_id = pair
            match1 = matches_from_runs[run1_id]
            match2 = matches_from_runs[run2_id]

            # logger.debug(f"Comparing runs {run1_id} vs {run2_id} for match")
            comparison_result = self.compare_all_components(match1, match2)
            pair_results[pair] = comparison_result

            # logger.debug(f"Result: {comparison_result}")

        return pair_results

    def process_pair_results(
        self, pair_results: Dict[Tuple[str, str], Dict[str, bool]]
    ) -> Dict[str, ComponentConsensusResult]:
        """
        Process pairwise comparison results to determine consensus per component.

        Args:
            pair_results: Results from compare_all_pairs()

        Returns:
            Dict mapping component_name -> ComponentConsensusResult
        """
        # Get all components that were compared
        all_components: Set = set()
        for comparison in pair_results.values():
            all_components.update(comparison.keys())

        component_consensus = {}

        for component in all_components:
            agreed_pairs = []
            disagreed_pairs = []

            # Check each pair for this component
            for pair, comparison in pair_results.items():
                if comparison.get(
                    component, False
                ):  # Component agreed between these runs
                    agreed_pairs.append(pair)
                else:  # Component disagreed between these runs
                    disagreed_pairs.append(pair)

            # Component has consensus if AT LEAST ONE pair agrees (good data exists)
            has_consensus = len(agreed_pairs) > 0

            component_consensus[component] = ComponentConsensusResult(
                name=component,
                has_consensus=has_consensus,
                agreed_pairs=agreed_pairs,
                disagreed_pairs=disagreed_pairs,
            )

            # logger.debug(
            #     f"Component {component}: consensus={has_consensus}, "
            #     f"agreed={len(agreed_pairs)}, disagreed={len(disagreed_pairs)}"
            # )

        return component_consensus

    def build_match_consensus(
        self,
        match_id: int,
        matches_from_runs: Dict[str, Any],  # run_id -> FootballMatchResultDetailed
    ) -> MatchConsensusResult:
        """
        Build consensus for a single match across multiple runs.

        Args:
            match_id: ID of the match being analyzed
            matches_from_runs: Dict mapping run_id -> match_data

        Returns:
            MatchConsensusResult with complete consensus analysis
        """
        # logger.info(
        #     f"Building consensus for match {match_id} across {len(matches_from_runs)} runs"
        # )

        if len(matches_from_runs) < 2:
            logger.warning(
                f"Match {match_id} has fewer than 2 runs, cannot build consensus"
            )
            return MatchConsensusResult(
                match_id=match_id,
                run_ids=list(matches_from_runs.keys()),
                has_consensus=False,
                retry_components=list(self.active_components),
            )

        pair_results = self.compare_all_pairs(matches_from_runs)

        component_consensus = self.process_pair_results(pair_results)

        retry_components = [
            comp_name
            for comp_name, result in component_consensus.items()
            if not result.has_consensus
        ]

        # Overall consensus = ALL active components have consensus
        has_overall_consensus = len(retry_components) == 0

        result = MatchConsensusResult(
            match_id=match_id,
            run_ids=list(matches_from_runs.keys()),
            has_consensus=has_overall_consensus,
            retry_components=retry_components,
            component_results=component_consensus,
        )

        # logger.info(
        #     f"Match {match_id} consensus: {has_overall_consensus}, "
        #     f"components needing retry: {retry_components}"
        # )

        return result

    def _collect_all_match_ids(self, run_data: Dict[str, List[Any]]) -> Set[int]:
        """Collect all unique match IDs across all runs"""
        all_match_ids = set()
        for run_id, matches in run_data.items():
            for match in matches:
                if hasattr(match, "match_id"):
                    all_match_ids.add(match.match_id)
        return all_match_ids

    def _collect_matches_for_id(
        self, match_id: int, run_data: Dict[str, List[Any]]
    ) -> Dict[str, Any]:
        """Collect all match data for a specific match_id across runs"""
        matches_for_id = {}

        for run_id, matches in run_data.items():
            for match in matches:
                if hasattr(match, "match_id") and match.match_id == match_id:
                    matches_for_id[run_id] = match
                    break  # Found the match in this run

        return matches_for_id

    def build_season_consensus(
        self,
        run_data: Dict[str, List[Any]],
        tournament_id: int,
        season_id: int,
    ) -> SeasonConsensusResult:
        """
        Build consensus for entire season across all runs.

        Args:
            run_data: Dict mapping run_id -> List[FootballMatchResultDetailed]
            tournament_id: Tournament ID for metadata
            season_id: Season ID for metadata

        Returns:
            SeasonConsensusResult with complete season analysis
        """
        logger.info(f"Building season consensus across {len(run_data)} runs")

        # Step 1: Get all unique match_ids across all runs
        all_match_ids = self._collect_all_match_ids(run_data)
        # logger.info(f"Found {len(all_match_ids)} unique matches across all runs")

        # Step 2: Process each match
        match_results = {}
        single_run_matches = []

        for match_id in all_match_ids:
            # Collect match data from all runs that have this match
            matches_for_id = self._collect_matches_for_id(match_id, run_data)

            if len(matches_for_id) < 2:
                # Edge case: match only appears in 1 run, can't build consensus
                single_run_matches.append(match_id)
                logger.debug(
                    f"Match {match_id} only appears in 1 run: {list(matches_for_id.keys())}"
                )
                continue

            # Build consensus for this match
            # logger.debug(
            #     f"Building consensus for match {match_id} across runs: {list(matches_for_id.keys())}"
            # )
            match_consensus = self.build_match_consensus(match_id, matches_for_id)
            match_results[match_id] = match_consensus

        # Step 3: Create season result
        season_result = SeasonConsensusResult(
            tournament_id=tournament_id or 0,
            season_id=season_id or 0,
            run_ids=list(run_data.keys()),
            match_results=match_results,
            matches_in_single_run_only=single_run_matches,
        )

        # Log summary
        summary = season_result.season_summary
        logger.info("Season consensus complete:")
        logger.info(f"  Total matches: {summary['total_matches']}")
        logger.info(f"  Analyzable matches: {summary['analyzable_matches']}")
        logger.info(f"  Single run matches: {summary['single_run_matches']}")
        # logger.info(f"  Matches with consensus: {summary['matches_with_consensus']}")
        logger.info(f"  Consensus rate: {summary['consensus_rate']:.1%}")

        return season_result

    # ========================================================================
    # Utils/ Debug METHODS
    # ========================================================================

    def find_dict_differences(
        self,
        match1_dict: Dict[str, Any],
        match2_dict: Dict[str, Any],
    ) -> Dict[str, Dict[str, Any]]:
        """
        Find all keys/values where two dictionaries don't match.

        Returns:
            Dict with structure: {
                "key_name": {
                    "match1": value_from_match1,
                    "match2": value_from_match2
                }
            }
        """
        differences = {}

        # Get all unique keys from both dictionaries
        all_keys = set(match1_dict.keys()) | set(match2_dict.keys())

        for key in all_keys:
            match1_value = match1_dict.get(key, "<MISSING>")
            match2_value = match2_dict.get(key, "<MISSING>")

            # If values are different, add to differences
            if match1_value != match2_value:
                differences[key] = {"match1": match1_value, "match2": match2_value}

        return differences
