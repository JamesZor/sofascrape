import logging
from dataclasses import dataclass, field
from itertools import combinations
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from omegaconf import DictConfig, OmegaConf

import sofascrape.schemas.general as sofaschema

logger = logging.getLogger(__name__)

# ============================================================================
# DATACLASSES FOR RESULTS
# ============================================================================


@dataclass
class ComponentConsensusResult:
    """Results for a single component's consensus"""

    name: str
    has_consensus: bool
    agreed_pairs: List[Tuple[str, str]] = field(default_factory=list)
    disagreed_pairs: List[Tuple[str, str]] = field(default_factory=list)

    @property
    def consensus_runs(self) -> Set[str]:
        """Set of run IDs that have the consensus data"""
        if not self.has_consensus or not self.agreed_pairs:
            return set()
        # Find the runs that appear in agreeing pairs
        # The consensus runs are the ones that agree with each other
        run_counts: Dict = {}
        for run1, run2 in self.agreed_pairs:
            run_counts[run1] = run_counts.get(run1, 0) + 1
            run_counts[run2] = run_counts.get(run2, 0) + 1
        # Return runs that appear in at least one agreeing pair
        return set(run_counts.keys())

    @property
    def outlier_runs(self) -> Set[str]:
        """Set of run IDs that need to be retried for this component"""
        if not self.has_consensus:
            # If no consensus at all, all runs are problematic
            all_runs = set()
            for run1, run2 in self.agreed_pairs + self.disagreed_pairs:
                all_runs.add(run1)
                all_runs.add(run2)
            return all_runs

        # Find runs that only appear in disagreed pairs
        consensus_runs = self.consensus_runs
        outliers = set()

        for run1, run2 in self.disagreed_pairs:
            if run1 not in consensus_runs:
                outliers.add(run1)
            if run2 not in consensus_runs:
                outliers.add(run2)

        return outliers


@dataclass
class MatchConsensusResult:
    """Complete consensus analysis for a single match"""

    match_id: int
    run_ids: List[str]
    has_consensus: bool  # True if ALL components have at least one agreeing pair
    retry_components: List[str] = field(
        default_factory=list
    )  # Components with NO consensus
    component_results: Dict[str, ComponentConsensusResult] = field(default_factory=dict)

    @property
    def consensus_summary(self) -> Dict[str, Any]:
        """Summary statistics about this match's consensus"""
        return {
            "total_components": len(self.component_results),
            "components_with_consensus": sum(
                1 for cr in self.component_results.values() if cr.has_consensus
            ),
            "overall_agreement_rate": (
                sum(cr.agreement_rate for cr in self.component_results.values())
                / len(self.component_results)
                if self.component_results
                else 0.0
            ),
        }

    @property
    def outlier_runs_by_component(self) -> Dict[str, Set[str]]:
        """Get outlier runs that need retry for each component"""
        return {
            comp_name: result.outlier_runs
            for comp_name, result in self.component_results.items()
            if result.outlier_runs
        }

    @property
    def all_outlier_runs(self) -> Set[str]:
        """Get all run IDs that need retry for any component"""
        outliers = set()
        for result in self.component_results.values():
            outliers.update(result.outlier_runs)
        return outliers


# ============================================================================
# DATACLASSES FOR RESULTS
# ============================================================================


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
        logger.debug(f"Hasher init finished.")

    def _get_config(self) -> DictConfig:
        config_path = (Path(__file__).parent.parent) / "config" / "quality_config.yaml"
        return OmegaConf.load(config_path)

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
        from typing import Any, Dict

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
        return list(combinations(run_ids, PAIRS))

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

            logger.debug(f"Comparing runs {run1_id} vs {run2_id} for match")
            comparison_result = self.compare_all_components(match1, match2)
            pair_results[pair] = comparison_result

            logger.debug(f"Result: {comparison_result}")

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

            logger.debug(
                f"Component {component}: consensus={has_consensus}, "
                f"agreed={len(agreed_pairs)}, disagreed={len(disagreed_pairs)}"
            )

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
        logger.info(
            f"Building consensus for match {match_id} across {len(matches_from_runs)} runs"
        )

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

        # Step 1: Compare all pairs of runs
        pair_results = self.compare_all_pairs(matches_from_runs)

        # Step 2: Process results to determine component consensus
        component_consensus = self.process_pair_results(pair_results)

        # Step 3: Determine overall consensus and retry needs
        components_with_consensus = [
            comp_name
            for comp_name, result in component_consensus.items()
            if result.has_consensus
        ]

        retry_components = [
            comp_name
            for comp_name, result in component_consensus.items()
            if not result.has_consensus
        ]

        # Overall consensus = ALL active components have consensus
        has_overall_consensus = len(retry_components) == 0

        # Step 4: Create and return result
        result = MatchConsensusResult(
            match_id=match_id,
            run_ids=list(matches_from_runs.keys()),
            has_consensus=has_overall_consensus,
            retry_components=retry_components,
            component_results=component_consensus,
        )

        logger.info(
            f"Match {match_id} consensus: {has_overall_consensus}, "
            f"components needing retry: {retry_components}"
        )

        return result

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

    # def compare_single_component(
    #     self, component_name: str, comp1: Any, comp2: Any
    # ) -> bool:
    #     """
    #     Compare two components of the same type, excluding configured fields.
    #
    #     Args:
    #         component_name: Name of component (base, stats, lineup, etc.)
    #         comp1, comp2: Component data to compare
    #
    #     Returns:
    #         True if components are equal (ignoring excluded fields)
    #     """
    #     pass
