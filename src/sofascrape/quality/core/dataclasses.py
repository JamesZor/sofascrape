import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Set, Tuple

logger = logging.getLogger(__name__)
# ============================================================================
# DATACLASSES FOR RESULTS
# ============================================================================
FOOTBALL_COMPONENTS = ["base", "stats", "lineup", "incidents", "graph"]


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


@dataclass
class MatchGoldenClass:
    """handle the matching between runs and component (Football)"""

    match_id: int
    base: int  # This will be a suitable run to pick
    stats: int
    lineup: int
    incidents: int
    graph: int


@dataclass
class SeasonConsensusResult:
    """Complete consensus analysis for an entire season"""

    tournament_id: int
    season_id: int
    run_ids: List[str]
    match_results: Dict[int, MatchConsensusResult] = field(default_factory=dict)
    matches_in_single_run_only: List[int] = field(
        default_factory=list
    )  # Edge case matches

    @property
    def total_matches(self) -> int:
        """Total number of matches analyzed"""
        return len(self.match_results) + len(self.matches_in_single_run_only)

    @property
    def matches_with_consensus(self) -> List[int]:
        """Match IDs that have full consensus (all components have at least one agreeing pair)"""
        return [
            match_id
            for match_id, result in self.match_results.items()
            if result.has_consensus
        ]

    @property
    def matches_with_perfect_consensus(self) -> List[int]:
        """Match IDs where ALL runs agree on ALL components (no outliers)"""
        return [
            match_id
            for match_id, result in self.match_results.items()
            if result.has_consensus and not result.all_outlier_runs
        ]

    @property
    def matches_with_outliers(self) -> List[int]:
        """Match IDs that have consensus but some runs need retry"""
        return [
            match_id
            for match_id, result in self.match_results.items()
            if result.has_consensus and result.all_outlier_runs
        ]

    @property
    def matches_needing_retry(self) -> List[int]:
        """Match IDs that have NO consensus at all (complete failures)"""
        return [
            match_id
            for match_id, result in self.match_results.items()
            if not result.has_consensus
        ]

    @property
    def season_summary(self) -> Dict[str, Any]:
        """Overall season consensus statistics"""
        total_analyzable = len(self.match_results)
        perfect_consensus_count = len(self.matches_with_perfect_consensus)
        consensus_count = len(self.matches_with_consensus)
        outlier_count = len(self.matches_with_outliers)
        failed_count = len(self.matches_needing_retry)

        return {
            "total_matches": self.total_matches,
            "analyzable_matches": total_analyzable,
            "single_run_matches": len(self.matches_in_single_run_only),
            "perfect_consensus": perfect_consensus_count,
            "consensus_with_outliers": outlier_count,
            "consensus_total": consensus_count,
            "failed_matches": failed_count,
            "consensus_rate": (
                consensus_count / total_analyzable if total_analyzable > 0 else 0.0
            ),
            "perfect_rate": (
                perfect_consensus_count / total_analyzable
                if total_analyzable > 0
                else 0.0
            ),
        }

    @property
    def all_outlier_runs(self) -> Set[str]:
        """All run IDs that need retry for any match/component"""
        all_outliers = set()
        for result in self.match_results.values():
            all_outliers.update(result.all_outlier_runs)
        return all_outliers

    def get_retry_dict(self) -> Dict[int, List[str]]:
        """Return a dict of matchs by componets to retry"""
        retry_plan: Dict[int, List[str]] = {}
        for match_id, match_result in self.match_results.items():
            if match_result.has_consensus:
                continue
            retry_plan[match_id] = match_result.retry_components

        for match_id in self.matches_in_single_run_only:
            retry_plan[match_id] = FOOTBALL_COMPONENTS

        return retry_plan

    def get_golden_dataset_dict(self) -> Dict[int, Dict[str, int]]:
        golden_results_dict: Dict[int, Dict[str, int]] = {}
        for match_id, match_result in self.match_results.items():
            if not match_result.has_consensus:
                continue
            # Build component values dict
            component_values = {
                component_name: int(component_result.agreed_pairs[0][0])
                for component_name, component_result in match_result.component_results.items()
            }

            # Create object with all values at once
            golden_results_dict[match_id] = component_values

        return golden_results_dict

    def get_golden_dataset_plan(self) -> Dict[int, Dict[str, Set[str]]]:
        """Get plan for golden dataset - which runs to use for each component"""
        golden_plan: dict = {}

        for match_id, result in self.match_results.items():
            if not result.has_consensus:
                continue  # Skip matches with no consensus

            golden_plan[match_id] = {}

            for comp_name, comp_result in result.component_results.items():
                if comp_result.has_consensus:
                    # Use data from consensus runs
                    golden_plan[match_id][comp_name] = comp_result.consensus_runs
                # If no consensus, this component won't be in golden dataset

        return golden_plan

    def list_outlier_matches(self) -> Dict[int, Dict[str, Any]]:
        """List all matches with outliers and their details"""
        outlier_details = {}

        for match_id in self.matches_with_outliers:
            result = self.match_results[match_id]
            outlier_details[match_id] = {
                "outlier_runs": list(result.all_outlier_runs),
                "components_with_outliers": result.outlier_runs_by_component,
                "consensus_summary": result.consensus_summary,
            }

        return outlier_details

    def list_failed_matches(self) -> Dict[int, Dict[str, Any]]:
        """List all matches that completely failed consensus"""
        failed_details = {}

        for match_id in self.matches_needing_retry:
            result = self.match_results[match_id]
            failed_details[match_id] = {
                "runs": result.run_ids,
                "failed_components": result.retry_components,
                "consensus_summary": result.consensus_summary,
            }

        return failed_details

    def print_retry_summary(self):
        """Print a nice summary of what needs to be retried"""
        print("\n=== SEASON RETRY SUMMARY ===")
        print(f"Total matches: {self.total_matches}")
        print(
            f"Perfect consensus: {len(self.matches_with_perfect_consensus)} ({len(self.matches_with_perfect_consensus)/len(self.match_results)*100:.1f}%)"
        )
        print(
            f"Consensus with outliers: {len(self.matches_with_outliers)} ({len(self.matches_with_outliers)/len(self.match_results)*100:.1f}%)"
        )
        print(
            f"Complete failures: {len(self.matches_needing_retry)} ({len(self.matches_needing_retry)/len(self.match_results)*100:.1f}%)"
        )
        print("=" * 40)
