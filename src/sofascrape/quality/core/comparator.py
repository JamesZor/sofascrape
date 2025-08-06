import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

from omegaconf import DictConfig, OmegaConf

import sofascrape.schemas.general as sofaschema

logger = logging.getLogger(__name__)


def component_required(component_name):
    """Decorator to only run function if component is active"""

    def decorator(func):
        def wrapper(self, match1, match2):
            if component_name in self.active_components:
                return func(self, match1, match2)
            else:
                logger.debug(f"Skipping {component_name} - not active")

        return wrapper

    return decorator


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
