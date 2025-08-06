import logging
from functools import partial
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

from omegaconf import DictConfig, OmegaConf

import sofascrape.schemas.general as sofaschema

logger = logging.getLogger(__name__)


def process_component_pipeline(
    match_data: sofaschema.FootballMatchResultDetailed,
    extract_fn: Callable,
    transform_fn: Callable,
    filter_fn: Callable,
    hasher_fn: Callable[[dict], str],
) -> str:
    """Generic pipeline that any component can use"""
    raw_data = extract_fn(match_data)
    return raw_data
    # dict_data = transform_fn(raw_data)
    # filtered_data = filter_fn(dict_data)
    # return hasher_fn(filtered_data)


def hash_data(filtered_data: dict[str, Union[int, float, str]]) -> str:
    """hash the filtered data"""
    # TODO
    hash: str = "Hasher_fn output"
    return hash


def extract_base_data(
    match_data: sofaschema.FootballMatchResultDetailed,
) -> sofaschema.FootballDetailsSchema:
    if (base_data := match_data.base) is not None:
        return base_data
    raise ValueError(f"Failed to get base details for match id {match_data.match_id}.")


# Create specialized functions using partial application
process_base_pipeline = partial(
    process_component_pipeline,
    extract_fn=extract_base_data,
    # transform_fn=transform_base_to_dict,
    # filter_fn=filter_base_fields,
    # hasher_fn=hash_data,
)


def component_required(component_name):
    """Decorator to only run function if component is active"""

    def decorator(func):
        def wrapper(self, data):
            if component_name in self.active_components:
                return func(self, data)
            else:
                logger.debug(f"Skipping {component_name} - not active")

        return wrapper

    return decorator


class HasherMain:
    """
    Generates consistent hashes for match components.
    Contains method to help process Football matches for hashing.
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

    @component_required("base")
    def process_base(self, data):
        logger.debug("base process set")

        # processing logic
        # 1 extract the base data from match data
        # 2 transform base data to dict / or similar
        # 3 remove fields not applicable
        # 4 hash and return
        return process_base_pipeline(
            match_data=data, transform_fn=None, filter_fn=None, hasher_fn=None
        )

    @component_required("stats")
    def process_stats(self, data):
        # processing logic
        logger.debug("stats process set")
        # processing logic
        # 1 extract the base data from match data
        # 2 transform base data to dict / or similar
        # 3 remove fields not applicable
        # 4 hash and return
        pass

    def process_all(self, data):
        self.process_base(data)
        self.process_stats(data)

    def hash_match(
        self, match_data: sofaschema.FootballMatchResultDetailed
    ) -> Dict[str, str]:
        """
        Generate hash for each available component in match data

        Args:
            match_data: FootballMatchResultDetailed object

        Returns:
            Dict mapping component_name -> hash_string
        """
        self.process_all("data")

        return {}
