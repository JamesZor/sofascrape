import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from omegaconf import DictConfig, OmegaConf

import sofascrape.schemas.general as sofaschema

logger = logging.getLogger(__name__)


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
        pass

    @component_required("stats")
    def process_stats(self, data):
        # processing logic
        logger.debug("stats process set")
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
