from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from omegaconf import DictConfig, OmegaConf


class HasherMain:
    """
    Generates consistent hashes for match components.
    Contains method to help process Football matches for hashing.
    """

    def __init__(self, config: Optional[DictConfig] = None) -> None:
        if config is None:
            self.config = self._get_config()
        else:
            self.config = config

    def _get_config(self) -> DictConfig:
        config_path = (Path(__file__).parent.parent) / "config" / "quality_config.yaml"
        return OmegaConf.load(config_path)
