import json
import pickle
from enum import Enum
from pathlib import Path
from typing import Any

from hydra import compose, initialize
from omegaconf import DictConfig, OmegaConf
from webdriver import ManagerWebdriver


class NoteBookType(Enum):
    GENERAL = "general"
    FOOTBALL = "football"
    CRICKET = "cricket"


class NotebookUtils:
    def __init__(self, type: NoteBookType, web_on: bool = True) -> None:
        self.base_path: Path = Path(__file__).parent.parent.parent.parent
        self.dir: Path = self.base_path / "example_json" / type.value

        self.type = type

        self.cfg = self.create_global_cfg()

        if web_on:
            self.set_up_webdriver()

    def set_up_webdriver(self) -> None:
        try:
            self.webmanager = ManagerWebdriver()
            self.driver = self.webmanager.spawn_webdriver()
        except Exception as e:
            print(f"Failed to set up webdriver: {str(e)}.")

    def create_global_cfg(self):
        with initialize(config_path="../conf", version_base="1.3"):
            cfg = compose(config_name="notebook_general.yaml")
        return cfg

    def load(self, file_name: str) -> Any:
        file_path: Path = self.dir / f"{file_name}.json"
        with open(file_path, "r") as f:
            data = json.load(f)
        return data

    def save(self, file_name: str, data: Any, **kwags) -> None:
        file_path: Path = self.dir / f"{file_name}.json"
        with open(file_path, "w") as f:
            json.dump(obj=data, fp=f, **kwags)
        print(f"File saved to {file_path}")

    def save_pickle(self, file_name: str, data: Any, **kwags) -> None:
        file_path: Path = self.dir / f"{file_name}.pkl"
        with open(file_path, "wb") as f:
            pickle.dump(data, f)

    def load_pickle(self, file_name: str) -> Any:
        file_path: Path = self.dir / f"{file_name}.pkl"
        with open(file_path, "rb") as f:
            data = pickle.load(f)
        return data

    def __repr__(self):
        return f"{self.base_path =}, {self.dir =}"
