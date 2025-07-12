"""
Method to get an example of the tournament data.

Method to disply layout.
"""

import json
from pathlib import Path
from typing import Any, Dict, List

import hydra
from hydra import compose, initialize
from omegaconf import DictConfig, OmegaConf
from webdriver import ManagerWebdriver

base_path: Path = Path(__file__).parent.parent
dir_example_general: Path = base_path / "example_json" / "general"


@hydra.main(
    config_path="../src/sofascrape/conf",
    config_name="notebook_general.yaml",
    version_base="1.3",
)
def test_cfg(cfg: DictConfig):
    print(OmegaConf.to_yaml(cfg))
    url: str = cfg.links.tournament_empty.format(tournamentID=1)
    print(url)


def create_global_cfg():
    with initialize(config_path="../src/sofascrape/conf", version_base="1.3"):
        cfg = compose(config_name="notebook_general.yaml")
    return cfg


def get_tournament_json(cfg: DictConfig, tournamentid: int = 1) -> Dict:
    webmanger = ManagerWebdriver()
    d1 = webmanger.spawn_webdriver()
    url: str = cfg.links.tournament_empty.format(tournamentID=tournamentid)
    page_data = d1.get_page(url)
    d1.close()
    return page_data


def save_page_data(
    data: dict,
    file_name: str = "tournament",
):
    file_path: Path = dir_example_general / f"{file_name}.json"
    with open(file_path, "w") as f:
        json.dump(obj=data, fp=f, indent=2)
    print(f"saved {file_name =} to {file_path =}.")


if __name__ == "__main__":
    cfg = create_global_cfg()
    tournamentid = 1
    tournament_page_data = get_tournament_json(cfg, tournamentid=tournamentid)
    save_page_data(data=tournament_page_data, file_name=f"tournament_{tournamentid}")
