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
from pydantic import BaseModel, ValidationError
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


def get_and_save_data():
    cfg = create_global_cfg()
    tournamentid = 1
    tournament_page_data = get_tournament_json(cfg, tournamentid=tournamentid)
    save_page_data(data=tournament_page_data, file_name=f"tournament_{tournamentid}")


def load_page_data(file_name: str = "tournament_1"):
    file_path: Path = dir_example_general / f"{file_name}.json"
    with open(file_path, "r") as f:
        data = json.load(f)
    return data


# pydantic notes
class Sport(BaseModel):
    name: str
    id: int


class Category(BaseModel):
    name: str
    slug: str
    sport: Sport
    id: int


class Tournament(BaseModel):
    id: int
    name: str
    slug: str
    category: Category  # Keep it as 'category' to match JSON


class TournamentData(BaseModel):
    tournament: Tournament


def process_tournament_data(data):
    try:
        if isinstance(data, str):
            data = json.loads(data)

        tournament_data = TournamentData.model_validate(data)
        return tournament_data

    except (json.JSONDecodeError, ValidationError) as e:
        print(f"Processing failed: {e}")
        return None


def pydantic_example(data):
    result = process_tournament_data(data)
    if result:
        print(f"Tournament: {result.tournament.name}")
        print(f"Sport: {result.tournament.category.sport.name}")
        print(f"Country: {result.tournament.category.name}")
        print(result)


if __name__ == "__main__":
    data = load_page_data()
    json_data = json.dumps(data)
    print(f"{type(json_data)=}.")
    pydantic_example(json_data)
