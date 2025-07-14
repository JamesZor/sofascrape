import json
from typing import Dict, List

from hydra import compose, initialize
from omegaconf import DictConfig, OmegaConf
from pydantic import BaseModel, ValidationError
from webdriver import ManagerWebdriver

from sofascrape.utils import NotebookSaveLoad


# ns = NotebookSaveLoad()
# data = ns.load(file_name="tournament_1")
# print(json.dumps(data, indent=4))
#
def create_global_cfg():
    with initialize(config_path="../src/sofascrape/conf", version_base="1.3"):
        cfg = compose(config_name="notebook_general.yaml")
    return cfg


def get_season_json(cfg: DictConfig, tournamentid: int = 1) -> Dict:
    webmanger = ManagerWebdriver()
    d1 = webmanger.spawn_webdriver()
    url: str = cfg.links.seasons_empty.format(tournamentID=tournamentid)
    page_data = d1.get_page(url)
    d1.close()
    return page_data


def process_get():
    ns = NotebookSaveLoad()
    cfg = create_global_cfg()
    print(OmegaConf.to_yaml(cfg))
    tournamentid = 1
    url: str = cfg.links.seasons_empty.format(tournamentID=tournamentid)
    print(f"{url=}")

    data = get_season_json(cfg, 1)
    print(data)
    ns.save("seasons_1", data, indent=2)


## test pydantic


class SeasonSchema(BaseModel):
    name: str
    id: int
    year: str


class SeasonsListSchema(BaseModel):
    seasons: List[SeasonSchema]


def process_season(seaosn: dict):
    try:
        season_data = SeasonSchema.model_validate(seaosn)
        return season_data
    except Exception as e:
        print(f"Processing failed: {str(e)}.")
        raise e


def process_season_list(season_list: List):
    try:
        season_data = SeasonsListSchema.model_validate(season_list)
        return season_data
    except Exception as e:
        print(f"Processing failed: {str(e)}.")
        raise e


def pydantic_example(seaon):
    result = process_season(seaon)
    if result:
        print(result)


def pydantic_example_1(data):
    result = process_season_list(data)
    if result:
        print(f"Found {len(result.seasons)} seasons")
        for season in result.seasons[:3]:  # Show first 3
            print(season)
            # print(f"- {season.name}: {season.year}")


# seasons
def seasons_main_func():
    ns = NotebookSaveLoad()
    data = ns.load(file_name="seasons_1")
    pydantic_example_1(data)


### For events
def get_events_in_season(cfg: DictConfig, tournamentid: int, seasonid: int) -> Dict:
    webmanger = ManagerWebdriver()
    d1 = webmanger.spawn_webdriver()
    url: str = cfg.links.events_season_empty.format(
        tournamentID=tournamentid, seasonID=seasonid
    )
    page_date = d1.get_page(url)

    d1.close()
    return page_date


def main_event_run():
    cfg: DictConfig = create_global_cfg()
    tournamentid: int = 1
    seasonid: int = 61627

    page_date = get_events_in_season(
        cfg=cfg, tournamentid=tournamentid, seasonid=seasonid
    )

    ns = NotebookSaveLoad()
    ns.save(file_name=f"events_season_{seasonid}", data=page_date, indent=4)


if __name__ == "__main__":
    ns = NotebookSaveLoad()
    data = ns.load(file_name="events_season_61627")
    print(json.dumps(data.get("events")[0], indent=6))
    #    process_get()
