"""
Method to get an example of the tournament data.

Method to disply layout.
"""

import hydra
from omegaconf import DictConfig, OmegaConf
from webdriver import ManagerWebdriver


@hydra.main(
    config_path="../src/sofascrape/conf",
    config_name="notebook_general.yaml",
    version_base="1.3",
)
def test_cfg(cfg: DictConfig):
    print(OmegaConf.to_yaml(cfg))
    url: str = cfg.links.tournament_empty.format(tournamentID=1)
    print(url)


if __name__ == "__main__":
    main()


#    webmanger = ManagerWebdriver()
#    d1 = webmanger.spawn_webdriver()
#    data = d1.get_page("https://am.i.mullvad.net/json")
#    print(data)
