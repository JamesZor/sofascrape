"""
name='Premiership 25/26' id=77128 year='25/26'
name='Premiership 24/25' id=62408 year='24/25'
name='Premiership 23/24' id=52588 year='23/24'
name='Premiership 22/23' id=41957 year='22/23'
name='Premiership 21/22' id=37029 year='21/22'
name='Premiership 20/21' id=28212 year='20/21'
name='Premiership 19/20' id=23987 year='19/20'
name='Premiership 18/19' id=17364 year='18/19'
name='Premiership 17/18' id=13448 year='17/18'
name='Premiership 16/17' id=11743 year='16/17'
name='Premiership 15/16' id=10379 year='15/16'
name='Premiership 14/15' id=8190 year='14/15'
name='Premier League 13/14' id=6309 year='13/14'
name='Premier League 12/13' id=4728 year='12/13'
name='Premier League 11/12' id=3392 year='11/12'
name='Premier League 10/11' id=2752 year='10/11'
name='Premier League 09/10' id=2140 year='09/10'
name='Premier League 08/09' id=1570 year='08/09'

####

name='Championship 25/26' id=77037 year='25/26'
name='Championship 24/25' id=62411 year='24/25'
name='Championship 23/24' id=52606 year='23/24'
name='Championship 22/23' id=41958 year='22/23'
name='Championship 21/22' id=37030 year='21/22'
name='Championship 20/21' id=29268 year='20/21'
name='Championship 19/20' id=23988 year='19/20'
name='Championship 18/19' id=17366 year='18/19'
name='Championship 17/18' id=13458 year='17/18'
name='Championship 16/17' id=11765 year='16/17'
name='Championship 15/16' id=10380 year='15/16'
name='Championship 14/15' id=8192 year='14/15'
name='First Division 13/14' id=6319 year='13/14'
name='First Division 12/13' id=4730 year='12/13'
name='First Division 11/12' id=3394 year='11/12'
name='First Division 10/11' id=2753 year='10/11'
name='First Division 09/10' id=2145 year='09/10'
name='First Division 08/09' id=1578 year='08/09'

"""

from omegaconf import OmegaConf
from webdriver import ManagerWebdriver

from sofascrape.general import SeasonsComponentScraper

sofa_tournament_ids = {
    "scot_pl": 54,
    "scot_ch": 55,
}

if __name__ == "__main__":

    mw = ManagerWebdriver()
    d1 = mw.spawn_webdriver()

    tour_scrape_pl = SeasonsComponentScraper(
        tournamentid=sofa_tournament_ids["scot_ch"], webdriver=d1
    )

    results = tour_scrape_pl.process()

    for season in results.seasons:
        print(season)
