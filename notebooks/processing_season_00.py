import logging

from sofascrape.football import SeasonFootballScraper
from sofascrape.utils import FootballLeague, ProcessingUtils

scot_details = FootballLeague.SCOTLAND
pu = ProcessingUtils(type=FootballLeague.SCOTLAND, web_on=False)


if __name__ == "__main__":
