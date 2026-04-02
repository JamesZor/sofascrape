import os

os.environ["DISPLAY"] = ":0"

from webdriver import ManagerWebdriver

from sofascrape.conf.config import load_config
from sofascrape.db.manager import DatabaseManager
from sofascrape.general.events import EventsComponentScraper

# 1. Load our new infrastructure
config = load_config()
db = DatabaseManager(config)

# 2. Spin up your custom proxy-rotating webdriver
print("Spawning Webdriver...")
mw = ManagerWebdriver()
driver = mw.spawn_webdriver()

try:
    # 3. Let's try to scrape the Scottish Premiership (Assuming ID is 36, change if needed!)
    # Actually, let's use Tournament ID 17 (Premier League) or whatever you know works.
    season_id = 77129
    target_id = 56

    season_id = 77128
    target_id = 54

    print(f"Instantiating Scraper for Tournament {target_id}...")
    scraper = EventsComponentScraper(
        tournamentid=target_id, seasonid=season_id, webdriver=driver, cfg=config
    )

    # 4. Execute the scrape steps
    scraper.get_data()
    print("Raw Data Fetched!")

    scraper.parse_data()
    print("Data Parsed into Pydantic successfully!")

    # Let's look at what we got:
    print(f"\n--- Result ---")
    print(scraper.data.events[1].model_dump_json(indent=6))  # Pretty print the JSON

    # 5. Let's test saving it to our brand new Postgres table!
    # db.upsert_tournament(scraper.data, scraper.raw_data)
    # print("Saved successfully!")

finally:
    # Always clean up the browser!
    print("Closing webdriver...")
    driver.close()


"""
Example out from scraper 

    season_id = 77128
    target_id = 54

{                                
      "slug": "rangers-motherwell",
      "id": 14035140,     
      "startTimestamp": 1754152200,
      "status": {                      
            "code": 100,                 
            "description": "Ended",
            "type": "finished"
      },                
      "time": {                      
            "injuryTime1": 1,        
            "injuryTime2": 4,    
            "currentPeriodStartTimestamp": 1754155935
      },                      
      "tournament": {      
            "id": 54,                  
            "name": "Scottish Premiership",
            "slug": "premiership",   
            "category": {
                  "name": "Scotland",
                  "id": 22,
                  "slug": "scotland",
                  "sport": {         
                        "name": "Football",
                        "slug": "football",
                        "id": 1
                  }        
            }                          
      },                                 
      "season": {                                                                              
            "name": "Premiership 25/26",                                                       
            "id": 77128,                                                                       
            "year": "25/26"                                                                    
      },                                                                                       
      "roundInfo": {                                                                           
            "round": 1                                                                         
      },                                                                                       
      "winnerCode": 3,                                                                         
      "homeScore": {                                                                           
            "current": 1,                                                                      
            "display": 1,                                                                      
            "period1": 0,                                                                      
            "period2": 1,                                                                      
            "normaltime": 1                                                                    
      },                                                                                       
      "awayScore": {                                                                           
            "current": 1,                                                                      
            "display": 1,                                                                      
            "period1": 1,                                                                                                                                     
            "period2": 0,                                                                      
            "normaltime": 1                                                                                                                                   
      },                                                                                                                                                      
      "homeTeam": {                                                                                                                                           
            "name": "Motherwell",                                                                                                                             
            "slug": "motherwell",                                                                                                                             
            "shortName": null,                                                                                                                                
            "nameCode": "MOT",                                                                                                                                
            "gender": "M",                                                                                                                                    
            "sport": {                                                                                                                                        
                  "name": "Football",                                                                                                                         
                  "slug": "football",                                                                                                                         
                  "id": 1                                                                                                                                     
            },                                                                                                                                                
            "country": {                                                                                                                                      
                  "name": "Scotland",                                                                                                                         
                  "slug": "scotland",                                                                                                                         
                  "alpha2": "SX",                                                                                                                             
                  "alpha3": "SCO"                                                                                                                             
            },                                                                                                                                                
            "teamColors": {                                                                                                                                   
                  "primary": "#ffcc00",                                                                                                                       
                  "secondary": "#97004b",                                                                                                                     
                  "text": "#97004b"                                                                                                                           
            }                                                                                                                                                 
      },                                                                                                                                                      
      "awayTeam": {                                                                                                                                           
            "name": "Rangers",                                                                                                                                
            "slug": "rangers",                                                                                                                                
            "shortName": "Rangers",                                                                                                                           
            "nameCode": "RAN",                                                                                                                                
            "gender": "M",                                                                                                                                    
            "sport": {                                                                                                                                        
                  "name": "Football",                                                                                                                         
                  "slug": "football",                                                                                                                         
                  "id": 1                                                                                                                                     
            },                                                                                                                                                
            "country": {                                                                                                                                      
                  "name": "Scotland",                                                                                                                         
                  "slug": "scotland",                                                                                                                         
                  "alpha2": "SX",                                                                                                                             
                  "alpha3": "SCO"                                                                                                                             
            },                                                                                                                                                
            "teamColors": {                                                                                                                                   
                  "primary": "#0033a0",                                                                                                                       
                  "secondary": "#ff0000",                                                                                                                     
                  "text": "#ff0000"                                                                                                                           
            }                                                                                                                                                 
      },                                                                                                                                                      
      "hasGlobalHighlights": true,                                                                                                                            
      "hasXg": true,                                                                                                                                          
      "hasEventPlayerStatistics": true,                                                                                                                       
      "hasEventPlayerHeatMap": true                                                                                                                           
}    

------
    season_id = 77129
    target_id = 56

{
      "slug": "montrose-hamilton-academical",
      "id": 14035486,
      "startTimestamp": 1754143200,
      "status": {
            "code": 100,
            "description": "Ended",
            "type": "finished"
      },
      "time": {
            "injuryTime1": 0,
            "injuryTime2": 0,
            "currentPeriodStartTimestamp": 1754146838
      },
      "tournament": {
            "id": 56,
            "name": "League One",
            "slug": "league-one",
            "category": {
                  "name": "Scotland",
                  "id": 22,
                  "slug": "scotland",
                  "sport": {
                        "name": "Football",
                        "slug": "football",
                        "id": 1
                  }
            }
      },
      "season": {
            "name": "League 1 25/26",
            "id": 77129,
            "year": "25/26"
      },
      "roundInfo": {
            "round": 1
      },
      "winnerCode": 1,
      "homeScore": {
            "current": 2,
            "display": 2,
            "period1": 1,
            "period2": 1,
            "normaltime": 2
      },
      "awayScore": {
            "current": 0,
            "display": 0,
            "period1": 0,
            "period2": 0,
            "normaltime": 0
      },
      "homeTeam": {
            "name": "Hamilton Academical",
            "slug": "hamilton-academical",
            "shortName": "Hamilton",
            "nameCode": "HAM",
            "gender": "M",
            "sport": {
                  "name": "Football",
                  "slug": "football",
                  "id": 1
            },
            "country": {
                  "name": "Scotland",
                  "slug": "scotland",
                  "alpha2": "SX",
                  "alpha3": "SCO"
            },
            "teamColors": {
                  "primary": "#ffffff",
                  "secondary": "#a80054",
                  "text": "#a80054"
            }
      },
      "awayTeam": {
            "name": "Montrose",
            "slug": "montrose",
            "shortName": null,
            "nameCode": "MON",
            "gender": "M",
            "sport": {
                  "name": "Football",
                  "slug": "football",
                  "id": 1
            },
            "country": {
                  "name": "Scotland",
                  "slug": "scotland",
                  "alpha2": "SX",
                  "alpha3": "SCO"
            },
            "teamColors": {
                  "primary": "#0040e9",
                  "secondary": "#001b7a",
                  "text": "#001b7a"
            }
      },
      "hasGlobalHighlights": false,
      "hasXg": false,
      "hasEventPlayerStatistics": false,
      "hasEventPlayerHeatMap": false
}

*****
        
{
      "slug": "kelty-heartsc-peterhead",
      "id": 14035661,
      "startTimestamp": 1777730400,
      "status": {
            "code": 0,
            "description": "Not started",
            "type": "notstarted"
      },
      "time": null,
      "tournament": {
            "id": 56,
            "name": "League One",
            "slug": "league-one",
            "category": {
                  "name": "Scotland",
                  "id": 22,
                  "slug": "scotland",
                  "sport": {
                        "name": "Football",
                        "slug": "football",
                        "id": 1
                  }
            }
      },
      "season": {
            "name": "League 1 25/26",
            "id": 77129,
            "year": "25/26"
      },
      "roundInfo": {
            "round": 36
      },
      "winnerCode": null,
      "homeScore": null,
      "awayScore": null,
      "homeTeam": {
            "name": "Peterhead",
            "slug": "peterhead",
            "shortName": null,
            "nameCode": "PET",
            "gender": "M",
            "sport": {
                  "name": "Football",
                  "slug": "football",
                  "id": 1
            },
            "country": {
                  "name": "Scotland",
                  "slug": "scotland",
                  "alpha2": "SX",
                  "alpha3": "SCO"
            },
            "teamColors": {
                  "primary": "#0000ff",
                  "secondary": "#ffffff",
                  "text": "#ffffff"
            }
      },
      "awayTeam": {
            "name": "Kelty Hearts F.C.",
            "slug": "kelty-hearts-fc",
            "shortName": "Kelty Hearts",
            "nameCode": "KEH",
            "gender": "M",
            "sport": {
                  "name": "Football",
                  "slug": "football",
                  "id": 1
            },
            "country": {
                  "name": "Scotland",
                  "slug": "scotland",
                  "alpha2": "SX",
                  "alpha3": "SCO"
            },
            "teamColors": {
                  "primary": "#374df5",
                  "secondary": "#374df5",
                  "text": "#ffffff"
            }
      },
      "hasGlobalHighlights": false,
      "hasXg": false,
      "hasEventPlayerStatistics": false,
      "hasEventPlayerHeatMap": false
}

# ----
            raw data 
_____




{'events': [{'eventState': {},                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
   'tournament': {'name': 'League One',                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
    'slug': 'league-one',                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
    'category': {'name': 'Scotland',                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
     'slug': 'scotland',                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
     'sport': {'name': 'Football', 'slug': 'football', 'id': 1},                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
     'priority': 0,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
     'country': {'alpha2': 'SX',                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
      'alpha3': 'SCO',                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
      'name': 'Scotland',                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
      'slug': 'scotland'},                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
     'id': 22,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
     'flag': 'scotland',                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
     'alpha2': 'SX',                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
     'fieldTranslations': {'nameTranslation': {'ar': 'اسكتلندا',                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
       'ru': 'Шотландия'},                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
      'shortNameTranslation': {}}},                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
    'uniqueTournament': {'name': 'Scottish League One',                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
     'slug': 'league-one',                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
     'primaryColorHex': '#212759',                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
     'secondaryColorHex': '#d5a914',                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
     'category': {'name': 'Scotland',                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
      'slug': 'scotland',
      'sport': {'name': 'Football', 'slug': 'football', 'id': 1},
      'priority': 0,
      'country': {'alpha2': 'SX',
       'alpha3': 'SCO',
       'name': 'Scotland',
       'slug': 'scotland'},
      'id': 22,
      'flag': 'scotland',
      'alpha2': 'SX',
      'fieldTranslations': {'nameTranslation': {'ar': 'اسكتلندا',
        'ru': 'Шотландия'},
       'shortNameTranslation': {}}},
     'userCount': 1753,
     'hasPerformanceGraphFeature': False,
     'country': {},
     'id': 207,
     'hasEventPlayerStatistics': False,
     'displayInverseHomeAwayTeams': False,
     'fieldTranslations': {'nameTranslation': {'ar': 'الدوري الاسكتلندي الأول'},
      'shortNameTranslation': {}}},
    'priority': 0,
    'isGroup': False,
    'isLive': False,
    'id': 56,
    'fieldTranslations': {'nameTranslation': {'ar': 'دوري الدرجة الأولى'},
     'shortNameTranslation': {}}},
   'season': {'name': 'League 1 25/26',
    'year': '25/26',
    'editor': False,
    'seasonCoverageInfo': {'editorCoverageLevel': 5},
    'id': 77129},
   'roundInfo': {'round': 1},
   'customId': 'tXsmld',
   'status': {'code': 100, 'description': 'Ended', 'type': 'finished'},
   'winnerCode': 2,
   'homeTeam': {'name': 'Cove Rangers',
    'slug': 'cove-rangers',
    'gender': 'M',
    'sport': {'name': 'Football', 'slug': 'football', 'id': 1},
    'userCount': 3554,
    'nameCode': 'COV',
    'national': False,
    'type': 0,
    'country': {'alpha2': 'SX',
     'alpha3': 'SCO',
     'name': 'Scotland',
     'slug': 'scotland'},
    'id': 8062,
    'subTeams': [],
    'teamColors': {'primary': '#0000ff',
     'secondary': '#ffffff',
     'text': '#ffffff'},
    'fieldTranslations': {'nameTranslation': {'ar': 'كوف رينجرز',
      'ru': 'Коув Рейнджерс ФК'},
     'shortNameTranslation': {}}},
   'awayTeam': {'name': 'Queen of The South',
    'slug': 'queen-of-the-south',
    'shortName': 'QoS',
    'gender': 'M',
    'sport': {'name': 'Football', 'slug': 'football', 'id': 1},
    'userCount': 2482,
    'nameCode': 'QUE',
    'national': False,
    'type': 0,
    'country': {'alpha2': 'SX',
     'alpha3': 'SCO',
     'name': 'Scotland',
     'slug': 'scotland'},
    'id': 2368,
    'subTeams': [],
    'teamColors': {'primary': '#024a9f',
     'secondary': '#00439c',
     'text': '#00439c'},
    'fieldTranslations': {'nameTranslation': {'ar': 'ملكة الجنوب',
      'ru': 'Квин оф Сауф'},
     'shortNameTranslation': {'ar': 'كيو أو إس'}}},
   'homeScore': {'current': 0,
    'display': 0,
    'period1': 0,
    'period2': 0,
    'normaltime': 0},
   'awayScore': {'current': 2,
    'display': 2,
    'period1': 1,
    'period2': 1,
    'normaltime': 2},
   'time': {'currentPeriodStartTimestamp': 1754150218},
   'changes': {'changes': ['status.code',
     'status.description',
     'status.type',
     'homeScore.period2',
     'homeScore.normaltime',
     'awayScore.period2',
     'awayScore.normaltime',
     'time.currentPeriodStart'],
    'changeTimestamp': 1754150221},
   'hasGlobalHighlights': False,
   'crowdsourcingDataDisplayEnabled': False,
   'id': 14035482,
   'slug': 'cove-rangers-queen-of-the-south',
   'startTimestamp': 1754143200,
   'finalResultOnly': False,
   'feedLocked': True,
   'isEditor': False},
  {'eventState': {},
   'tournament': {'name': 'League One',
    'slug': 'league-one',
    'category': {'name': 'Scotland',
     'slug': 'scotland',
     'sport': {'name': 'Football', 'slug': 'football', 'id': 1},
     'priority': 0,
     'country': {'alpha2': 'SX',
      'alpha3': 'SCO',
      'name': 'Scotland',
      'slug': 'scotland'},
     'id': 22,
     'flag': 'scotland',
     'alpha2': 'SX',
"""
