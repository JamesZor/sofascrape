import os

os.environ["DISPLAY"] = ":0"

from webdriver import ManagerWebdriver

from sofascrape.conf.config import load_config
from sofascrape.db.manager import DatabaseManager
from sofascrape.football.eventComponent import EventFootallComponentScraper

# 1. Load our new infrastructure
config = load_config()
db = DatabaseManager(config)

# 2. Spin up your custom proxy-rotating webdriver
print("Spawning Webdriver...")
mw = ManagerWebdriver()
driver = mw.spawn_webdriver()

# 3. Let's try to scrape the Scottish Premiership (Assuming ID is 36, change if needed!)
# Actually, let's use Tournament ID 17 (Premier League) or whatever you know works.
match_id = 14035506

scraper = EventFootallComponentScraper(matchid=match_id, webdriver=driver, cfg=config)

# 4. Execute the scrape steps
scraper.get_data()
print("Raw Data Fetched!")

scraper.parse_data()
print("Data Parsed into Pydantic successfully!")

# Let's look at what we got:
print(f"\n--- Result ---")
print(scraper.data.model_dump_json(indent=6))  # Pretty print the JSON

# 5. Let's test saving it to our brand new Postgres table!

db.upsert_events(
    scraper.tournamentid,
    scraper.data.events,
    scraper.raw_data.get("events", []),
)

# Always clean up the browser!
print("Closing webdriver...")
driver.close()


"""
{                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
      "event": {
            "slug": "cove-rangers-stenhousemuir",
            "id": 14035506,
            "startTimestamp": 1754748000,
            "status": {
                  "code": 100,
                  "description": "Ended",                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
                  "type": "finished"                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
            },                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
            "time": {                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
                  "injuryTime1": 0,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
                  "injuryTime2": 0,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
                  "currentPeriodStartTimestamp": 1754751900                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
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
                  },                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
                  "competitionType": 1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
            },                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
            "season": {                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
                  "name": "League 1 25/26",                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
                  "id": 77129,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
                  "year": "25/26"                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
            },                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
            "roundInfo": {                                                                                                                                    
                  "round": 2                                                                                                                                  
            },                                                                                                                                                
            "winnerCode": 1,                                                                                                                                  
            "homeScore": {                                                                                                                                    
                  "current": 1,                                                                                                                               
                  "display": 1,                                                                                                                               
                  "period1": 1,                                                                                                                               
                  "period2": 0,                                                                                                                               
                  "normaltime": 1                                                                                                                             
            },                                                                                                                                                
            "awayScore": {                                                                                                                                    
                  "current": 0,                                                                                                                               
                  "display": 0,                                                                                                                               
                  "period1": 0,                                                                                                                               
                  "period2": 0,                                                                                                                               
                  "normaltime": 0                                                                                                                             
            },                                                                                                                                                
            "homeTeam": {                                                                                                                                     
                  "name": "Stenhousemuir",                                                                                                                    
                  "slug": "stenhousemuir",                                                                                                                    
                  "shortName": null,                                                                                                                          
                  "nameCode": "STE",                                                                                                                          
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
                        "primary": "#750d32",                                                                                                                 
                        "secondary": "#ffffff",                                                                                                               
                        "text": "#ffffff"                                                                                                                     
                  },                                                                                                                                          
                  "fullName": "Stenhousemuir",                                                                                                                
                  "manager": {                                                                                                                                
                        "name": "Gary Naysmith",                                                                                                              
                        "slug": "gary-naysmith",                                                                                                              
                        "shortName": "G. Naysmith",                                                                                                           
                        "id": 787732,                                                                                                                         
                        "country": {                                                                                                                          
                              "name": "Scotland",                                                                                                             
                              "slug": "scotland",                                                                                                             
                              "alpha2": "SX",                                                                                                                 
                              "alpha3": "SCO"                                                                                                                 
                        }                                                                                                                                     
                  },                                                                                                                                          
                  "venue": {                                                                                                                                  
                        "name": "Ochilview Park",                                                                                                             
                        "slug": "ochilview-park",                                                                                                             
                        "capacity": 3746,                                                                                                                     
                        "id": 3385,                                                                                                                           
                        "city": {                                                                                                                             
                              "name": "Stenhousemuir"                                                                                                         
                        },                                                                                                                                    
                        "venueCoordinates": null,                                                                                                             
                        "country": {                                                                                                                          
                              "name": "Scotland",                                                                                                             
                              "slug": "scotland",                                                                                                             
                              "alpha2": "SX",                                                                                                                 
                              "alpha3": "SCO"                                                                                                                 
                        },                                                                                                                                    
                        "stadium": {                                                                                                                          
                              "name": "Ochilview Park",                                                                                                       
                              "capacity": 3746
                        }
                  },
                  "class_": null
            },
            "awayTeam": {
                  "name": "Cove Rangers",
                  "slug": "cove-rangers",
                  "shortName": null,
                  "nameCode": "COV",
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
                  },
                  "fullName": "Cove Rangers",
                  "manager": {
                        "name": "Paul Hartley",
                        "slug": "paul-hartley",
                        "shortName": "P. Hartley",
                        "id": 9377,
                        "country": {
                              "name": "Scotland",
                              "slug": "scotland",
                              "alpha2": "SX",
                              "alpha3": "SCO"
                        }
                  },
                  "venue": {
                        "name": "Balmoral Stadium",
                        "slug": "balmoral-stadium",
                        "capacity": 3023,
                        "id": 43026,
                        "city": {
                              "name": "Aberdeen"
                        },
                        "venueCoordinates": null,
                        "country": {
                              "name": "Scotland",
                              "slug": "scotland",
                              "alpha2": "SX",
                              "alpha3": "SCO"
                        },
                        "stadium": {
                              "name": "Balmoral Stadium",
                              "capacity": 3023
                        }
                  },
                  "class_": null
            },
            "hasGlobalHighlights": false,
            "hasXg": false,
            "hasEventPlayerStatistics": false,
            "hasEventPlayerHeatMap": false,
            "attendance": null,
            "venue": null,
            "referee": null,
            "defaultPeriodCount": 2,
            "defaultPeriodLength": 45,
            "defaultOvertimeLength": 15,
            "currentPeriodStartTimestamp": 1754751900,
            "fanRatingEvent": null,
            "seasonStatisticsType": null,
            "showTotoPromo": true
      }
}

# --- raw data


{'event': {'eventState': {},                                                                                                                                                                                                                                                                                                
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
    'userCount': 1755
  'hasRounds': True,
    'hasPerformanceGraphFeature': False,
    'country': {},
    'id': 207,
    'hasEventPlayerStatistics': False,
    'displayInverseHomeAwayTeams': False,
    'fieldTranslations': {'nameTranslation': {'ar': 'الدوري الاسكتلندي الأول'},
     'shortNameTranslation': {}}},
   'priority': 0,
   'isGroup': False,
   'competitionType': 1,
   'isLive': False,
   'id': 56,
   'fieldTranslations': {'nameTranslation': {'ar': 'دوري الدرجة الأولى'},
    'shortNameTranslation': {}}},
  'season': {'name': 'League 1 25/26',
   'year': '25/26',
   'editor': False,
   'seasonCoverageInfo': {'editorCoverageLevel': 5},
   'id': 77129},
  'roundInfo': {'round': 2},
  'customId': 'vXsmld',
  'status': {'code': 100, 'description': 'Ended', 'type': 'finished'},
  'winnerCode': 1,
  'homeTeam': {'name': 'Stenhousemuir',
   'slug': 'stenhousemuir',
   'gender': 'M',
   'sport': {'name': 'Football', 'slug': 'football', 'id': 1},
   'userCount': 2194,
   'manager': {'name': 'Gary Naysmith',
    'slug': 'gary-naysmith',
    'shortName': 'G. Naysmith',
    'country': {'alpha2': 'SX',
     'alpha3': 'SCO',
     'name': 'Scotland',
     'slug': 'scotland'},
    'id': 787732,
    'fieldTranslations': {'nameTranslation': {'ar': 'غاري نيسميث'},
     'shortNameTranslation': {'ar': 'غ. نيسميث'}}},
   'venue': {'hidden': True,
    'slug': 'ochilview-park',
    'name': 'Ochilview Park',
    'capacity': 3746,
    'country': {'alpha2': 'SX',
     'alpha3': 'SCO',
     'name': 'Scotland',
     'slug': 'scotland'},
    'id': 3385,
    'city': {'name': 'Stenhousemuir'},
    'fieldTranslations': {'nameTranslation': {'ar': 'أوشيلفيو بارك',
      'hi': 'ओचिलव्यू पार्क',
      'bn': 'ওচিলভিউ পার্ক'},
     'shortNameTranslation': {}},
    'stadium': {'name': 'Ochilview Park', 'capacity': 3746}},
   'nameCode': 'STE',
   'national': False,
   'type': 0,
   'country': {'alpha2': 'SX',
    'alpha3': 'SCO',
    'name': 'Scotland',
    'slug': 'scotland'},
   'id': 2370,
   'fullName': 'Stenhousemuir',
   'subTeams': [],
   'teamColors': {'primary': '#750d32',
    'secondary': '#ffffff',
    'text': '#ffffff'},
   'foundationDateTimestamp': -2713910400,
   'fieldTranslations': {'nameTranslation': {'ar': 'ستنهوسموير',
     'ru': 'Стенхаузмер'},
    'shortNameTranslation': {}},
   'timeActive': []},
  'awayTeam': {'name': 'Cove Rangers',
   'slug': 'cove-rangers',
   'gender': 'M',
   'sport': {'name': 'Football', 'slug': 'football', 'id': 1},
   'userCount': 3560,
   'manager': {'name': 'Paul Hartley',
    'slug': 'paul-hartley',
    'shortName': 'P. Hartley',
    'country': {'alpha2': 'SX',
     'alpha3': 'SCO',
     'name': 'Scotland',
     'slug': 'scotland'},
    'id': 9377,
    'fieldTranslations': {'nameTranslation': {'ar': 'بول هارتلي'},
     'shortNameTranslation': {'ar': 'ب. هارتلي'}}},
   'venue': {'hidden': True,
    'slug': 'balmoral-stadium',
    'name': 'Balmoral Stadium',
    'capacity': 3023,
    'country': {'alpha2': 'SX',
     'alpha3': 'SCO',
     'name': 'Scotland',
     'slug': 'scotland'},
    'id': 43026,
    'city': {'name': 'Aberdeen'},
    'fieldTranslations': {'nameTranslation': {'ar': 'ملعب بالمورال',
      'hi': 'बालमोरल स्टेडियम',
      'bn': 'বালমোরাল স্টেডিয়াম'},
     'shortNameTranslation': {}},
    'stadium': {'name': 'Balmoral Stadium', 'capacity': 3023}},
   'nameCode': 'COV',
   'national': False,
   'type': 0,
   'country': {'alpha2': 'SX',
    'alpha3': 'SCO',
    'name': 'Scotland',
    'slug': 'scotland'},
   'id': 8062,
   'fullName': 'Cove Rangers',
   'subTeams': [],
   'teamColors': {'primary': '#0000ff',
    'secondary': '#ffffff',
    'text': '#ffffff'},
   'foundationDateTimestamp': -1514764800,
   'fieldTranslations': {'nameTranslation': {'ar': 'كوف رينجرز',
     'ru': 'Коув Рейнджерс ФК'},
    'shortNameTranslation': {}},
   'timeActive': []},
  'homeScore': {'current': 1,
   'display': 1,
   'period1': 1,
   'period2': 0,
   'normaltime': 1},
  'awayScore': {'current': 0,
   'display': 0,
   'period1': 0,
   'period2': 0,
   'normaltime': 0},
  'time': {'currentPeriodStartTimestamp': 1754751900,
   'period1StartTimestamp': 1754748120,
   'period2StartTimestamp': 1754751900},
  'changes': {'changes': ['status.code', 'status.description', 'status.type'],
   'changeTimestamp': 1754754809},
  'hasGlobalHighlights': False,
  'crowdsourcingDataDisplayEnabled': False,
  'id': 14035506,
  'defaultPeriodCount': 2,
  'defaultPeriodLength': 45,
  'defaultOvertimeLength': 15,
  'slug': 'cove-rangers-stenhousemuir',
  'currentPeriodStartTimestamp': 1754751900,
  'startTimestamp': 1754748000,
  'finalResultOnly': False,
  'feedLocked': True,
  'showTotoPromo': True,
  'isEditor': False}}

"""
