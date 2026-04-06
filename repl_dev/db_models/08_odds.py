import os

os.environ["DISPLAY"] = ":0"

from webdriver import ManagerWebdriver

from sofascrape.conf.config import load_config
from sofascrape.db.manager import DatabaseManager
from sofascrape.football.oddsComponent import FootballOddsComponentScraper

# 1. Load our new infrastructure
config = load_config()
db = DatabaseManager(config)

# 2. Spin up your custom proxy-rotating webdriver
print("Spawning Webdriver...")
mw = ManagerWebdriver()
driver = mw.spawn_webdriver()

# 3. Let's try to scrape the Scottish Premiership (Assuming ID is 36, change if needed!)
# Actually, let's use Tournament ID 17 (Premier League) or whatever you know works.
# match_id = 14035506
match_id = 14035136

scraper = FootballOddsComponentScraper(matchid=match_id, webdriver=driver, cfg=config)

# 4. Execute the scrape steps
scraper.get_data()
print("Raw Data Fetched!")

scraper.parse_data()

# Let's look at what we got:
print(f"\n--- Result ---")
print(scraper.data.model_dump_json(indent=2))  # Pretty print the JSON

# 5. Let's test saving it to our brand new Postgres table!
# TODO: create this functions
db.upsert_match_odds(
    match_id=match_id,
    parsed_odds_markets=scraper.data.markets,
    raw_odds=scraper.raw_data.get("markets", []),
)
# Always clean up the browser!
print("Closing webdriver...")
driver.close()


"""
--- Result ---                                                                                                                                                                                                                                                                                                              
{                                                                                                                                                                                                                                                                                                                           
  "markets": [                                                                                                                                                                                                                                                                                                              
    {                                                                                                                                                                                                                                                                                                                       
      "market_id": 1,                                                                                                                                                                                                                                                                                                       
      "market_name": "Full time",                                                                                                                                                                                                                                                                                           
      "market_group": "1X2",                                                                                                                                                                                                                                                                                                
      "is_live": false,                                                                                                                                                                                                                                                                                                     
      "suspended": false,                                                                                                                                                                                                                                                                                                   
      "choices": [                                                                                                                                                                                                                                                                                                          
        {                                                                                                                                                                                                                                                                                                                   
          "initial_fractional_value": [                                                                                                                                                                                                                                                                                     
            7,                                                                                                                                                                                                                                                                                                              
            2                                                                                                                                                                                                                                                                                                               
          ],                                                                                                                                                                                                                                                                                                                
          "fractional_value": [                                                                                                                                                                                                                                                                                             
            5,                                                                                                                                                                                                                                                                                                              
            2                                                                                                                                                                                                                                                                                                               
          ],                                                                                                                                                                                                                                                                                                                
          "source_id": 1330363686,                                                                                                                                                                                                                                                                                          
          "name": "1",                                                         
          "winning": false,                                                    
          "change": -1                                                         
        },                                                                     
        {                                                                      
          "initial_fractional_value": [                                        
            7,                                                                 
            2                                                                  
          ],                                                                   
          "fractional_value": [                                                
            14,                                                                
            5                                                                  
          ],                                                                   
          "source_id": 1330363691,                                             
          "name": "X",                                                         
          "winning": true,                                                     
          "change": -1                                                         
        },                                                                     
        {                                                                      
          "initial_fractional_value": [                                        
            31,                                                                
            50                                                                 
          ],                                                                   
          "fractional_value": [                                                
            19,                                                                
            20                                                                 
          ],                                                                   
          "source_id": 1330363693,                                             
          "name": "2",                                                         
          "winning": false,                                                    
          "change": 1                                                          
        }                                                                      
      ],                                                                       
      "choice_group": null                                                     
    },         


"""
