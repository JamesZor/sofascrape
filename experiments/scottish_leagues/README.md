
# The Strategy: Use the "Retry" Mechanism to Backfill Odds Data

You won't be re-scraping; you'll be performing a targeted "repair" job on all your existing matches to add just the new odds component. Your SeasonQualityManager already has the logic to do this.

Here is a step-by-step breakdown:

## Step 1: Implement the Odds Component

First, complete the initial work we discussed:

    Create Pydantic Models: Develop the necessary schemas for the odds data in src/sofascrape/schemas/odds.py.

    Create the Scraper: Build the FootballOddsComponentScraper in src/sofascrape/football/oddsComponent.py.

    Update FootballMatchScraper: Add ODDS to your MatchComponentType enum in src/sofascrape/football/matchScraper.py.

    Update FootballMatchResultDetailed: Add odds: Optional[OddsSchema] = None to the schema in src/sofascrape/schemas/general.py.

## Step 2: Create a Targeted "Odds-Only" Retry Script

This is the key step. Instead of running a full scrape, you will create a new script that generates a "retry" dictionary. This dictionary will instruct the pipeline to scrape only the odds component for every match you have already collected.

Your SeasonQualityManager.execute_scraping_retry method is the tool for this job.

Here’s what your script will do:

    Load Existing Data: Use your FootballLoader to get a list of all tournament and season IDs that you have already processed.

    Get All Match IDs: For each season, load the "golden" dataset to get a complete list of every match_id you have stored.

    Build the "Odds-Only" Retry Dictionary: Create a dictionary where every match_id is a key, and the value is a list containing just one item: "odds".

It will look like this:
```Python

# This is a conceptual script you would create

from sofascrape.quality.manager import SeasonQualityManager
from sofascrape.loader import FootballLoader

# 1. Get all your existing match IDs from your golden data
loader = FootballLoader()
available_data = loader.discover_available_data()
all_match_ids = []
for tournament_id, season_ids in available_data.items():
    loaded_data = loader.load_tournament_seasons({tournament_id: season_ids})
    for season_data in loaded_data.values():
        for match_collection in season_data.values():
            all_match_ids.extend(match_collection.keys())

# 2. Build the retry dictionary
odds_retry_dict = {match_id: ["odds"] for match_id in all_match_ids}

# 3. Execute the retry scrape for a specific season
# (You would loop through all your seasons)
# For example, for a single season:
T_ID = 54
S_ID = 62408 
quality_manager = SeasonQualityManager(tournament_id=T_ID, season_id=S_ID)
quality_manager.execute_scraping_retry(odds_retry_dict)

print(f"Successfully scraped odds for {len(all_match_ids)} matches.")
```


## Step 3: Run the Final Pipeline Stages

After you have run your "odds-only" scrape for all your seasons, you will have new partial run files in your data directory. These files will contain only the odds data for each match.

Now, you can run your pipeline as usual, but you'll skip the initial scraping stages:

    Build Golden Dataset: Run your build-golden command. The Comparator will now find that for each match, it has:

        The base, lineup, and incidents data from your original runs.

        The odds data from your new, partial run.
        It will automatically merge these into a complete "golden" record for each match.

    Export to CSV: Run your export command. The FootballDataTransformer will now have access to the complete golden dataset (including odds) and will create your final CSVs with all the data combined.
