import pandas as pd
import requests
import time
import os
from prefect import flow, task

# A fact tábla betöltése egy Pandas DataFrame-be
@task(name="Load Fact Table", retries = 3)
def load_fact_table(file_path: str) -> pd.DataFrame:
    print(f"Loading fact table from {file_path}")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Fact table not found at {file_path}")
    return pd.read_csv(file_path)

# Lekéri az átlagos achievement teljesítési százalékot a maximális egyidejű játékosszám alapján vett top "limit" számú játékhoz,
# és létrehoz egy ténytáblát game_id és avg_ach_pct oszlopokkal
@task(name="Fetch Steam API Data", retries = 3)
def fetch_achievements(df: pd.DataFrame, limit: int = 500) -> pd.DataFrame:
    print("Fetching achievements data from Steam API")
    games_to_fetch = df.sort_values("peak_ccu", ascending=False).head(limit).copy()

    ach_data_list = []

    for app_id in games_to_fetch["game_id"]:
        url = f"https://api.steampowered.com/ISteamUserStats/GetGlobalAchievementPercentagesForApp/v0002/?gameid={app_id}"
        avg_completion_pct = 0.0
        
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                
                ach_data = data.get('achievementpercentages', {})
                
                if isinstance(ach_data, dict):
                    achievements = ach_data.get('achievements', [])
                    
                    if len(achievements) > 0:
                        total_percent = 0.0
                        for ach in achievements:
                            try:
                                total_percent += float(ach.get('percent', 0.0))
                            except (ValueError, TypeError):
                                pass
                            
                        avg_completion_pct = round(total_percent / len(achievements), 2)
            # ha túl sok lekérést küldtünk (429-es error), akkor 10 mp várakozás            
            elif response.status_code == 429:
                print("Too many requests, waiting 10 seconds...")
                time.sleep(10)
                continue
            else:
                print(f"Failed to fetch data for app_id {app_id}, status code: {response.status_code}")
        except Exception as e:
                print(f"Error fetching data for app_id {app_id}: {e}")
                avg_completion_pct = 0.0
                # A lekéréseknél gyakran a 403-mas hibát kapjuk, ami azt jelenti, hogy a Steam API megtagadja a válaszadást
            
        ach_data_list.append({
            "game_id": app_id,
            "avg_ach_pct": avg_completion_pct
        })
        time.sleep(0.5)
    
    print("Steam API data fetching completed") 
    return pd.DataFrame(ach_data_list)

# Elmenti a achievement adatokat egy CSV fájlba
@task(name="Save Achievements Data")
def save_achievements_data(df: pd.DataFrame, output_dir: str):
    print(f"Saving achievements data to {output_dir}")
    output_path = f"{output_dir}/fact_achievements.csv"
    fact_achievements = pd.DataFrame(df)
    fact_achievements.to_csv(output_path, index=False)

# A flow, ami futtatja a teljes pipeline-t
@flow(name="Steam API Pipeline", log_prints=True)
def run_api_pipeline():
    fact_path = "data/processed/fact_game_statistics.csv"
    output_dir = "data/processed"
    
    fact_df = load_fact_table(fact_path)
    ach_df = fetch_achievements(fact_df)
    save_achievements_data(ach_df, output_dir)

# Futtatja a pipeline-t
if __name__ == "__main__":
    run_api_pipeline()