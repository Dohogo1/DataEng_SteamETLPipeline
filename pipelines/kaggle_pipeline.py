import pandas as pd 
import os
from prefect import flow, task

# Betölti az adatokat egy JSON fájlból, és Pandas DataFrame-mé alakítja
@task(name="Load Data", retries = 3)
def load_data(file_path: str) -> pd.DataFrame:
    print(f"Loading data from {file_path}")
    df = pd.read_json(file_path, orient="index")
    return df

# A hiányzó értékeket NA-ra cseréli, az objektum típusú oszlopokban "Unknown"-nal tölti fel,
# különválasztja az első fejlesztőt és kiadót, a pontszám oszlopokban lévő 0 értékeket NA-ra cseréli,
# datetime formátumúvá alakítja a release_date oszlopot, és átnevezi a game_id oszlopot
@task(name="Clean Data")
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    print("Cleaning data")
    df_clean = df.copy()
    
    # Átírja a hiányzó adatokat kifejező értékeket NA-ra
    df_clean.replace(["", "[]", "null", "N/A", "None", "NaN"], pd.NA, inplace=True)
    
    # Az object típusú oszlopokban a hiányzó értékeket "Unknown"-ra cseréli
    object_cols = df_clean.select_dtypes(include=['object']).columns.tolist()
    df_clean[object_cols] = df_clean[object_cols].fillna("Unknown")
    
    # Az üres listákat "Unknown"-ra cseréli
    def fix_empty_lists(val):
        bad_values = ["", "[]", "null", "n/a", "none", "na", "nan"]
        
        if isinstance(val, list):
            cleaned_list = [item for item in val if str(item).strip().lower() not in bad_values]
            
            if len(cleaned_list) == 0:
                return ["Unknown"]
            
            return cleaned_list
            
        return val
    
    list_columns = ['genres', 'developers', 'publishers']
    for col in list_columns:
        df_clean[col] = df_clean[col].apply(fix_empty_lists)
    

    # Az első fejlesztőt és kiadót külön oszlopba helyezi
    df_clean["developer_name"] = df_clean["developers"].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x)
    df_clean["publisher_name"] = df_clean["publishers"].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x)
    
    # Átírja a 0 értékeket a score oszlopokban NA-ra, mert 0 érvénytelen érték
    if "metacritic_score" in df_clean.columns:
        df_clean["metacritic_score"] = df_clean["metacritic_score"].replace(0, pd.NA)
    if "user_score" in df_clean.columns:
        df_clean["user_score"] = df_clean["user_score"].replace(0, pd.NA)
    
    # Átalakítja a release_date oszlopot datetime formátumra
    df_clean['release_date'] = pd.to_datetime(df_clean['release_date'], errors='coerce')
    
    # Átnevezi az indexet game_id-re és oszlopként hozzáadja a DataFrame-hez
    df_clean.index.name = "game_id"
    df_clean.reset_index(inplace=True)   
    
    return df_clean
    
# Elkészíti a táblákat a megtisztított DataFrame-ből
@task(name="Build Model")
def build_model(df: pd.DataFrame) -> dict:
    print("Building tables for the model")
    
    # dim_games tábla
    dim_games = df[["game_id", "name", "required_age", "about_the_game"]]
    
    # dim_developers tábla
    dim_developers = df[["developer_name"]].drop_duplicates().dropna()
    dim_developers["developer_id"] = range(1, len(dim_developers) + 1)

    # dim_publishers tábla
    dim_publishers = df[["publisher_name"]].drop_duplicates().dropna()
    dim_publishers["publisher_id"] = range(1, len(dim_publishers) + 1)

    # dim_date tábla
    unique_dates = df[["release_date"]].dropna().drop_duplicates()
    dim_date = unique_dates.copy()
    dim_date["date_id"] = range(1, len(dim_date) + 1)
    dim_date["release_year"] = dim_date["release_date"].dt.year
    dim_date["release_month"] = dim_date["release_date"].dt.month
    dim_date["release_day"] = dim_date["release_date"].dt.day

    # dim_genres tábla
    all_genres = df["genres"].explode().unique()
    dim_genres = pd.DataFrame(all_genres, columns=["genre_name"]).dropna()
    dim_genres = dim_genres.sort_values("genre_name").reset_index(drop=True)
    dim_genres["genre_id"] = dim_genres.index + 1
    
    # bridge_game_genres tábla
    bridge_temp = df[["game_id", "genres"]].copy()
    bridge_temp = bridge_temp.explode("genres")
    bridge_temp = bridge_temp.rename(columns={"genres": "genre_name"})

    bridge_game_genres = bridge_temp.merge(dim_genres, on="genre_name", how="left")
    bridge_game_genres = bridge_game_genres[["game_id", "genre_id"]].dropna().astype(int)

    # fact_game_statistics tábla
    fact_temp = df.merge(dim_developers, left_on="developer_name", right_on="developer_name", how="left")
    fact_temp = fact_temp.merge(dim_publishers, left_on="publisher_name", right_on="publisher_name", how="left")
    fact_temp = fact_temp.merge(dim_date, left_on="release_date", right_on="release_date", how="left")

    fact_game_statistics = fact_temp[[
        "game_id",
        "developer_id",
        "publisher_id",
        "date_id",
        "price",
        "achievements",
        "average_playtime_forever",
        "discount", 
        "peak_ccu"
    ]].copy()

    fact_game_statistics["discount"] = fact_game_statistics["discount"] * 0.01

    return {
        "dim_games": dim_games,
        "dim_developers": dim_developers,
        "dim_publishers": dim_publishers,
        "dim_date": dim_date,
        "dim_genres": dim_genres,
        "bridge_game_genres": bridge_game_genres,
        "fact_game_statistics": fact_game_statistics
    }

# Elmenti a táblákat CSV fájlokba a megadott kimeneti könyvtárba
@task(name="Save Tables")
def save_tables(tables: dict, output_dir: str):
    print(f"Saving tables to {output_dir}")
    
    # Létrehozza a kimeneti könyvtárat, ha nem létezik
    os.makedirs(output_dir, exist_ok=True)
    
    for table_name, df in tables.items():
        output_path = f"{output_dir}/{table_name}.csv"
        df.to_csv(output_path, index=False)
        print(f"Saved {table_name} to {output_path}")

# A flow, ami futtatja a teljes pipeline-t
@flow(name="Kaggle Data Pipeline", log_prints=True)
def run_kaggle_pipeline():
    file_path = "data/raw/games.json"
    output_dir = "data/processed"
    
    df = load_data(file_path)
    df_clean = clean_data(df)
    tables = build_model(df_clean)
    save_tables(tables, output_dir)

# Futtatja a pipeline-t
if __name__ == "__main__":
    run_kaggle_pipeline()
