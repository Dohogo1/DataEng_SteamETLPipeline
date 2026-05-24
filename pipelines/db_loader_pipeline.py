import pandas as pd 
from sqlalchemy import create_engine, text
import os
from prefect import flow, task

# Feltölti a táblát egy CSV fájlból a PostgreSQL adatbázisba
@task(name="Upload Table to PostgreSQL")
def upload_table(file_path: str, table_name: str, db_url: str):
    print(f"Uploading {table_name} to PostgreSQL")
    if not os.path.exists(file_path):
        print(f"File not found at {file_path}")
        return
    try:
        df = pd.read_csv(file_path)
        engine = create_engine(db_url)
        # Idempotensen üríti a táblát új adatok betöltése előtt
        with engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE;"))
            conn.commit()
        
        df.to_sql(table_name, con=engine, if_exists="append", index=False)
        engine.dispose()
        print(f"Successfully uploaded {table_name}")
        
    except Exception as e:
        print(f"Error loading {table_name}: {e}")

# A flow, ami futtatja a teljes pipeline-t
@flow(name="Database Loader Pipeline")
def run_db_loader_pipeline():
    db_url = "postgresql://steam:steam@postgres:5432/steam_dw"
    
    tables_to_load = {
        "dim_games": "data/processed/dim_games.csv",
        "dim_genres": "data/processed/dim_genres.csv",
        "dim_publishers": "data/processed/dim_publishers.csv",
        "dim_developers": "data/processed/dim_developers.csv",
        "dim_date": "data/processed/dim_date.csv",
        "bridge_game_genres": "data/processed/bridge_game_genres.csv",
        "fact_game_statistics": "data/processed/fact_game_statistics.csv",
        "fact_achievements": "data/processed/fact_achievements.csv"
    }
    
    for table_name, file_path in tables_to_load.items():
        upload_table(file_path, table_name, db_url)

# Futtatja a pipeline-t
if __name__ == "__main__":
    run_db_loader_pipeline()