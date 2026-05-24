from prefect import flow 
from pipelines.kaggle_pipeline import run_kaggle_pipeline
from pipelines.api_pipeline import run_api_pipeline
from pipelines.db_loader_pipeline import run_db_loader_pipeline
import time
import urllib.request

# Fő pipeline, ami futtatja a teljes adatfeldolgozást és betöltést
@flow(name="Steam main pipeline", log_prints=True)
def main_pipeline():
    print("Starting main pipeline")
    
    # Kaggle adatfeldolgozó pipeline futtatása
    run_kaggle_pipeline()
    
    # API lekérdező pipeline futtatása
    run_api_pipeline()
    
    # Adatbázis töltő pipeline futtatása
    run_db_loader_pipeline()
    
    print("Main pipeline completed")

# Futtatja a fő pipeline-t
if __name__ == "__main__":
    prefect_ready = False
    while not prefect_ready:
        try:
            # Megpróbál csatlakozni a Prefect belső szerveréhez
            urllib.request.urlopen("http://prefect-server:4200/", timeout=2)
            prefect_ready = True
        except Exception:
            # Ha még nem él a szerver, vár 3 másodpercet és újra megpróbál csatlakozni
            time.sleep(3)
    # Indításkor lefut
    main_pipeline()
    # Minden nap éjfélkor lefut újra
    main_pipeline.serve(
        name="steam-daily-batch",
        cron="0 0 * * *",
        tags=["production", "steam"]
    )