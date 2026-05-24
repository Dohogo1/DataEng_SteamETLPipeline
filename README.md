# Steam Data Engineering Pipeline

Ez a projekt egy végponttól végpontig terjedő adatmérnöki folyamatot valósít meg, amely a Steam platform játékaival kapcsolatos adatokat használ, azokat tisztítja, strukturált adattárházba rendezi, majd vizualizálja.

## Futtatás

A futtatáshoz a repository tartalmán kívül a kiinduló adathalmaz (games.json) letöltésére van szükség a [kaggle oldaláról](https://www.kaggle.com/datasets/fronkongames/steam-games-dataset). A letöltött JSON fájlt a repository-n belül a data/raw mappába kell másolni.

A projektet Docker-ből lehet futtatni. Az első indításhoz a következő parancsot kell futtatni a repository gyökeréből:
```PS> docker compose up --build -d```

A konténer felállása után elindul a pipeline, ezt a [prefect webes oldalán](http://localhost:4200/) lehet valós időben követni, az API hívások miatt 5-10 percig is eltelhet a futás.

A pipeline lefutása után megtekinthető [Metabase Dashboardon](http://localhost:3000/) az ETL pipeline eredménye. A Metabase oldalán a ***viewer@steam.mb*** email cím, és ***A7g9^C4)s7*yz1***  jelszó beírásával lehet ezt elérni.

## Architektúra

A rendszer komponensei teljesen konténerizált, izolált Docker környezetben futnak.
Az architektúra felépítése:

```mermaid
graph TD
    subgraph Data_Sources [Adatforrások]
        JSON[Kaggle Dataset<br>games.json]
        API[Steam API<br>GetGlobalAchievementPercentagesForApp]
    end

    subgraph ETL_Pipeline [steam_etl_pipeline Konténer]
        
        subgraph Kaggle_Flow [Kaggle Data Pipeline Flow]
            T1[Load Data] --> T2[Clean Data]
            T2 --> T3[Build Model]
            T3 --> T4[Save Tables]
        end

        subgraph API_Flow [Steam API Pipeline Flow]
            T5[Load Fact Table] --> T6[Fetch Steam API Data]
            T6 --> T7[Save Achievements Data]
        end

        subgraph DB_Flow [Database Loader Pipeline Flow]
            T8[Upload Table to PostgreSQL<br>TRUNCATE CASCADE & Append]
        end
    end

    subgraph Local_Storage [Lokális Tárolás]
        CSV_Raw[data/processed/<br>dim_* & fact_game_statistics.csv]
        CSV_API[data/processed/<br>fact_achievements.csv]
    end

    subgraph Orchestration [Orchestrator]
        Prefect[Prefect Server<br>steam_prefect_server:4200]
    end

    subgraph Data_Warehouse [Adattárház & BI]
        Postgres[(PostgreSQL 16<br>steam_postgres:5432<br>steam_dw DB)]
        Metabase[Metabase Dashboard<br>steam_metabase:3000]
    end

    JSON -->|Beolvasás| T1
    T4 -->|Mentés| CSV_Raw
    
    CSV_Raw -->|Beolvasás legnépszerűbb peak_ccu alapján| T5
    API -->|REST API lekérés / limit 500| T6
    T7 -->|Mentés| CSV_API

    CSV_Raw & CSV_API -->|6. CSV betöltés| T8
    T8 -->|Idempotens betöltés| Postgres
    
    Postgres -->|SQL nézetek / Forrásadat| Metabase

    Prefect -.->|Vezérlés / Ütemezés<br>cron: 0 0 * * *| ETL_Pipeline

    style Data_Sources fill:#f9f9f9,stroke:#333,stroke-width:1px
    style Local_Storage fill:#fff3cd,stroke:#ffc107,stroke-width:1px
    style Orchestration fill:#e2f0d9,stroke:#385723,stroke-width:1px
    style Data_Warehouse fill:#d9e1f2,stroke:#1f4e78,stroke-width:1px
    style ETL_Pipeline fill:#f2f2f2,stroke:#7f7f7f,stroke-width:1px
```