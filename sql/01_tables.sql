-- dim táblák létrehozása
CREATE TABLE IF NOT EXISTS dim_games (
    game_id INT PRIMARY KEY,
    name TEXT,
    required_age INT,
    about_the_game TEXT
);

CREATE TABLE IF NOT EXISTS dim_developers (
    developer_id SERIAL PRIMARY KEY,
    developer_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_publishers (
    publisher_id SERIAL PRIMARY KEY,
    publisher_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id SERIAL PRIMARY KEY,
    release_date DATE,
    release_year INT,
    release_month INT,
    release_day INT
);

CREATE TABLE IF NOT EXISTS dim_genres (
    genre_id SERIAL PRIMARY KEY,
    genre_name TEXT NOT NULL UNIQUE
);

-- központi fact tábla létrehozása
CREATE TABLE IF NOT EXISTS fact_game_statistics (
    game_id INT PRIMARY KEY REFERENCES dim_games(game_id),
    developer_id INT REFERENCES dim_developers(developer_id),
    publisher_id INT REFERENCES dim_publishers(publisher_id),
    date_id INT REFERENCES dim_date(date_id),
    price FLOAT,
    achievements INT,
    peak_ccu INT,
    average_playtime_forever INT,
    discount FLOAT
);

-- bridge tábla a műfajok és játékok között
CREATE TABLE IF NOT EXISTS bridge_game_genres (
    game_id INT NOT NULL REFERENCES dim_games(game_id),
    genre_id INT NOT NULL REFERENCES dim_genres(genre_id),
    PRIMARY KEY (game_id, genre_id)
);

-- fact tábla az API-ból szerzett achievement adatnak
CREATE TABLE IF NOT EXISTS fact_achievements (
    game_id INT REFERENCES dim_games(game_id),
    avg_ach_pct FLOAT,
    PRIMARY KEY (game_id)
);