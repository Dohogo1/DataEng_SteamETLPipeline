CREATE OR REPLACE VIEW dashboard_view AS
SELECT
    g.name AS game_name,
    d.developer_name,
    p.publisher_name,
    dd.release_date,
    dd.release_year,
    dd.release_month,
    dd.release_day,
    STRING_AGG(DISTINCT gen.genre_name, ', ') AS genres,
    f.price,
    f.achievements,
    f.peak_ccu,
    f.average_playtime_forever,
    f.discount,
    a.avg_ach_pct
FROM fact_game_statistics f
JOIN dim_games g ON f.game_id = g.game_id
JOIN dim_developers d ON f.developer_id = d.developer_id
JOIN dim_publishers p ON f.publisher_id = p.publisher_id
JOIN dim_date dd ON f.date_id = dd.date_id
LEFT JOIN bridge_game_genres bg ON g.game_id = bg.game_id
LEFT JOIN dim_genres gen ON bg.genre_id = gen.genre_id
LEFT JOIN fact_achievements a ON g.game_id = a.game_id
GROUP BY
    g.name,
    d.developer_name,
    p.publisher_name,
    dd.release_date,
    dd.release_year,
    dd.release_month,
    dd.release_day,
    f.price,
    f.achievements,
    f.peak_ccu,
    f.average_playtime_forever,
    f.discount,
    a.avg_ach_pct;

CREATE OR REPLACE VIEW genre_view AS
SELECT
    gen.genre_name,
    COUNT(DISTINCT g.game_id) as game_count,
    AVG(f.price) as avg_price,
    SUM(f.peak_ccu) as total_peak_ccu,
    AVG(a.avg_ach_pct) as avg_achievement_pct
FROM dim_genres gen
JOIN bridge_game_genres bg ON gen.genre_id = bg.genre_id
JOIN dim_games g ON bg.game_id = g.game_id
JOIN fact_game_statistics f ON g.game_id = f.game_id
LEFT JOIN fact_achievements a ON g.game_id = a.game_id
GROUP BY gen.genre_name
ORDER BY total_peak_ccu DESC;

CREATE OR REPLACE VIEW popularity_view AS
SELECT
    g.name AS game_name,
    f.peak_ccu,
    f.average_playtime_forever,
    f.price
FROM dim_games g
JOIN fact_game_statistics f ON g.game_id = f.game_id
ORDER BY f.peak_ccu DESC;

CREATE OR REPLACE VIEW achievements_view AS
SELECT
    g.name AS game_name,
    a.avg_ach_pct,
    f.achievements,
    f.average_playtime_forever,
    f.peak_ccu
FROM dim_games g
JOIN fact_achievements a ON g.game_id = a.game_id
JOIN fact_game_statistics f ON g.game_id = f.game_id
ORDER BY a.avg_ach_pct DESC;