-- Počet hodnotení pre jednotlivé pohlavia
SELECT d.gender AS Pohlavie, COUNT(f.ratingid) AS Pocet
FROM fact_ratings f
JOIN dim_users d ON d.dim_userid = f.user_id
GROUP BY Pohlavie;

-- Najlepšie hodnotené filmy so 100 alebo viac hodnoteniami
SELECT m.title AS Film, ROUND(AVG(f.rating), 2) Hodnotenie
FROM fact_ratings f
JOIN dim_movies m ON m.movie_id = f.movie_id
GROUP BY Film HAVING COUNT(f.movie_id) >= 100
ORDER BY Hodnotenie DESC LIMIT 10;

-- Počet hodnotení v jednotlivých dňoch v týždni
SELECT d.day_of_week_string AS Den, COUNT(f.ratingid)
FROM fact_ratings f
JOIN dim_date d ON d.dim_dateid = f.date_id
GROUP BY Den;

-- Počet hodnotení v jednotlivých hodinách dňa
SELECT t.hour AS Hodina, COUNT(f.ratingid) AS Pocet
FROM fact_ratings f
JOIN dim_time t ON t.dim_timeid = f.time_id
GROUP BY Hodina
ORDER BY POCET DESC;

-- Najlepšie Hodnotené žánre (Top 10)
SELECT m.genres AS Zaner, ROUND(AVG(f.rating), 2) Hodnotenie
FROM fact_ratings f
JOIN dim_movies m ON m.movie_id = f.movie_id
GROUP BY Zaner
ORDER BY Hodnotenie DESC LIMIT 10;