-- 10 Najviac hodnotených filmov
SELECT m.title AS Movie_Name, COUNT(ratingid) AS Total_Ratings
FROM fact_ratings f
JOIN dim_movies m ON m.movie_id = f.movie_id
GROUP BY Movie_Name
ORDER BY Total_Ratings DESC
LIMIT 10;

-- Podiel hodnotení pre jednotlivé pohlavia
SELECT u.gender, COUNT(r.ratingid) AS Pocet
FROM fact_ratings r
JOIN dim_users u ON u.dim_userid = r.user_id
GROUP BY u.gender;

-- 15 Najlepšie hodnotených filmov na základe priemerného hodnoteni
SELECT m.title AS Film, ROUND(AVG(r.rating),2) AS Priemerne_Hodnotenie
FROM fact_ratings r
JOIN dim_movies m ON m.movie_id = r.movie_id
GROUP BY Film
ORDER BY Priemerne_Hodnotenie DESC
LIMIT 15;

-- Povolania, ktoré hodnotili filmy najviac (Top 5)
SELECT u.occupation AS Povolanie, COUNT(r.ratingid) AS Pocet
FROM fact_ratings r
JOIN dim_users u ON u.dim_userid = r.user_id
GROUP BY Povolanie
ORDER BY Pocet DESC
LIMIT 5;

-- Najaktívnejšie mesiace na základe hodnotenia
SELECT d.month_string AS Mesiac, COUNT(r.ratingid) AS Pocet
FROM fact_ratings r
JOIN dim_date d ON d.dim_dateid = r.date_id
GROUP BY Mesiac
ORDER BY Pocet DESC;

-- Rozdelenie hodnotení podľa vekových kategórii v daných časových údajoch
SELECT t.ampm AS Casovy_Udaj,
       u.age_group AS Vekova_Kategoria,
       COUNT(r.ratingid) AS Pocet
FROM fact_ratings r
JOIN dim_users u ON u.dim_userid = r.user_id
JOIN dim_time t ON t.dim_timeid = r.time_id
GROUP BY casovy_udaj, vekova_kategoria
ORDER BY casovy_udaj, Pocet DESC;