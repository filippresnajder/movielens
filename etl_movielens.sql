-- DB Creation
CREATE DATABASE POODLE_MOVIELENS_DB;

-- DB Schema
CREATE SCHEMA POODLE_MOVIELENS_DB.staging;
USE SCHEMA POODLE_MOVIELENS_DB.staging;

-- Creating staging tables
CREATE TABLE occupations_staging (
    occupation_id INT PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE age_group_staging (
    age_group_id INT PRIMARY KEY,
    name VARCHAR(45)
);

CREATE TABLE genres_staging (
    genres_id INT PRIMARY KEY,
    name VARCHAR(255)
);

CREATE TABLE users_staging (
    user_id INT PRIMARY KEY,
    age INT,
    gender CHAR(1),
    occupation_id INT,
    zip_code VARCHAR(255),
    FOREIGN KEY (occupation_id) REFERENCES occupations_staging(occupation_id),
    FOREIGN KEY (age) REFERENCES 
    age_group_staging(age_group_id)
);

CREATE TABLE movies_staging (
    movie_id INT PRIMARY KEY,
    title VARCHAR(255),
    release_year CHAR(4)
);

CREATE TABLE tags_staging (
    tag_id INT PRIMARY KEY,
    user_id INT,
    movie_id INT,
    tags VARCHAR(4000),
    created_at DATETIME,
    FOREIGN KEY (user_id) REFERENCES users_staging(user_id),
    FOREIGN KEY (movie_id) REFERENCES 
    movies_staging(movie_id)
);

CREATE TABLE genres_movies_staging (
    genres_movies_id INT PRIMARY KEY,
    movie_id INT,
    genre_id INT,
    FOREIGN KEY (movie_id) REFERENCES 
    movies_staging(movie_id),
    FOREIGN KEY (genre_id) REFERENCES genres_staging(genres_id)
);

CREATE TABLE ratings_staging (
    rating_id INT PRIMARY KEY,
    user_id INT,
    movie_id INT,
    rating INT,
    rated_at DATETIME,
    FOREIGN KEY (user_id) REFERENCES users_staging(user_id),
    FOREIGN KEY (movie_id) REFERENCES 
    movies_staging(movie_id)
);

-- Creating Stage
CREATE OR REPLACE STAGE my_stage;

-- Copying data into staging tables from csv file
COPY INTO occupations_staging
FROM @my_stage/occupations.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

COPY INTO age_group_staging
FROM @my_stage/age_group.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

COPY INTO genres_staging
FROM @my_stage/genres.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

COPY INTO users_staging
FROM @my_stage/users.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

COPY INTO movies_staging
FROM @my_stage/movies.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

COPY INTO tags_staging
FROM @my_stage/tags.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

COPY INTO genres_movies_staging
FROM @my_stage/genres_movies.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

COPY INTO ratings_staging
FROM @my_stage/ratings.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';


-- Creating dimensional tables
CREATE TABLE dim_users AS
SELECT DISTINCT
    u.user_id AS dim_userId,
    u.gender AS gender,
    a.name AS age_group,
    o.name AS occupation
FROM users_staging u
JOIN age_group_staging a ON u.age = a.age_group_id
JOIN occupations_staging o ON u.occupation_id = o.occupation_id;


CREATE TABLE dim_movies AS
SELECT DISTINCT
    m.movie_id AS movie_id,
    m.title AS title,
    m.release_year AS release_year,
    g.name AS genres,
    t.tags AS tag_name
FROM movies_staging m
JOIN genres_movies_staging gm ON gm.movie_id = m.movie_id
JOIN genres_staging g ON g.genres_id = gm.genre_id
LEFT JOIN tags_staging t ON t.movie_id = m.movie_id;

CREATE TABLE dim_time AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY DATE_TRUNC('HOUR', r.rated_at)) AS dim_timeID,
    TO_TIMESTAMP(r.rated_at) AS time,
    TO_NUMBER(TO_CHAR(r.rated_at, 'HH24')) AS hour
FROM ratings_staging r
GROUP BY r.rated_at;

CREATE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY rated_at) AS dim_dateID,
    CAST(rated_at AS DATE) AS date,  
    DATE_PART(day, rated_at) AS day,
    DATE_PART(month, rated_at) AS month,
    DATE_PART(year, rated_at) AS year,                
    DATE_PART(week, rated_at) AS week,
    DATE_PART(dow, rated_at) + 1 AS day_of_week,        
    CASE DATE_PART(dow, rated_at) + 1
        WHEN 1 THEN 'Pondelok'
        WHEN 2 THEN 'Utorok'
        WHEN 3 THEN 'Streda'
        WHEN 4 THEN 'Štvrtok'
        WHEN 5 THEN 'Piatok'
        WHEN 6 THEN 'Sobota'
        WHEN 7 THEN 'Nedeľa'
    END AS day_of_week_string,             
    CASE DATE_PART(month, rated_at)
        WHEN 1 THEN 'Január'
        WHEN 2 THEN 'Február'
        WHEN 3 THEN 'Marec'
        WHEN 4 THEN 'Apríl'
        WHEN 5 THEN 'Máj'
        WHEN 6 THEN 'Jún'
        WHEN 7 THEN 'Júl'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'Október'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS month_string         
FROM ratings_staging
GROUP BY rated_at,
         DATE_PART(day, rated_at),
         DATE_PART(month, rated_at), 
         DATE_PART(year, rated_at), 
         DATE_PART(week, rated_at), 
         DATE_PART(dow, rated_at);

-- Creating fact table         
CREATE TABLE fact_ratings AS
SELECT DISTINCT
       r.rating_id AS ratingId,
       r.rated_at AS rating_datetime,
       r.rating AS rating,
       du.dim_userid AS user_id,
       dm.movie_id AS movie_id,
       dd.dim_dateid AS date_id,
       dt.dim_timeid AS time_id
FROM ratings_staging r
JOIN dim_date dd ON CAST(r.rated_at AS DATE) = dd.date
JOIN dim_time dt ON TO_TIMESTAMP(r.rated_at) = dt.time
JOIN dim_users du ON du.dim_userid = r.user_id
JOIN dim_movies dm ON dm.movie_id = r.movie_id;

-- Dropping staging tables
DROP TABLE occupations_staging;
DROP TABLE age_group_staging;
DROP TABLE genres_staging;
DROP TABLE users_staging;
DROP TABLE movies_staging;
DROP TABLE tags_staging;
DROP TABLE genres_movies_staging;
DROP TABLE ratings_staging;



