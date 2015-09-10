--ratings
CREATE EXTERNAL TABLE IF NOT EXISTS ratings (
userId BIGINT, 
movieId BIGINT, 
rating DECIMAL,
`timestamp` FLOAT)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/hive/ratings.csv' OVERWRITE INTO TABLE ratings;

--movies
CREATE EXTERNAL TABLE IF NOT EXISTS movies (
movieId BIGINT,
title STRING,
genres STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/hive/movies.csv' OVERWRITE INTO TABLE movies;

--tags
CREATE EXTERNAL TABLE IF NOT EXISTS tags (
userId BIGINT,
movieId BIGINT,
tag STRING,
`timestamp` FLOAT)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/hive/tags.csv' OVERWRITE INTO TABLE tags;

--links
CREATE EXTERNAL TABLE IF NOT EXISTS links (
movieId BIGINT,
imdbId BIGINT,
tmdbId BIGINT)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/hive/links.csv' OVERWRITE INTO TABLE links;


--Run a simple query, distribution of ratings
SELECT rating, COUNT(rating)
FROM ratings
WHERE rating IS NOT NULL
GROUP BY rating ORDER BY rating;


/* 
-- I don't know how to calculate the "sum of ratings" into the query. This is now set manually as 100234 (/100 for percent).
-- In a standard query, it should be "SELECT COUNT(rating) FROM ratings"

--Run a simple query, distribution of ratings with %
SELECT rating, COUNT(rating),
CONCAT(ROUND(COUNT(rating)/1002.34, 2), ' %')  
FROM ratings
WHERE rating IS NOT NULL
GROUP BY rating ORDER BY rating;
*/







