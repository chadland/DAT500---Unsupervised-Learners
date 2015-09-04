--create table and import data
--ratings
CREATE EXTERNAL TABLE ratings (
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
CREATE EXTERNAL TABLE movies (
movieId BIGINT,
title STRING,
genres STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/hive/movies.csv' OVERWRITE INTO TABLE movies;

--tags
CREATE EXTERNAL TABLE tags (
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
CREATE EXTERNAL TABLE links (
movieId BIGINT,
imdbId BIGINT,
tmdbId BIGINT)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/hive/links.csv' OVERWRITE INTO TABLE links;

--Run a simple query, count ratings pr. user and list top 10 results
Select 
userId, 
COUNT(movieId) as moviesRated FROM 
ratings 
GROUP BY userId ORDER BY moviesRated desc limit 10;


