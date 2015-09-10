--create table and import data
--ratings
CREATE EXTERNAL TABLE IF NOT EXISTS ratings (
userId BIGINT, 
movieId BIGINT, 
rating DOUBLE,
`timestamp` FLOAT)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/hive/ratings.csv' OVERWRITE INTO TABLE ratings;

--create new date features in a new rating table derived from ratings
DROP TABLE IF EXISTS ratingsTimestamp;

CREATE TABLE ratingsTimestamp AS
SELECT 
*, 
from_unixtime(`timestamp`) as new_timestamp,
year(from_unixtime(`timestamp`)) as yearOfRating,
month(from_unixtime(`timestamp`)) as monthOfRating,
day(from_unixtime(`timestamp`)) as dayOfRating,
hour(from_unixtime(`timestamp`)) as hourOfRating
from ratings;

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
`timestamp` BIGINT)
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



