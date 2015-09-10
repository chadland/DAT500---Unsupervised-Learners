--Run a simple query, count ratings pr. user and store results top 10 results
DROP TABLE IF EXISTS TOPUSERS;

CREATE TABLE TOPUSERS AS
Select 
userId, 
COUNT(movieId) as moviesRated FROM 
ratings 
GROUP BY userId ORDER BY moviesRated desc limit 10;