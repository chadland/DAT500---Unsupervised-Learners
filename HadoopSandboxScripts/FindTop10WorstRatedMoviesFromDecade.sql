--Run a simple query, count top 10 worst rated movies with over 10 ratings
--from time period: 1990-01-01 - 2000-01-01
--this is represented like: 641152000 - 946684800

DROP TABLE IF EXISTS WORSTRATINGS;

CREATE TABLE WORSTRATINGS1990 AS
Select 
b.title,
AVG(a.rating) as avgRating,
COUNT(a.rating) as nrRatings
FROM ratings a
JOIN movies b
ON a.movieId = b.movieId
WHERE a.`timestamp` BETWEEN 631152000 AND 946684800
GROUP BY
b.title
HAVING COUNT(a.rating) > 10
ORDER BY avgRating ASC limit 10;

