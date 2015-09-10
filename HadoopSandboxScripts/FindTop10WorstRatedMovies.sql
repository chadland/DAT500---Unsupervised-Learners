--Run a simple query, count top 10 worst rated movies with over 10 ratings
DROP TABLE IF EXISTS WORSTRATINGS;

CREATE TABLE WORSTRATINGS AS
Select 
b.title,
AVG(a.rating) as avgRating,
COUNT(a.rating) as nrRatings
FROM ratings a
JOIN movies b
ON a.movieId = b.movieId
GROUP BY
b.title
HAVING COUNT(a.rating) > 10
ORDER BY avgRating ASC limit 10;