--Run a simple query, distribution of ratings

DROP TABLE IF EXISTS RATINGDISTRIBUTION;

CREATE TABLE RATINGDISTRIBUTION AS
SELECT rating, COUNT(rating)
FROM ratings
WHERE rating IS NOT NULL
GROUP BY rating ORDER BY rating;
