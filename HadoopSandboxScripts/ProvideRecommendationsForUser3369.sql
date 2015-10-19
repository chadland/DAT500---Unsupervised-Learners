/* Works in HIVE QL*/
/*Step 1, 
Select active user and the max rated movie  */

CREATE TABLE UserMaxRatedMovies AS
SELECT
a.*
FROM 
ratings a
INNER JOIN 
(SELECT 
userId,
MAX(rating) AS maxRating,
AVG(rating) AS avgRating
FROM 
ratings
GROUP BY
userId) b
ON 
a.userId = b.userId AND
a.rating = b.maxRating 
WHERE a.userId=3369
ORDER BY a.`timestamp` DESC LIMIT 1;

/*Step2, 
select movies to compare
Selected 3 movies withe everyone else*/
CREATE TABLE USER3369 AS
SELECT DISTINCT 
a.userId, a.movieId, a.rating, a.`timestamp`
FROM
ratings a
INNER JOIN 
ratings b
ON 
a.userId = b.userId
INNER JOIN 
UserMaxRatedMovies c
ON
b.movieId = c.movieId
LEFT JOIN 
(SELECT *
FROM 
ratings
WHERE userId=3369 AND
movieId NOT IN (SELECT movieId FROM UserMaxRatedMovies)  ) d
ON 
a.movieId = d.movieId
WHERE d.movieId IS NULL 
ORDER BY rand() LIMIT 500000;


/*Run Python script with Step 2, and provide recommendations*/
