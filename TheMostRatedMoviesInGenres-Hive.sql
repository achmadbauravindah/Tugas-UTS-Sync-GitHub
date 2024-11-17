USE movielens;

WITH GenreMovies AS (
    SELECT 
        i.movie_id,
        i.movie_title,
        'Action' AS genre
    FROM items i WHERE i.action = 1
    UNION ALL
    SELECT 
        i.movie_id,
        i.movie_title,
        'Adventure' AS genre
    FROM items i WHERE i.adventure = 1
    UNION ALL
    SELECT 
        i.movie_id,
        i.movie_title,
        'Animation' AS genre
    FROM items i WHERE i.animation = 1
    UNION ALL
    SELECT 
        i.movie_id,
        i.movie_title,
        'Children' AS genre
    FROM items i WHERE i.children = 1
    UNION ALL
    SELECT 
        i.movie_id,
        i.movie_title,
        'Comedy' AS genre
    FROM items i WHERE i.comedy = 1
    UNION ALL
    SELECT 
        i.movie_id,
        i.movie_title,
        'Crime' AS genre
    FROM items i WHERE i.crime = 1
    UNION ALL
    SELECT 
        i.movie_id,
        i.movie_title,
        'Documentary' AS genre
    FROM items i WHERE i.documentary = 1
    UNION ALL
    SELECT 
        i.movie_id,
        i.movie_title,
        'Drama' AS genre
    FROM items i WHERE i.drama = 1
    UNION ALL
    SELECT 
        i.movie_id,
        i.movie_title,
        'Fantasy' AS genre
    FROM items i WHERE i.fantasy = 1
    UNION ALL
    SELECT 
        i.movie_id,
        i.movie_title,
        'Film-Noir' AS genre
    FROM items i WHERE i.film_noir = 1
    UNION ALL
    SELECT 
        i.movie_id,
        i.movie_title,
        'Horror' AS genre
    FROM items i WHERE i.horror = 1
    UNION ALL
    SELECT 
        i.movie_id,
        i.movie_title,
        'Musical' AS genre
    FROM items i WHERE i.musical = 1
    UNION ALL
    SELECT 
        i.movie_id,
        i.movie_title,
        'Mystery' AS genre
    FROM items i WHERE i.mystery = 1
    UNION ALL
    SELECT 
        i.movie_id,
        i.movie_title,
        'Romance' AS genre
    FROM items i WHERE i.romance = 1
    UNION ALL
    SELECT 
        i.movie_id,
        i.movie_title,
        'Sci-Fi' AS genre
    FROM items i WHERE i.sci_fi = 1
    UNION ALL
    SELECT 
        i.movie_id,
        i.movie_title,
        'Thriller' AS genre
    FROM items i WHERE i.thriller = 1
    UNION ALL
    SELECT 
        i.movie_id,
        i.movie_title,
        'War' AS genre
    FROM items i WHERE i.war = 1
    UNION ALL
    SELECT 
        i.movie_id,
        i.movie_title,
        'Western' AS genre
    FROM items i WHERE i.western = 1
), RatingsSummary AS (
    SELECT
        r.item_id,
        COUNT(r.rating) AS total_ratings,
        AVG(r.rating) AS average_rating
    FROM 
        ratings r
    GROUP BY 
        r.item_id
    HAVING 
        COUNT(r.rating) >= 30
), CombinedData AS (
    SELECT
        gm.movie_id,
        gm.movie_title,
        gm.genre,
        rs.total_ratings,
        rs.average_rating,
        (rs.total_ratings * rs.average_rating) AS score
    FROM 
        GenreMovies gm
    JOIN 
        RatingsSummary rs
    ON 
        gm.movie_id = rs.item_id
), RankedMovies AS (
    SELECT 
        genre,
        movie_title,
        total_ratings,
        average_rating,
        score,
        ROW_NUMBER() OVER (PARTITION BY genre ORDER BY score DESC) AS rank
    FROM 
        CombinedData
)
SELECT 
    genre,
    movie_title,
    total_ratings,
    average_rating,
    score
FROM 
    RankedMovies
WHERE 
    rank = 1
ORDER BY 
    score DESC;
