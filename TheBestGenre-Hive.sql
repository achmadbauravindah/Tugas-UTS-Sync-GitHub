USE movielens;
SELECT 
    genre,
    AVG(r.rating) AS average_rating
FROM 
    (
        SELECT 
            i.movie_id,
            'Action' AS genre
        FROM items i WHERE i.action = 1
        UNION ALL
        SELECT 
            i.movie_id,
            'Adventure' AS genre
        FROM items i WHERE i.adventure = 1
        UNION ALL
        SELECT 
            i.movie_id,
            'Animation' AS genre
        FROM items i WHERE i.animation = 1
        UNION ALL
        SELECT 
            i.movie_id,
            'Children' AS genre
        FROM items i WHERE i.children = 1
        UNION ALL
        SELECT 
            i.movie_id,
            'Comedy' AS genre
        FROM items i WHERE i.comedy = 1
        UNION ALL
        SELECT 
            i.movie_id,
            'Crime' AS genre
        FROM items i WHERE i.crime = 1
        UNION ALL
        SELECT 
            i.movie_id,
            'Documentary' AS genre
        FROM items i WHERE i.documentary = 1
        UNION ALL
        SELECT 
            i.movie_id,
            'Drama' AS genre
        FROM items i WHERE i.drama = 1
        UNION ALL
        SELECT 
            i.movie_id,
            'Fantasy' AS genre
        FROM items i WHERE i.fantasy = 1
        UNION ALL
        SELECT 
            i.movie_id,
            'Film-Noir' AS genre
        FROM items i WHERE i.film_noir = 1
        UNION ALL
        SELECT 
            i.movie_id,
            'Horror' AS genre
        FROM items i WHERE i.horror = 1
        UNION ALL
        SELECT 
            i.movie_id,
            'Musical' AS genre
        FROM items i WHERE i.musical = 1
        UNION ALL
        SELECT 
            i.movie_id,
            'Mystery' AS genre
        FROM items i WHERE i.mystery = 1
        UNION ALL
        SELECT 
            i.movie_id,
            'Romance' AS genre
        FROM items i WHERE i.romance = 1
        UNION ALL
        SELECT 
            i.movie_id,
            'Sci-Fi' AS genre
        FROM items i WHERE i.sci_fi = 1
        UNION ALL
        SELECT 
            i.movie_id,
            'Thriller' AS genre
        FROM items i WHERE i.thriller = 1
        UNION ALL
        SELECT 
            i.movie_id,
            'War' AS genre
        FROM items i WHERE i.war = 1
        UNION ALL
        SELECT 
            i.movie_id,
            'Western' AS genre
        FROM items i WHERE i.western = 1
    ) AS genre_data
JOIN 
    ratings r
ON 
    genre_data.movie_id = r.item_id
GROUP BY 
    genre
ORDER BY 
    average_rating DESC
LIMIT 5;
