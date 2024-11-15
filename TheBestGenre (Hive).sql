SELECT 
    genre,
    AVG(r.rating) AS average_rating
FROM 
    (
        SELECT 
            i.movie_id,
            CASE 
                WHEN i.action = 1 THEN 'action'
                WHEN i.adventure = 1 THEN 'adventure'
                WHEN i.animation = 1 THEN 'animation'
                WHEN i.children = 1 THEN 'children'
                WHEN i.comedy = 1 THEN 'comedy'
                WHEN i.crime = 1 THEN 'crime'
                WHEN i.documentary = 1 THEN 'documentary'
                WHEN i.drama = 1 THEN 'drama'
                WHEN i.fantasy = 1 THEN 'fantasy'
                WHEN i.film_noir = 1 THEN 'film_noir'
                WHEN i.horror = 1 THEN 'horror'
                WHEN i.musical = 1 THEN 'musical'
                WHEN i.mystery = 1 THEN 'mystery'
                WHEN i.romance = 1 THEN 'romance'
                WHEN i.sci_fi = 1 THEN 'sci_fi'
                WHEN i.thriller = 1 THEN 'thriller'
                WHEN i.war = 1 THEN 'war'
                WHEN i.western = 1 THEN 'western'
                ELSE 'unknown'
            END AS genre
        FROM 
            items i
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
