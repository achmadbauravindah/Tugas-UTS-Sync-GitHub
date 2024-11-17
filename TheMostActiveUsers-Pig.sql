-- Load ratings data
ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);

-- Group ratings by userID
groupedByUser = GROUP ratings BY userID;

-- Count the number of movies rated by each user
userActivity = FOREACH groupedByUser GENERATE 
               group AS userID, 
               COUNT(ratings.movieID) AS totalRatings;

-- Sort users by totalRatings in descending order
sortedUsers = ORDER userActivity BY totalRatings DESC;

-- Limit the result to the top 10 most active users
top10Users = LIMIT sortedUsers 10;

-- Display the results
DUMP top10Users;