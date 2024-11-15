from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, desc

# Load up movie ID -> movie name dictionary
def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    return movieNames

# Convert u.data lines into (userID, movieID, rating) rows
def parseInput(line):
    fields = line.value.split()
    return Row(userID=int(fields[0]), movieID=int(fields[1]), rating=float(fields[2]))

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("Top10Movies").getOrCreate()

    # Load up our movie ID -> name dictionary
    movieNames = loadMovieNames()

    # Get the raw data
    lines = spark.read.text("hdfs:///user/maria_dev/ml-100k/u.data").rdd

    # Convert it to an RDD of Row objects with (userID, movieID, rating)
    ratingsRDD = lines.map(parseInput)

    # Convert to a DataFrame and cache it
    ratings = spark.createDataFrame(ratingsRDD).cache()

    # Calculate the average rating and rating count for each movie
    movieStats = ratings.groupBy("movieID").agg(
        avg("rating").alias("avgRating"),
        count("rating").alias("ratingCount")
    )

    # Filter for movies with at least 50 ratings
    popularMovies = movieStats.filter("ratingCount >= 50")

    # Sort by average rating in descending order and take the top 10
    top10Movies = popularMovies.orderBy(desc("avgRating")).limit(10)

    # Collect and print the results
    print("\nTop 10 movies based on average rating (with at least 50 ratings):")
    for row in top10Movies.collect():
        print("{0}: {1:.2f} ({2} ratings)".format(
            movieNames[row['movieID']], row['avgRating'], row['ratingCount']))

    spark.stop()
