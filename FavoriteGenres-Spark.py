from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, count, desc, row_number
from pyspark.sql.types import ArrayType, StringType, Row
from pyspark.sql.window import Window

# Load up movie ID -> movie name and genre dictionary
def loadMovieGenres():
    movieGenres = {}
    with open("u.item") as f:
        for line in f:
            fields = line.split('|')
            movieID = int(fields[0])
            genres = fields[5:]  # Genres are binary encoded as 0/1
            genreNames = ["Unknown", "Action", "Adventure", "Animation", "Children's", "Comedy", 
                          "Crime", "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", 
                          "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"]
            genreList = [genreNames[i] for i, g in enumerate(genres) if g == '1']
            movieGenres[movieID] = genreList
    return movieGenres

# Convert u.data lines into (userID, movieID, rating) rows
def parseInput(line):
    fields = line.value.split()
    return Row(userID=int(fields[0]), movieID=int(fields[1]), rating=float(fields[2]))

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("UserGenrePreferences").getOrCreate()

    # Load up our movie ID -> genre dictionary
    movieGenres = loadMovieGenres()

    # Broadcast the movieGenres dictionary
    movieGenresBroadcast = spark.sparkContext.broadcast(movieGenres)

    # Get the raw data
    lines = spark.read.text("hdfs:///user/maria_dev/ml-100k/u.data").rdd

    # Convert it to an RDD of Row objects with (userID, movieID, rating)
    ratingsRDD = lines.map(parseInput)

    # Convert to a DataFrame
    ratings = spark.createDataFrame(ratingsRDD)

    # Add genre information to ratings DataFrame
    def addGenres(movieID):
        return movieGenresBroadcast.value.get(movieID, [])

    # Register a UDF to map movieID to genres
    addGenresUDF = udf(addGenres, ArrayType(StringType()))

    ratingsWithGenres = ratings.withColumn("genres", addGenresUDF(col("movieID")))

    # Explode the genres array to have one row per genre per rating
    explodedGenres = ratingsWithGenres.withColumn("genre", explode(col("genres")))

    # Calculate the genre preferences for each user
    userGenrePreferences = explodedGenres.groupBy("userID", "genre").agg(count("*").alias("genreCount"))

    # Use a Window to find the favorite genre for each user
    windowSpec = Window.partitionBy("userID").orderBy(desc("genreCount"))

    # Add a row number for each user and genre based on the count
    rankedGenres = userGenrePreferences.withColumn("rank", row_number().over(windowSpec))

    # Filter to get only the top-ranked genre for each user
    favoriteGenres = rankedGenres.filter(col("rank") == 1).select("userID", "genre", "genreCount")

    # Collect and display results
    print("\nUser Genre Preferences:")
    favoriteGenres.show(truncate=False)

    spark.stop()
