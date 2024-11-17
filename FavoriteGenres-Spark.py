from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, count, desc, row_number, udf
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.window import Window

# Simplified movie ID -> genre dictionary loader
def loadMovieGenres():
    genreNames = ["Unknown", "Action", "Adventure", "Animation", "Children's", "Comedy", 
                  "Crime", "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", 
                  "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"]
    movieGenres = {}
    with open("u.item") as f:
        for line in f:
            fields = line.strip().split('|')
            movieID = int(fields[0])
            genres = [genreNames[i] for i, g in enumerate(fields[5:]) if g == '1']
            movieGenres[movieID] = genres
    return movieGenres

if __name__ == "__main__":
    spark = SparkSession.builder.appName("UserGenrePreferences").getOrCreate()

    # Load and broadcast movie genres
    movieGenres = loadMovieGenres()
    movieGenresBroadcast = spark.sparkContext.broadcast(movieGenres)

    # Read and parse the dataset
    ratings = spark.read.option("delimiter", "\t").csv(
        "hdfs:///user/maria_dev/ml-100k/u.data",
        schema="userID INT, movieID INT, rating FLOAT, timestamp STRING"
    )

    # UDF to map movieID to genres
    @udf(ArrayType(StringType()))
    def mapGenres(movieID):
        return movieGenresBroadcast.value.get(movieID, [])

    # Add genres and explode
    ratingsWithGenres = ratings.withColumn("genres", mapGenres(col("movieID")))
    explodedGenres = ratingsWithGenres.withColumn("genre", explode(col("genres")))

    # Count genres per user and find favorite
    genreCounts = explodedGenres.groupBy("userID", "genre").count()
    windowSpec = Window.partitionBy("userID").orderBy(desc("count"))
    favoriteGenres = genreCounts.withColumn("rank", row_number().over(windowSpec)).filter(col("rank") == 1)

    # Show results
    favoriteGenres.select("userID", "genre", "count").show(truncate=False)

    spark.stop()
