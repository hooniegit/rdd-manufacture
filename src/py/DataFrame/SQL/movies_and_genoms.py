import os
from time import time

# record start time
start_time = time()

now_dir = os.path.dirname(os.path.abspath(__file__))
scores_dir = os.path.join(now_dir, "../../../../../../datas/ml-latest/genome-scores.csv")
tags_dir = os.path.join(now_dir, "../../../../../../datas/ml-latest/genome-tags.csv")
movies_dir = os.path.join(now_dir, "../../../../../../datas/ml-latest/movies.csv")

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# build spark session
conf = SparkConf().setAppName("movies_and_genoms")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# create dataframe
df_score = spark.read.csv(f"file://{scores_dir}", header=True, inferSchema=True)
df_tag = spark.read.csv(f"file://{tags_dir}", header=True, inferSchema=True)
df_movies = spark.read.csv(f"file://{movies_dir}", header=True, inferSchema=True)

# create temp view
df_score.createOrReplaceTempView("scores")
df_tag.createOrReplaceTempView("tags")
df_movies.createOrReplaceTempView("movies")

# create dataframe with sql
df_merged = spark.sql("""SELECT s.movieId, m.title, m.genres, s.tagId, t.tag, s.relevance
          FROM scores s
          JOIN tags t ON s.tagId = t.tagId
          JOIN movies m ON s.movieId = m.movieId 
          """)

df_merged.show()

# stop spark session
spark.stop()

# record end time & print result
end_time = time()
print(f"spent : {end_time - start_time}")
