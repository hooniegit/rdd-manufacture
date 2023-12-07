import os
from time import time

# record start time
start_time = time()

now_dir = os.path.dirname(os.path.abspath(__file__))
scores_dir = os.path.join(now_dir, "../../../../../datas/ml-latest/genome-scores.csv")
tags_dir = os.path.join(now_dir, "../../../../../datas/ml-latest/genome-tags.csv")
movies_dir = os.path.join(now_dir, "../../../../../datas/ml-latest/movies.csv")

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import row_number, desc

# build spark session
conf = SparkConf().setAppName("movies_and_genoms")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# create dataframe
df_score = spark.read.csv(f"file://{scores_dir}", header=True, inferSchema=True)
df_tag = spark.read.csv(f"file://{tags_dir}", header=True, inferSchema=True)

# join dataframes
df_genome = df_score.join(df_tag, on="tagId", how="inner")

# create dataframe
df_movies = spark.read.csv(f"file://{movies_dir}", header=True, inferSchema=True)

# set Windows specification
window_spec = Window.partitionBy("tagId").orderBy(desc("relevance"))

# manufacture dataframe
df_merged = df_genome.join(df_movies, on="movieId", how="inner") \
                     .withColumn("rank", row_number().over(window_spec)) \
                     .filter("rank = 1") \
                     .drop("rank")    

df_merged.show()
                    
# stop spark session
spark.stop()

# record end time & print result
end_time = time()
print(f"spent : {end_time - start_time}")
