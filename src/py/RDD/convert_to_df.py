import os

now_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(now_dir, "../../../../../datas/ml-latest/movies.csv")

from pyspark.sql import SparkSession

# build spark session
spark = SparkSession.builder.appName("csv_to_rdd").getOrCreate()

# create rdd
rdd = spark.sparkContext.textFile(data_dir).map(lambda line: line.split(',')) \
                                           .map(lambda row: (row[0], row[1], row[2]))

# convert rdd to dataframe
df = rdd.toDF(["movieId", "title", "genres"])
df.show()

# stop spark session
spark.stop()
