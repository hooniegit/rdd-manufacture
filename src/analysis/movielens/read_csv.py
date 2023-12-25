import os

now_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(now_dir, "../../../../../datas/ml-latest/movies.csv")

from pyspark.sql import SparkSession

# build spark session
spark = SparkSession.builder.appName("csv_to_rdd").getOrCreate()

# create rdd
rdd = spark.sparkContext.textFile(data_dir).map(lambda line: line.split(','))

# print rdd
rdd.foreach(print)

# stop spark session
spark.stop()
