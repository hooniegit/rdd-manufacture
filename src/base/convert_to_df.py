import os

now_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(now_dir, "../../../../../datas/ml-latest/movies.csv")

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("Convert_To_DataFrame").setMaster("spark://workspace:7077")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

rdd = spark.sparkContext.textFile(data_dir).map(lambda line: line.split(',')) \
                                           .map(lambda row: (row[0], row[1], row[2]))
schema = ["movieId", "title", "genres"]
df = rdd.toDF(schema)
df.show()

spark.stop()
