import os

now_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(now_dir, "../../../../../../datas/ml-latest/movies.csv")

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("Convert_To_RDD").setMaster("spark://workspace:7077")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

df_csv = spark.read.csv(f"file://{data_dir}", header=True, inferSchema=True)
rdd_data = df_csv.rdd
rdd_data.foreach(print)

spark.stop()
