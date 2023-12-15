from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# build spark session
conf = SparkConf().setAppName("Demo_Session").setMaster("spark://workspace:7077")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)