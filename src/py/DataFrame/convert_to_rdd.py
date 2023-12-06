import os

now_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(now_dir, "../../../../../datas/ml-latest/movies.csv")

from pyspark.sql import SparkSession

# build spark session
spark = SparkSession.builder.appName("csv_to_rdd").getOrCreate()

# create dataframe
df_csv = spark.read.csv(f"file://{data_dir}", header=True, inferSchema=True)

# change dataframe to rdd
rdd_data = df_csv.rdd

# print rdd
rdd_data.foreach(print)

# stop spark session
spark.stop()
