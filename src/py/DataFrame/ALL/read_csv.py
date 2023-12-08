import os

now_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(now_dir, "../../../../../../datas/ml-latest/movies.csv")

from pyspark.sql import SparkSession

# build spark session
spark = SparkSession.builder.appName("csv_to_rdd").getOrCreate()

# create dataframe
df_csv = spark.read.csv(f"file://{data_dir}", header=True, inferSchema=True)

# show
df_csv.show()

# stop spark session
spark.stop()
