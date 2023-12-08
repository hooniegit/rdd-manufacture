import os
from time import time

# record start time
start_time = time()

now_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(now_dir, "../../../../../../datas/ml-latest/ratings.csv")

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime

# build spark session
spark = SparkSession.builder.appName("csv_change_timestamp").getOrCreate()

# create & manufacture rdd
df = spark.read.csv(f"file://{data_dir}", header=True, inferSchema=True) \
               .withColumn("date", from_unixtime("timestamp", "yyyy-MM-dd")) \
               .withColumn("time", from_unixtime("timestamp", "HH:mm:ss")) \
               .drop("timestamp")

# show dataframe
df.show()

# stop spark session
spark.stop()

# record end time & print result
end_time = time()
print(f"spent : {end_time - start_time}")