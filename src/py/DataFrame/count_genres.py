import os
from time import time

# record start time
start_time = time()

now_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(now_dir, "../../../../../datas/ml-latest/movies.csv")

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode

# build spark session
spark = SparkSession.builder.appName("csv_count_genres").getOrCreate()

# create datframe
df = spark.read.csv(f"file://{data_dir}", header=True, inferSchema=True)

# manufacture dataframe
df_count = df.withColumn("genres_split", split(df["genres"], "\\|")) \
             .select(explode("genres_split").alias("genre")) \
             .groupBy("genre").count() \
             .orderBy("count", ascending=False)
             

# show dataframe
df_count.show()

# stop spark session
spark.stop()

# record end time & print result
end_time = time()
print(f"spent : {end_time - start_time}")