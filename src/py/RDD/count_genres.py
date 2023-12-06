import os
from time import time

# record start time
start_time = time()

now_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(now_dir, "../../../../../datas/ml-latest/movies.csv")

from pyspark.sql import SparkSession

# build spark session
spark = SparkSession.builder.appName("csv_count_genres").getOrCreate()

# manufacture rdd
rdd = spark.sparkContext.textFile(data_dir).map(lambda line: line.split(',')) \
                                           .map(lambda row: row[2].split('|')) \
                                           .flatMap(lambda row: [(row[i], 1) for i in range(len(row))]) \
                                           .reduceByKey(lambda x, y : x + y) \
                                           .sortBy(lambda row: row[1], ascending=False)

# transfer rdd to dataframe
df_result = rdd.toDF(["genre", "count"])
df_result.show()

# stop spark session
spark.stop()

# record end time & print result
end_time = time()
print(f"spent : {end_time - start_time}")