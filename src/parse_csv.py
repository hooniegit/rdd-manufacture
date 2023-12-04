import os

now_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(now_dir, "../../../datas/ml-latest/movies_fixed.csv")

from pyspark.sql import SparkSession
from pyspark.sql.functions import split

# build spark session
spark = SparkSession.builder.appName("csv_count_genres").getOrCreate()

# create dataframe == read csv
df = spark.read.csv(data_dir, header=True, inferSchema=True)

# split genres & create genre
df_parsed = df.withColumn("genres", split(df["genre"], "\|"))

# change dataframe to rdd
rdd_data = df_parsed.rdd

# manufacture rdd
rdd_result = rdd_data.flatMap(lambda row: [(row.id, row.name, genre) for genre in row.genres]) \
                     .map(lambda record: (record[2], 1)) \
                     .reduceByKey(lambda x, y: x + y)

# change rdd to dataframe
df_result = rdd_result.toDF(["genre", "count"])

# show dataframe
df_result.show()

# stop spark session
spark.stop()