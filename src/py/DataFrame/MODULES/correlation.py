import os
from time import time

start_time = time()

now_dir = os.path.dirname(os.path.abspath(__file__))
ratings_dir = os.path.join(now_dir, "../../../../../../datas/ml-latest/ratings.csv")
tags_dir = os.path.join(now_dir, "../../../../../../datas/ml-latest/tags.csv")

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import length
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation

conf = SparkConf().setAppName("correlation")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

df_ratings = spark.read.csv(f"file://{ratings_dir}", header=True, inferSchema=True).select("movieId","rating")
df_tags = spark.read.csv(f"file://{tags_dir}", header=True, inferSchema=True).select("movieId", "tag")
df_merged = df_ratings.join(df_tags, on="movieId", how="inner") \
                      .withColumn("col_tag_length", length("tag").cast("double")) \
                      .withColumn("col_rating", col("rating").cast("double")) \
                      .select("col_tag_length", "col_rating")

# # convert to vector column first
# vector_col = "corr_features"
# assembler = VectorAssembler(inputCols=df.columns, outputCol=vector_col)
# df_vector = assembler.transform(df).select(vector_col)

# # get correlation matrix
# matrix = Correlation.corr(df_vector, vector_col)

vector_assembler = VectorAssembler(inputCols=["col_tag_length", "col_rating"], outputCol="features")
df_assembled = vector_assembler.transform(df_merged).select("features") \
                                                    .limit(10)

df_assembled.printSchema() # Check Schema
df_assembled.show(truncate=False)

r1 = Correlation.corr(df_assembled, "features") # Calculate Correlation


# df_result = df_merged.withColumn("correlation_result", correlation_coefficient)
# df_result.show()

spark.stop()

end_time = time()
print(f"spent : {end_time - start_time}")
