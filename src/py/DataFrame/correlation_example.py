import os
from time import time

# record start time
start_time = time()

now_dir = os.path.dirname(os.path.abspath(__file__))
ratings_dir = os.path.join(now_dir, "../../../../../../datas/ml-latest/ratings.csv")
tags_dir = os.path.join(now_dir, "../../../../../../datas/ml-latest/tags.csv")

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation

# build spark session
conf = SparkConf().setAppName("movies_and_genoms")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# create dataframe
df_ratings = spark.read.csv(f"file://{ratings_dir}", header=True, inferSchema=True).select("movieId","rating")
df_tags = spark.read.csv(f"file://{tags_dir}", header=True, inferSchema=True).select("movieId", "tag")

# join dataframes
df_merged = df_ratings.join(df_tags, on="movieId", how="inner") \
                      .drop("movieId")


# 두 컬럼을 벡터로 변환
vector_assembler = VectorAssembler(inputCols=["rating", "tag"], outputCol="features")
df_assembled = vector_assembler.transform(df_merged)

# 상관계수 계산
correlation_matrix = Correlation.corr(df_assembled, "features").head()
correlation_coefficient = correlation_matrix[0][0][1]

# 결과 출력
print(f"Correlation Coefficient between column1 and column2: {correlation_coefficient}")

# 데이터프레임에 새로운 컬럼으로 추가
df_result = df_merged.withColumn("correlation_result", correlation_coefficient)

# 결과 데이터프레임 출력
df_result.show()

# Spark 세션 종료
spark.stop()

# record end time & print result
end_time = time()
print(f"spent : {end_time - start_time}")
