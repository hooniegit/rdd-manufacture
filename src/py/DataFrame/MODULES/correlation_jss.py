from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

spark = SparkSession.builder.appName("MovieTags").getOrCreate()
movie = spark.read.csv("/Users/kimdohoon/git/datas/ml-latest/movies.csv", header=True, inferSchema=True)
tag=spark.read.csv("/Users/kimdohoon/git/datas/ml-latest/tags.csv", header=True, inferSchema=True)
rating=spark.read.csv("/Users/kimdohoon/git/datas/ml-latest/ratings.csv", header=True, inferSchema=True)

total_data = movie.join(tag,on="movieId",how='Inner') \
                  .join(rating,on=['movieId','userId'],how="Inner")

indexer = StringIndexer(inputCol='tag', outputCol='tag_index')
indexed = indexer.fit(total_data).transform(total_data)

first_value_udf = udf(lambda x: float(x[0]), DoubleType())
encoder = OneHotEncoder(inputCol='tag_index', outputCol='tag_encoded')

encoder.fit(indexed).transform(indexed).show()

result = encoder.fit(indexed).transform(indexed) \
                 .withColumn('tag_encoded_value', first_value_udf('tag_encoded')) \
                 .withColumn('rating_double', F.col('rating').cast('double')) \
                 .select(F.corr('tag_encoded_value', 'rating_double')).collect()[0][0]

print(result)
spark.stop()