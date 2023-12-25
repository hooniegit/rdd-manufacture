from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from pyspark.ml.linalg import Vectors # Change List to Vector
from pyspark.ml.stat import Correlation # Calculate Correlations

conf = SparkConf().setAppName("correlation")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

data = [(Vectors.dense([5.0, 11.0]),),
        (Vectors.dense([5.0, 15.0]),),
        (Vectors.dense([5.0, 8.0]),),
        (Vectors.dense([5.0, 12.0]),),
        (Vectors.dense([5.0, 11.0]),),
        (Vectors.dense([5.0, 15.0]),),
        (Vectors.dense([5.0, 8.0]),),
        (Vectors.dense([5.0, 12.0]),)]

df = spark.createDataFrame(data, ["features"])
df.printSchema() # Check Data Type
df.show()

r1 = Correlation.corr(df, "features").head() # Calculate Correlations - Pearson
print("Pearson correlation matrix:\n" + str(r1[0]))

r2 = Correlation.corr(df, "features", "spearman").head() # Calculate Correlations - Spearman
print("Spearman correlation matrix:\n" + str(r2[0]))