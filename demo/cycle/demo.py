import os

now_dir = os.path.dirname(os.path.abspath(__file__))
cycle_dir = os.path.join(now_dir, "./json_cycle_finale.json")

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, explode

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

conf = SparkConf().setAppName("demo")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

df = spark.read.option("multiline", "true").json(f"file://{cycle_dir}") \
                      .withColumn("rows", explode(expr("response.body.items.item"))) \
                      .select("rows.*")

for column_name in df.columns:
    df = df.withColumn(column_name, col(column_name).cast("double"))

df = df.na.drop(subset=["avgM15Te", "avgRhm", "maxInsWsWd", "avgTs", "avgTa"])
print(df.count())
df.show()

assembler = VectorAssembler(inputCols=["avgM15Te", "avgRhm", "maxInsWsWd", "avgTs", "avgTa"], outputCol="features_vector")
df_assembled = assembler.transform(df)
df_assembled.show()

rf_regressor = RandomForestRegressor(labelCol="avgTa", featuresCol="features_vector", numTrees=3)
model = rf_regressor.fit(df_assembled)
df_predicted = model.transform(df_assembled)
df_predicted.select("prediction").show()

evaluator = RegressionEvaluator(labelCol="avgTa", predictionCol="prediction", metricName="mse")
mse = evaluator.evaluate(df_predicted)
print(f"Mean Squared Error (MSE): {mse}")