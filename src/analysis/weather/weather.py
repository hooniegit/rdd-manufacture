import pyspark
spark = pyspark.sql.SparkSession.builder.appName("MyApp") \
            .master("spark://workspace:7077") \
            .config("spark.jars.packages", "com.microsoft.azure:synapseml_2.12:1.0.2") \
            .getOrCreate()


# READ & CREATE DATAFRAME
from pyspark.sql.functions import expr, explode, col

weather_dir = "/home/hooniegit/git/study/rdd-manufacture/data/json_weather.json"
df = spark.read.option("multiline", "true").json(f"file://{weather_dir}") \
                      .withColumn("rows", explode(expr("response.body.items.item"))) \
                      .select("rows.*")
df.show()

for column_name in df.columns:
    df = df.withColumn(column_name, col(column_name).cast("double"))

# 1.5m 지중온도 / 평균 상대습도 / 평균 증기압 / 최대 풍속 / 일강수량
df = df.select("avgTa", "avgRhm", "avgPv", "maxWs", "sumRn")\
       .na.drop()
train, test = df.randomSplit([0.85, 0.15], seed=1)


# ASSEMBLE VECTOR
from pyspark.ml.feature import VectorAssembler

inputCols = df.columns[1:]
featurizer = VectorAssembler(inputCols=inputCols, outputCol="features", handleInvalid="skip")
train_data = featurizer.transform(train)["avgTa", "features"]
test_data = featurizer.transform(test)["avgTa", "features"]

train_data.show()


# CREATE REGRESSOR & FIT DATA
from synapse.ml.lightgbm import LightGBMRegressor

model = LightGBMRegressor(
    objective="regression", labelCol="avgM15Te", featuresCol="features", alpha=0.2, learningRate=0.3, numLeaves=31
).fit(train_data)


# TEST DATASET
scoredData = model.transform(test_data)
scoredData.select("prediction").show()


from synapse.ml.train import ComputeModelStatistics

metrics = ComputeModelStatistics(
    evaluationMetric="regression", labelCol="avgM15Te", scoresCol="prediction"
).transform(scoredData)

metrics.show()
