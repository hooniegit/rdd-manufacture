import pyspark
spark = pyspark.sql.SparkSession.builder.appName("MyApp") \
            .master("spark://workspace:7077") \
            .config("spark.jars.packages", "com.microsoft.azure:synapseml_2.12:1.0.2") \
            .getOrCreate()
            
import synapse.ml

triazines = spark.read.format("libsvm").load(
    "wasbs://publicwasb@mmlspark.blob.core.windows.net/triazines.scale.svmlight"
)

# print some basic info
print("records read: " + str(triazines.count()))
print("Schema: ")
triazines.printSchema()
display(triazines.limit(10))

train, test = triazines.randomSplit([0.85, 0.15], seed=1)

from synapse.ml.lightgbm import LightGBMRegressor

model = LightGBMRegressor(
    objective="quantile", alpha=0.2, learningRate=0.3, numLeaves=31
).fit(train)

scoredData = model.transform(test)
display(scoredData)

from synapse.ml.train import ComputeModelStatistics

metrics = ComputeModelStatistics(
    evaluationMetric="regression", labelCol="label", scoresCol="prediction"
).transform(scoredData)
display(metrics)

