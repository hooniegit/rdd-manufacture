import os

now_dir = os.path.dirname(os.path.abspath(__file__))
cycle_dir = os.path.join(now_dir, "./json_cycle_finale.json")

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, explode, col

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# VectorAssembler: 컬럼 데이터를 기반으로 한 벡터 생성 모델
# RandomForestRegressor: 랜덤 포레스트 회귀 모델
# RegressionEvaluator: 회귀 평가 모델

conf = SparkConf().setAppName("demo")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

df = spark.read.option("multiline", "true").json(f"file://{cycle_dir}") \
                      .withColumn("rows", explode(expr("response.body.items.item"))) \
                      .select("rows.*")

# 'read option'에 'multiline true' 설정 필요 
# 'expr 함수를 사용해 스키마를 지정

for column_name in df.columns:
    df = df.withColumn(column_name, col(column_name).cast("double"))

# cast: 데이터 타입 변환

df = df.na.drop(subset=["avgM15Te", "avgRhm", "maxInsWsWd", "avgTs", "avgTa"])
print(df.count())
df.show()

# na.drop: 결측치 제거

assembler = VectorAssembler(inputCols=["avgM15Te", "avgRhm", "maxInsWsWd", "avgTs", "avgTa"], outputCol="features_vector")
df_assembled = assembler.transform(df)
df_assembled.show()

# VectorAssembler: dataframe을 가공하기 위한 주형
# 데이터프레임(with 벡터) 생성: assembler.transform(df)

rf_regressor = RandomForestRegressor(labelCol="avgTa", featuresCol="features_vector", numTrees=3)
model = rf_regressor.fit(df_assembled)
df_predicted = model.transform(df_assembled)
df_predicted.select("prediction").show()

# RandomForestRegressor: model을 생성하기 위한 주형  * dataframe 내에 vector 컬럼이 있어야 한다
# 모델 생성: regressor.fit(df_assembled)
# 데이터프레임(with 예측치) 생성: model.transform(df_assembled)

evaluator = RegressionEvaluator(labelCol="avgTa", predictionCol="prediction", metricName="mse")
mse = evaluator.evaluate(df_predicted)
print(f"Mean Squared Error (MSE): {mse}")

# RegressionEvaluator: mse를 계산하기 위한 주형
# mse 계산: evaluator.evaluate(df_predicted)