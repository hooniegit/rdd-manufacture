from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from time import time

conf = SparkConf().setAppName("streaming_pub_sub") \
                  .setMaster("spark://workspace:7077")
sparkContext = SparkContext(conf=conf)
spark = SparkSession(sparkContext=sparkContext)

kafka_bootstrap_servers = "localhost:9092"
input_kafka_topic = "hoonie"
output_kafka_topic = "hoonie_back"

checkpoint_dir = "file:///home/hooniegit/git/study/rdd-manufacture/demo/kafka/checkpoint" 

def process_data(batchDF, batchId):
    # 새로운 처리 시작 시간 측정
    start_time = time()

    # transformed_df 생성
    transformed_df = batchDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # query1: console에 출력
    transformed_df \
        .write \
        .format("console") \
        .option("checkpointLocation", f"{checkpoint_dir}/query1_checkpoint") \
        .save()

    # spent_time 계산
    end_time = time()
    spent_time = float(f"{end_time - start_time:.2f}")

    # time_df 생성
    time_df = batchDF.drop("value") \
                     .withColumn("value", lit(spent_time))

    # query2: Kafka에 저장
    time_df \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", output_kafka_topic) \
        .option("checkpointLocation", f"{checkpoint_dir}/query2_checkpoint") \
        .save()

# 스트리밍 데이터프레임 생성 및 처리
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", input_kafka_topic) \
    .load()

# foreachBatch를 사용하여 각 배치에 대한 처리 함수 등록
query = df.writeStream \
    .foreachBatch(process_data) \
    .start()

query.awaitTermination()
