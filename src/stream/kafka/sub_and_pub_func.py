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

checkpoint_dir = "file:////home/hooniegit/git/study/rdd-manufacture/src/kafka/checkpoint" 

def process_data(batchDF, batchId):
    start_time = time()
    transformed_df = batchDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    transformed_df \
        .write \
        .format("console") \
        .option("checkpointLocation", f"{checkpoint_dir}/query1_checkpoint") \
        .save()

    end_time = time()
    spent_time = str({"time_spent": float(f"{end_time - start_time:.2f}")})
    time_df = batchDF.drop("value") \
                     .withColumn("value", lit(spent_time))

    time_df \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", output_kafka_topic) \
        .option("checkpointLocation", f"{checkpoint_dir}/query2_checkpoint") \
        .save()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", input_kafka_topic) \
    .load()

query = df \
    .writeStream \
    .foreachBatch(process_data) \
    .start()

query.awaitTermination()
