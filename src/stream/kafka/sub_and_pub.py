from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("streaming_pub_sub") \
                  .setMaster("spark://workspace:7077")
sparkContext = SparkContext(conf=conf)
spark = SparkSession(sparkContext=sparkContext)

kafka_bootstrap_servers = "localhost:9092"
input_kafka_topic = "hoonie"
output_kafka_topic = "hoonie_back"

checkpoint_dir = "file:///home/hooniegit/git/study/rdd-manufacture/src/kafka/checkpoint" 

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", input_kafka_topic) \
    .load()

transformed_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

query1 = transformed_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", f"{checkpoint_dir}/query1_checkpoint") \
    .start()

query2 = transformed_df \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("topic", output_kafka_topic) \
    .option("checkpointLocation", f"{checkpoint_dir}/query2_checkpoint") \
    .start()

query1.awaitTermination()
query2.awaitTermination()