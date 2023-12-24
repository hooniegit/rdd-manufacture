from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField

conf = SparkConf().setAppName("streaming_pub_sub") \
                  .setMaster("spark://workspace:7077")
sparkContext = SparkContext(conf=conf)
spark = SparkSession(sparkContext=sparkContext)

kafka_bootstrap_servers = "localhost:9092"
input_kafka_topic = "hoonie"
output_kafka_topic = "hoonie_back"

checkpoint_dir = "file:///home/hooniegit/git/study/rdd-manufacture/demo/kafka/checkpoint" 

# Function to perform data processing and record processing time
def process_data(batchDF, batchId):
    import time
    start_time = time.time()

    processed_data = perform_data_processing(batchDF)
    
    end_time = time.time()
    processing_time = end_time - start_time
    
    send_to_query2(processing_time)

def perform_data_processing(batchDF):
    # Modify this function with your actual data processing logic
    # Currently, it returns the same DataFrame as it receives
    return batchDF

def send_to_query2(processing_time):
    # Create a DataFrame with processing time
    data = [(str(processing_time),)]
    schema = StructType([StructField("processing_time", StringType(), True)])
    result_df = spark.createDataFrame(data=data, schema=schema)
    
    # Write the DataFrame to Kafka (query2)
    # Use "writeStream" instead of "write"
    result_df \
        .selectExpr("CAST(processing_time AS STRING) AS value") \
        .write\
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", output_kafka_topic) \
        .option("checkpointLocation", f"{checkpoint_dir}/query2_checkpoint") \

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
    .foreachBatch(process_data) \
    .option("checkpointLocation", f"{checkpoint_dir}/query1_checkpoint") \
    .start()

query1.awaitTermination()