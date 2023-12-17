from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

conf = SparkConf().setAppName("SparkProducer")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

data = [("HOONIE_1", 25), ("HOONIE_2", 27), ("HOONIE_3", 29)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

kafka_bootstrap_servers = "localhost:9093"

kafka_topic = "hoonie"

df_json = df.select(col("Name").cast(StringType()), 
                    col("Age").cast(StringType())).toJSON()

df_json.write.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("topic", kafka_topic) \
    .save()

spark.stop()
