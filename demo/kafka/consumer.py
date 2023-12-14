from pyspark.sql import SparkSession
 
sc = SparkSession.builder.getOrCreate()
 
sc.sparkContext.setLogLevel('ERROR')
 
log = sc.readStream.format("kafka") \
.option("kafka.bootstrap.servers", "localhost:9092") \
.option("subscribe", "test") \
.option("startingOffsets", "earliest") \
.load()
 
query = log.selectExpr("CAST(value AS STRING)") \
.writeStream \
.format("console") \
.option("truncate", "false") \
.start()
 
 
query.awaitTermination()
