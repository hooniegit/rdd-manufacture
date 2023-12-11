from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(appName="demo_stream")
ssc = StreamingContext(sc, 1)

def process_stream(rdd):
    for record in rdd.collect():
        print(f"Received: {record}")

stream = ssc.socketTextStream("localhost", 9999)
stream.foreachRDD(process_stream)

ssc.start()
ssc.awaitTermination()
