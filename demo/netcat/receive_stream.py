from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(appName="demo_stream")
ssc = StreamingContext(sc, 1)

stream = ssc.socketTextStream("localhost", 9999)
stream.pprint()

ssc.start()
ssc.awaitTermination()
