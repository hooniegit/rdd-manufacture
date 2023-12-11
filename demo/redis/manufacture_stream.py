from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import redis

def process_message(message):
    print(f"Received: {message['data']} from channel {message['channel']}")

# 스트리밍 처리를 위한 함수
def process_stream(rdd):
    rdd.foreach(process_message)


sc = SparkContext(appName="demo_stream")
ssc = StreamingContext(sc, 1)

redis_host = 'localhost'
redis_port = 6379
redis_client = redis.StrictRedis(host=redis_host, 
                                 port=redis_port, 
                                 decode_responses=True)

channel_name = 'hoonie_channel'
pubsub = redis_client.pubsub()
pubsub.subscribe(channel_name)

def process_stream(rdd):
    for record in rdd.collect():
        print(f"Received: {record}")

# 스트리밍 소스 생성
stream = ssc.socketTextStream("localhost", 6379)

# 스트리밍 처리 함수 설정
stream.foreachRDD(process_stream)

# 스트리밍 시작
ssc.start()
ssc.awaitTermination()