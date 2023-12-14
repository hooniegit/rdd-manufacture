from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import redis
import json

# Redis로부터 데이터를 수신하는 함수
def receive_data_from_redis(rdd):
    if not rdd.isEmpty():
        for message in rdd.collect():
            print(f"Received message: {message}")

# Spark Session 및 StreamingContext 생성
spark = SparkSession.builder.appName("RedisStreamingExample").getOrCreate()
sc = spark.sparkContext
ssc = StreamingContext(sc, 1)  # 1초마다 배치 처리

# Redis 연결 정보 및 채널 설정
redis_host = "localhost"
redis_port = 6379
redis_channel = "hoonie_channel"

# Redis로부터 데이터를 수신하는 DStream 생성
stream = ssc \
    .socketTextStream(redis_host, redis_port) \
    .map(lambda message: json.loads(message))  # 각 메시지를 JSON 형태로 파싱

# 수신된 데이터 처리 함수 등록
stream.foreachRDD(receive_data_from_redis)

# 스트림 시작
ssc.start()

# 스트림이 종료될 때까지 대기
ssc.awaitTermination()
