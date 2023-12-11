import redis
import time


redis_host = 'localhost'
redis_port = 6379
redis_client = redis.StrictRedis(host=redis_host,
                                 port=redis_port, 
                                 decode_responses=True)

channel_name = 'hoonie_channel'


def send_message(channel:str, message:str):
    try:
        redis_client.publish(channel, message)
        print(f"Published: '{message}' to channel '{channel_name}'")
    except Exception as E:
        print(f"Exception Appeared - {E}")


if __name__ == "__main__":
    from datetime import datetime

    channel = "hoonie_channel"
    interval = 1
		
    for count in range (1, 11):
        start_time = time.time()

        nowtime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        message = f"Nowtime: {nowtime}"
        send_message(channel, message)

        end_time = time.time()
        time.sleep(interval - (end_time - start_time))