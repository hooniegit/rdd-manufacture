import redis


redis_host = 'localhost'
redis_port = 6379
redis_client = redis.StrictRedis(host=redis_host,
                                 port=redis_port, 
                                 decode_responses=True)

pubsub = redis_client.pubsub()

channel_name = 'hoonie_channel'
pubsub.subscribe(channel_name)

for message in pubsub.listen():
    if message['type'] == 'message':
        print(f"Received: '{message['data']}' from channel '{channel_name}'")