from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

bootstrap_servers = 'localhost:9092'
topic = 'test'

conf = {'bootstrap.servers': bootstrap_servers}
producer = Producer(conf)

for i in range(10):
    message = f"DOHOON - Message {i}"
    producer.produce(topic, key=str(i), value=message, callback=delivery_report)

producer.flush()
