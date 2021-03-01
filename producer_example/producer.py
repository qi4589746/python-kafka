from confluent_kafka import Producer
import socket

conf = {'bootstrap.servers': "localhost:9092",
        'client.id': socket.gethostname()}

producer = Producer(conf)

# Asynchronous
def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))
producer.produce("example_topic1", key="data_string", value="abc", callback=acked)
producer.poll(1)


# Synchronous
producer.produce("example_topic1", key="data_string", value="abc")
producer.flush()