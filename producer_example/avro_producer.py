from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer


# https://betterprogramming.pub/avro-producer-with-python-and-confluent-kafka-library-4a1a2ed91a24
value_schema_str = """
{
   "namespace": "avro.example.topic",
   "name": "avro_example_topic_value",
   "type": "record",
   "fields" : [
     {
       "name" : "value_name",
       "type" : "string"
     },
     {
       "name" : "value_gender",
       "type" : "string"
     },
     {
       "name" : "value_email",
       "type" : "string"
     }
   ]
}
"""

key_schema_str = """
{
   "namespace": "avro.example.topic",
   "name": "avro_example_topic_key",
   "type": "record",
   "fields" : [
     {
       "name" : "key_country",
       "type" : "string"
     },
     {
       "name" : "key_city",
       "type" : "string"
     },
     {
       "name" : "key_type",
       "type" : "string"
     }
   ]
}
"""

value_schema = avro.loads(value_schema_str)
key_schema = avro.loads(key_schema_str)
value = {"value_name": "Jone", "value_gender":"male", "value_email":"123@abc.com"}
key = {"key_country": "Taiwan","key_city": "Kaohsiung", "key_type": "Normal"}


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


avroProducer = AvroProducer({
    'bootstrap.servers': 'localhost:9092',
    'on_delivery': delivery_report,
    'schema.registry.url': 'http://localhost:8081'
    }, default_key_schema=key_schema, default_value_schema=value_schema)

avroProducer.produce(topic='avro_example_topic', value=value, key=key)
avroProducer.flush()