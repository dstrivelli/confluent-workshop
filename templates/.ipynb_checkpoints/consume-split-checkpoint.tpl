from prometheus_client import start_http_server, Gauge
from confluent_kafka import DeserializingConsumer, Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from pprint import pprint as pp
import getpass

_username = "{{ _username }}"
_password = "{{ _password }}"

bootstrap_servers = 'bip-kafka.confluent.svc.cluster.local:9092'
topic_name = "{{ topic_name }}"

consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': _username,
    'sasl.password': _password,
    'group.id': f"DEMO_SPLIT_{_username.upper()}",
    'auto.offset.reset': 'earliest'   # Start consuming from the earliest available offset
}
consumer = Consumer(consumer_config)
consumer.subscribe([topic_name])
print("-------------------------BEGIN CONSUMING--------------------------")

try:
    while True:
        msg = consumer.poll(timeout=1.0)  # Adjust the timeout as needed
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                print('%% %s [%d] reached end at offset %d\n' %
                      (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                # Error
                raise KafkaException(msg.error())
        else:
            # Print the consumed message
            print('Received message (partition {}): {}'.format(msg.partition(), msg.value().decode('utf-8')))

except KeyboardInterrupt:
    pass
finally:
    # Close the consumer
    consumer.close()
print("--------------------------END CONSUMING----------------------------")