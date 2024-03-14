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
    'group.id': f"DEMO_MULTI_{_username.upper()}",
    'auto.offset.reset': 'earliest'   # Start consuming from the earliest available offset
}
consumer1 = Consumer(consumer_config)
consumer1.subscribe([topic_name])

consumer2 = Consumer(consumer_config)
consumer2.subscribe([topic_name])

print("-------------------------BEGIN CONSUMING--------------------------")

try:
    while True:
        msg1 = consumer1.poll(timeout=1.0)  # Adjust the timeout as needed
        if msg1 is None:
            continue
        if msg1.error():
            if msg1.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                print('%% %s [%d] reached end at offset %d\n' %
                      (msg1.topic(), msg1.partition(), msg1.offset()))
            elif msg1.error():
                # Error
                raise KafkaException(msg1.error())
        else:
            # Print the consumed message
            print('Received message (consumer1 - partition {}): {}'.format(msg1.partition(), msg1.value().decode('utf-8')))

        msg2 = consumer2.poll(timeout=1.0)  # Adjust the timeout as needed
        if msg2 is None:
            continue
        if msg2.error():
            if msg2.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                print('%% %s [%d] reached end at offset %d\n' %
                      (msg2.topic(), msg2.partition(), msg2.offset()))
            elif msg2.error():
                # Error
                raise KafkaException(msg.error())
        else:
            # Print the consumed message
            print('Received message (consumer2 - partition {}): {}'.format(msg2.partition(), msg2.value().decode('utf-8')))
except KeyboardInterrupt:
    pass
finally:
    # Close the consumer
    consumer1.close()
    consumer2.close()
print("--------------------------END CONSUMING----------------------------")