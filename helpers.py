from prometheus_client import start_http_server, Gauge
from confluent_kafka import DeserializingConsumer, Consumer, Producer, TopicPartition, SerializingProducer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import StringDeserializer
from IPython.display import display, Image
from pprint import pprint as pp
from jinja2 import Template
from faker import Faker
from kubernetes import client, config
import requests
import getpass
import time

def render_from_template(template_name, data):
    # Expect template_name to exclude .tpl and .py
    with open(f"templates/{template_name}.tpl", 'r') as file:
        template_content = file.read()
    
    template = Template(template_content)
    rendered_template = template.render(data)
    
    # Write the rendered template to a new file
    with open(f"{template_name}.py", 'w') as file:
        file.write(rendered_template)
        
def admin_create_topic(admin_client, topic_name = None, num_partitions = None, replication_factor = None, topics=[]):
    if len(topics) > 0:
        return admin_client.create_topics(topics)
    return admin_client.create_topics([NewTopic(topic_name, num_partitions, replication_factor)])

def get_image():
    url = 'https://picsum.photos/200'
    resp = requests.get(url)
    if resp.status_code == 200:
        image = resp.content
        return image
    return None

def admin_delete_topic(admin_client, topic_name):
    return admin_client.delete_topics([topic_name])

def send_data_to_topic(producer, topic_name, num_messages, fake=False, messages=[]):
    if fake:
        fake = Faker()
        for _ in range(num_messages):
            producer.produce(topic_name, fake.sentence())
            producer.flush()
        return
    else:
        if len(messages) > 0:
            for msg in messages:
                producer.produce(topic_name, msg)
                producer.flush()
            return
        else:
            print("messages array is empty and fake is False")

def consume(consumer, num_messages):
    incoming_messages = []
    try:
        for i in range(num_messages):
            msg = consumer.poll(timeout=2.0)
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
                incoming_messages.append(msg.value())
    except KeyboardInterrupt:
        pass
    finally:
        return incoming_messages


def list_confluent_rolebindings():
    config.load_incluster_config()
    api_client = client.CustomObjectsApi()
    group = "platform.confluent.io"
    version = "v1beta1"
    plural = "confluentrolebindings"
    
    try:
        # List ConfluentRoleBindings across all namespaces
        confluent_role_bindings = api_client.list_cluster_custom_object(group, version, plural)
        print("List of ConfluentRoleBindings:")
        for crb in confluent_role_bindings['items']:
            print(f"- {crb['metadata']['name']}")
    except Exception as e:
        print(f"Error listing ConfluentRoleBindings: {e}")

def create_confluent_rolebinding_schema(name="", principal_name="", principal_type="", resource_id="", pattern_type="", resource_type="", role=""):
    config.load_incluster_config()
    api_client = client.CustomObjectsApi()
    crb_manifest = {
        "apiVersion": "platform.confluent.io/v1beta1",
        "kind": "ConfluentRolebinding",
        "metadata": {
            "name": name,
            "namespace": "confluent",
        },
        "spec": {
            "clustersScopeByIds": {
              "schemaRegistryClusterId": "id_bip-schemaregistry_confluent"
            },
            "principal": {
                "name": principal_name,
                "type": principal_type
            },
            "resourcePatterns": [{
                "name": resource_id,
                "patternType": pattern_type,
                "resourceType": resource_type
            }],
            "role": role
        }
    }
    try:
        # Create the ConfluentRoleBinding
        api_client.create_namespaced_custom_object(
            group="platform.confluent.io",
            version="v1beta1",
            namespace="confluent",
            plural="confluentrolebindings",
            body=crb_manifest,
        )
        print(f"ConfluentRoleBinding ({name}) created successfully.")
    except Exception as e:
        print(f"Error creating ConfluentRoleBinding: {e}")

def create_confluent_rolebinding(name="", principal_name="", principal_type="", resource_id="", pattern_type="", resource_type="", role=""):
    config.load_incluster_config()
    api_client = client.CustomObjectsApi()
    crb_manifest = {
        "apiVersion": "platform.confluent.io/v1beta1",
        "kind": "ConfluentRolebinding",
        "metadata": {
            "name": name,
            "namespace": "confluent",
        },
        "spec": {
            "principal": {
                "name": principal_name,
                "type": principal_type
            },
            "resourcePatterns": [{
                "name": resource_id,
                "patternType": pattern_type,
                "resourceType": resource_type
            }],
            "role": role
        }
    }
    
    try:
        # Create the ConfluentRoleBinding
        api_client.create_namespaced_custom_object(
            group="platform.confluent.io",
            version="v1beta1",
            namespace="confluent",
            plural="confluentrolebindings",
            body=crb_manifest,
        )
        print(f"ConfluentRoleBinding ({name}) created successfully.")
    except Exception as e:
        print(f"Error creating ConfluentRoleBinding: {e}")