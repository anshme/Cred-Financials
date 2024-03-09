from json import loads

from kafka import KafkaConsumer
import sys

def kafka_consumer(kafka_conf):
    consumer = KafkaConsumer(
        kafka_conf['topic'],
        bootstrap_servers=kafka_conf['bootstrap_server'],
        auto_offset_reset=kafka_conf['offset_reset'],
        enable_auto_commit=True,
        group_id=kafka_conf['group_id'],
        value_deserializer=lambda x: loads(x.decode('utf-8')))
    return consumer