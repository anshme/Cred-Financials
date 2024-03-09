from confluent_kafka import Consumer, KafkaException, KafkaError
import sys

def kafka_consumer(kafka_conf):
    conf = {
        'bootstrap.servers': kafka_conf['bootstrap_server'],  # Kafka broker address
        'group.id': kafka_conf['group_id'],        # Consumer group ID
        'auto.offset.reset': kafka_conf['offset_reset']         # Start from the earliest message available
    }

    consumer = Consumer(conf)
    consumer.subscribe(kafka_conf['topic'])  # Subscribe to one or more topics
    return consumer
