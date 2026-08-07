import os
import json

from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import logging


logger = logging.getLogger(__name__)

KAFKA_SERVER = os.getenv('KAFKA_BROKERS', 'localhost:9092')
KAFKA_TOPICS = {
    'movie_events': 'movie-events',
    'user_events': 'user-events',
    'payment_events': 'payment-events'
}


def get_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1,
        )
        logger.info("Kafka producer created successfully")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        return None


def get_kafka_consumer(topic, group_id='events_service_group'):
    try:
        consumer = KafkaConsumer (
            topic,
            bootstrap_servers=KAFKA_SERVER,
            group_id=group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
        )
        logger.info(f"Kafka consumer created for topic {topic}")
        return consumer
    except Exception as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        return None


def create_topics_if_not_exist():
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_SERVER,
            client_id='events-service-admin'
        )

        existing_topic = admin_client.list_topics()
        topics_to_create = []

        for topic in KAFKA_TOPICS.values():
            if topic not in existing_topic:
                topics_to_create.append(
                    NewTopic(
                        name=topic,
                        num_partitions=2,
                        replication_factor=1
                    )
                )

        if topics_to_create:
            admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
            logger.info(f"Created topics: {[t.name for t in topics_to_create]}")
        else:
            logger.info("All topics already exist")

        admin_client.close()
    except Exception as e:
        logger.warning(f"Could not create topics: {e} (they may already exist)")
