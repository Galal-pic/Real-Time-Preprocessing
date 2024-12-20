# producer.py
from kafka import KafkaProducer
import json
import logging
from .kafka_config import KAFKA_SERVER

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVER,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            key_serializer=lambda key: str(key).encode("utf-8"),
            acks="all",
            retries=5,
            max_in_flight_requests_per_connection=5,
            request_timeout_ms=10000,
            linger_ms=5,
            batch_size=32000,
        )
        logger.info("Kafka Producer created successfully")
        return producer
    except Exception as e:
        logger.error(f"Error creating Kafka producer: {e}")
        return None
