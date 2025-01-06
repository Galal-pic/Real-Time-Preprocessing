from kafka import KafkaConsumer
from typing import Optional
from .kafka_config import KAFKA_SERVER
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_kafka_consumer(
    topic_name: str, group_id: Optional[str] = None
) -> Optional[KafkaConsumer]:
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=KAFKA_SERVER,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda x: x.decode("utf-8"),
            key_deserializer=lambda x: (x.decode("utf-8") if x else None),
            group_id=group_id,
        )
        logger.info(f"Kafka Consumer created for topic: {topic_name}")
        return consumer
    except Exception as e:
        logger.error(f"Error creating Kafka consumer for topic {topic_name}: {e}")
        return None
