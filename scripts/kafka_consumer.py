import argparse
import logging
from config.kafka import create_kafka_consumer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Consume messages from a Kafka topic.")
    parser.add_argument(
        "--topic_name",
        required=True,
        help="The name of the Kafka topic to consume messages from.",
    )
    parser.add_argument(
        "--group_id",
        required=False,
        default=None,
        help="The consumer group ID.",
    )
    return parser.parse_args()


def consume_messages(topic_name, group_id=None):
    consumer = create_kafka_consumer(topic_name, group_id)
    if consumer is None:
        logger.error(f"Failed to create Kafka consumer for topic: {topic_name}")
        return
    try:
        logger.info(f"Starting to consume messages from topic: {topic_name}")
        for message in consumer:
            try:
                # Print detailed message information
                logger.info(
                    f"Topic: {message.topic} Partition: {message.partition} Offset: {message.offset}"
                )
                logger.info(f"Key: {message.key}")
                logger.info(f"Value: {message.value}")
                logger.info("-" * 50)
            except Exception as e:
                logger.error(f"Error processing message: {e}")
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    args = parse_args()
    consume_messages(topic_name=args.topic_name, group_id=args.group_id)
