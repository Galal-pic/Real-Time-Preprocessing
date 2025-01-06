import argparse
import logging
from pathlib import Path
from config.kafka import get_kafka_producer
from utils.data_generator import TransactionsData_json, send_orders

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Send JSON data to a Kafka topic.")
    parser.add_argument(
        "--topic_name",
        required=True,
        help="The name of the Kafka topic to which data will be sent.",
    )
    return parser.parse_args()


def main(topic):
    try:
        # Validate input
        if not topic.strip():
            raise ValueError("Topic name cannot be empty!")

        # Construct the file path dynamically
        file_path = Path("dataset") / f"{topic}.json"

        # Check if the dataset file exists
        if not file_path.exists():
            raise FileNotFoundError(f"Dataset file not found: {file_path}")

        # Load dataset
        fraudulent_dataset_generator = TransactionsData_json(file_path=file_path)
        producer = get_kafka_producer()

        # Send data to Kafka topic
        send_orders(
            producer=producer,
            topic_name=topic,
            data_generator=fraudulent_dataset_generator,
        )
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        raise


if __name__ == "__main__":
    args = parse_args()
    main(topic=args.topic_name)
