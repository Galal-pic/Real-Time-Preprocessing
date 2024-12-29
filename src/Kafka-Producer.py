import os
import argparse
from config.kafka import get_kafka_producer
from .fraud_data_generator import TransactionsData_json, send_orders


def main(topic):
    # Validate input
    if not topic.strip():
        raise ValueError("Topic name cannot be empty!")

    # Construct the file path dynamically
    file_path = os.path.join("dataset", f"{topic}.json")

    # Check if the dataset file exists
    if not os.path.exists(file_path):
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


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Send JSON data to a Kafka topic.")
    parser.add_argument(
        "--topic_name",
        required=True,
        help="The name of the Kafka topic to which data will be sent.",
    )
    args = parser.parse_args()

    try:
        main(topic=args.topic_name)
    except FileNotFoundError as e:
        print(f"Error: {e}")
    except ValueError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
