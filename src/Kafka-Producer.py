from config.kafka import get_kafka_producer
from .fraud_data_generator import TransactionsData_json, send_orders


def main():
    # load_dataset
    topic_name = "t1"
    file_path = r"dataset\Transactions.json"
    fraudulent_dataset_generator = TransactionsData_json(file_path=file_path)
    producer = get_kafka_producer()

    # !create a new topic (if not already created)
    # create_kafka_topic(topic_name=topic_name,num_partitions=3,replication_factor=1)
    send_orders(
        producer=producer,
        topic_name=topic_name,
        data_generator=fraudulent_dataset_generator,
    )


if __name__ == "__main__":
    main()
