import pandas as pd
import time
import logging
import json


def TransactionsData_CSV(file_path):
    df = pd.read_csv(file_path)
    for _, row in df.iterrows():
        yield row.to_dict()


def TransactionsData_json(file_path):
    with open(file_path, "r") as file:
        data = json.load(file)
        for line in data:
            yield line


def send_orders(producer, topic_name, data_generator):
    i = 0
    try:
        for i, fraud_data in enumerate(data_generator):
            producer.send(topic_name, key=str(i), value=fraud_data)
            print(f"Produced record {i} to topic '{topic_name}' with key '{i}'.")
            time.sleep(4)
            i = i + 1
    except Exception as e:
        logging.error(f"Error sending messages to Kafka: {e}")
    finally:
        producer.flush()
        producer.close()
