import pandas as pd
import time
import logging
import json

MCC_codes = pd.read_csv("dataset\mcc_codes.csv")


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
            converter = pd.DataFrame([fraud_data])
            merge_MCC = pd.merge(converter, MCC_codes, on="MCC", how="inner")
            if merge_MCC.shape[0] == 0:
                logging.warning(f"No MCC code found for MCC {fraud_data['MCC']}")
                print("-" * 50)
                continue
            else:
                producer.send(
                    topic_name, key=str(i), value=merge_MCC.to_dict(orient="records")
                )
                print(f"Produced record {i} to topic '{topic_name}' with key '{i}'.")
                print(merge_MCC.to_dict(orient="records"))
                print("-" * 50)

            time.sleep(4)
            i = i + 1
    except Exception as e:
        logging.error(f"Error sending messages to Kafka: {e}")
    finally:
        producer.flush()
        producer.close()
