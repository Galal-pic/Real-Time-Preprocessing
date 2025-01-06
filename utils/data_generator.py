import pandas as pd
import time
import logging
import json
from pathlib import Path
from typing import Generator, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def TransactionsData_CSV(file_path: Path) -> Generator[Dict[str, Any], None, None]:
    df = pd.read_csv(file_path)
    for _, row in df.iterrows():
        yield row.to_dict()


def TransactionsData_json(file_path: Path) -> Generator[Dict[str, Any], None, None]:
    with open(file_path, "r") as file:
        data = json.load(file)
        for line in data:
            yield line


def send_orders(
    producer, topic_name: str, data_generator: Generator[Dict[str, Any], None, None]
) -> None:
    try:
        for i, fraud_data in enumerate(data_generator):
            producer.send(topic_name, key=str(i), value=fraud_data)
            logger.info(f"Produced record {i} to topic '{topic_name}'")
            logger.info(f"Data: {fraud_data}")
            time.sleep(4)
    except Exception as e:
        logger.error(f"Error sending messages to Kafka: {e}")
    finally:
        producer.flush()
        producer.close()
