import argparse
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.data_stream import WatermarkStrategy
from config.flink.flink_config import KAFKA_FLINK_Connector
from config.flink.flink_source import Source, Sink
from config.flink.map_function import BusinessRulesParser
from config.flink.database import db_connection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FlinkKafkaJob:
    def __init__(
        self, source_topic, server_name, database, username, password, table, column
    ):
        self.source_topic = source_topic
        self.server_name = server_name
        self.database = database
        self.username = username
        self.password = password
        self.table = table
        self.column = column

    def run(self):
        # Set up the Stream Execution Environment
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(1)
        env.add_jars(KAFKA_FLINK_Connector)

        # Set up the Kafka Source
        kafka_source = Source(topic_name=self.source_topic)

        # Create an input data stream from the Kafka source
        input_stream = env.from_source(
            kafka_source,
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="KafkaSource",
            type_info=Types.STRING(),
        )

        # Fetch data from the database
        df_database = db_connection(
            server_name=self.server_name,
            database=self.database,
            username=self.username,
            password=self.password,
            table=self.table,
        )

        # Apply the BusinessRulesParser
        parsed_stream = input_stream.map(
            BusinessRulesParser(
                source_topic=self.source_topic,
                database=df_database,
                column=self.column,
            ),
            output_type=Types.LIST(Types.TUPLE([Types.STRING(), Types.STRING()])),
        )

        # Format the parsed stream into a string
        parsed_stream = parsed_stream.map(
            lambda value: f"{value[0][0]}, {value[0][1]}", output_type=Types.STRING()
        )

        # Print the parsed stream to the console
        parsed_stream.print()

        # Execute the job
        env.execute(f"Kafka Job - Reading from {self.source_topic}")


def parse_args():
    parser = argparse.ArgumentParser(description="Run a PyFlink Kafka job.")
    parser.add_argument("--source_topic", required=True, help="Kafka source topic.")
    parser.add_argument("--server_name", required=True, help="SQL Server name.")
    parser.add_argument("--database", required=True, help="Database name.")
    parser.add_argument("--username", required=True, help="SQL Server username.")
    parser.add_argument("--password", required=True, help="SQL Server password.")
    parser.add_argument("--table", required=True, help="Table name.")
    parser.add_argument("--column", required=True, help="Column name.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    job = FlinkKafkaJob(
        source_topic=args.source_topic,
        server_name=args.server_name,
        database=args.database,
        username=args.username,
        password=args.password,
        table=args.table,
        column=args.column,
    )
    job.run()
