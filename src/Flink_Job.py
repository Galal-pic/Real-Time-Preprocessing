import argparse
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.data_stream import WatermarkStrategy
from config.flink.flink_config import KAFKA_FLINK_Connector
from config.flink.flink_source import Source, Sink
from config.flink.Map_function import BusinessRulesParser
from config.flink.database import db_connection


def kafka_job(source_topic, server_name, database, username, password, table, column):

    # Set up the Stream Execution Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(KAFKA_FLINK_Connector)

    # Set up the Kafka Source
    kafka_source = Source(topic_name=source_topic)

    # Create an input data stream from the Kafka source
    input_stream = env.from_source(
        kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="KafkaSource",
        type_info=Types.STRING(),
    )

    df_database = db_connection(
        server_name=server_name,
        database=database,
        username=username,
        password=password,
        table=table,
    )

    # Apply the BusinessRulesParser
    parsed_stream = input_stream.map(
        BusinessRulesParser(
            source_topic=source_topic,
            database=df_database,
            column=column,
        ),
        output_type=Types.LIST(Types.TUPLE([Types.STRING(), Types.STRING()])),
    )

    # Format the parsed stream into a string
    parsed_stream = parsed_stream.map(
        lambda value: f"{value[0][0]}, {value[0][1]}", output_type=Types.STRING()
    )

    # Print the parsed stream to the console
    parsed_stream.print()

    # Add a dynamic Kafka sink (commented out for now)
    # parsed_stream.sink_to(Sink())

    # Execute the job
    env.execute(f"Kafka Job - Reading from {source_topic}")


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Run a PyFlink Kafka job.")
    parser.add_argument(
        "--source_topic",
        required=True,
        help="The name of the Kafka source topic to read messages from.",
    )
    parser.add_argument(
        "--server_name",
        required=True,
        help="The name of the SQL Server.",
    )
    parser.add_argument(
        "--database",
        required=True,
        help="The name of the database.",
    )
    parser.add_argument(
        "--username",
        required=True,
        help="The username for SQL Server authentication.",
    )
    parser.add_argument(
        "--password",
        required=True,
        help="The password for SQL Server authentication.",
    )
    parser.add_argument(
        "--table",
        required=True,
    )
    parser.add_argument(
        "--column",
        required=True,
    )
    args = parser.parse_args()

    # Run the Kafka job with the provided arguments
    kafka_job(
        source_topic=args.source_topic,
        server_name=args.server_name,
        database=args.database,
        username=args.username,
        password=args.password,
        table=args.table,
        column=args.column,
    )
