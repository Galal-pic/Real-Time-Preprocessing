from config.kafka import create_kafka_consumer


def consume_messages(topic_name, group_id=None):
    consumer = create_kafka_consumer(topic_name)
    if consumer is None:
        return
    try:
        print(f"Starting to consume messages from topic: {topic_name}")
        for message in consumer:
            try:
                # Print detailed message information
                print(
                    f"Topic: {message.topic} Partition: {message.partition} Offset: {message.offset}"
                )
                print(f"Key: {message.key}")
                print(f"Value: {message.value}")
                print("-" * 50)
            except Exception as e:
                print(f"Error processing message: {e}")

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    topic_name = "Luxury"
    consume_messages(topic_name)
