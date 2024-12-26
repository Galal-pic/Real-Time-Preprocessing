# Real-Time-Preprocessing

Directory structure:
└── Galal-pic-Real-Time-Preprocessing/
├── credit-card.ipynb
├── dataset/
│ ├── Transactions2.json
│ ├── Transactions&Rules.json
│ ├── test.json
│ └── Transactions.json
├── config/
│ ├── kafka/
│ │ ├── consumer.py
│ │ ├── **init**.py
│ │ ├── producer.py
│ │ └── kafka_config.py
│ └── flink/
│ ├── Map_function.py
│ ├── **init**.py
│ ├── flink_config.py
│ └── flink_source.py
├── flink-sql-connector-kafka-3.2.0-1.18.jar
├── README.md
└── src/
├── Flink_Job.py
├── fraud_data_generator.py
├── Kafka-Producer.py
└── Kafka-Consumer.py
