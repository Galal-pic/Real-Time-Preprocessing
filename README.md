# Real-Time with Kafka and Flink

This project is a real-time system that uses **Apache Kafka** for data streaming and **Apache Flink** for stream processing. It processes transaction data, enriches it with business rules, and detects fraudulent activities in real time.

---

## **Project Overview**

### **Key Components**

1. **Kafka Producer**:

   - Reads transaction data from a JSON file.
   - Sends the data to a Kafka topic for processing.

2. **Kafka Consumer**:

   - Consumes messages from a Kafka topic.
   - Prints the messages to the console for debugging.

3. **Flink Job**:

   - Processes transaction data from a Kafka topic.
   - Enriches transactions with business rules (json) and external data (e.g., MCC codes) SQL server database.
   - Detects fraudulent activities based on predefined rules.

4. **Business Rules**:

   - Defined in a JSON file (`dataset/Rules.json`).
   - Applied to transactions to determine if they are fraudulent.

5. **External Data**:

   - MCC codes are loaded from a SQL DB file.
   - Customer profiles are fetched from **Redis**.

6. **Database Integration**:
   - Connects to a SQL Server database to fetch additional data for enrichment.

---

## **Project Structure**

```bash
project/
├── config/
│ ├── init.py
│ ├── kafka/
│ │ ├── init.py
│ │ ├── consumer.py
│ │ ├── producer.py
│ │ └── kafka_config.py
│ ├── flink/
│ │ ├── init.py
│ │ ├── flink_config.py
│ │ ├── flink_source.py
│ │ ├── database.py
│ │ ├── Enrich.py
│ │ ├── map_function.py
│ │ └── test_functions.py
├── dataset/
│ ├── mcc_codes.csv
│ ├── Rules.json
│ └── CreditCardTransaction.json
| └── OnlineTransaction.json
├── jobs/
│ ├── init.py
│ └── flink_job.py
├── scripts/
│ ├── kafka_producer.py
│ └── kafka_consumer.py
├── utils/
│ ├── init.py
│ └── fraud_data_generator.py
└── README.md
```

---

## **Setup Instructions**

### **Prerequisites**

1. **Python 3.11.2**
2. **Apache Kafka** (running locally)
3. **Redis** (for customer profiles)
4. **SQL Server** (for additional data)
5. **Apache Flink** (for stream processing)
6. **Java** 11.0.25

### **Install Dependencies**

1. Clone the repository:

   ```bash
   git clone https://github.com/Galal-pic/Real-Time-Preprocessing.git
   cd Real-Time-Preprocessing

   ```

2. Install the required Python packages:
   ```bash
   pip install -r requirements.txt
   ```

### **Configure Kafka**

```bash
 - KAFKA_SERVER = "localhost:9092"
```

### **Flink Kafka connector**

```bash
change the path
KAFKA_FLINK_Connector = "file:///path/to/flink-sql-connector-kafka-3.2.0-1.18.jar"
```

### **Database Kafka**

```bash
server_name = "DESKTOP-5DG0EQD\SQLEXPRESS"
database = "SIA"
username = "Galal"
password = "123456789"
```

### **Redis Configuration**

```bash
REDIS_HOST = "localhost"
REDIS_PORT = 6379
```

---

### **Running the Project**

Ensure Kafka is running. You can start a local Kafka server using the following commands:

1. **Start Kafka**
   Ensure Kafka is running. You can start a local Kafka server using the following commands:

```bash
.\bin\windows\kafka-storage.bat format -t k5hJ6itRQHaTE-apfE0HjQ  -c .\config\kraft\server.properties

.\bin\windows\kafka-server-start.bat .\config\kraft\server.properties
```

2. **Run the Kafka Producer**
   Send transaction data to a Kafka topic:

```bash
py -m scripts.kafka_producer --topic_name CreditCardTransaction

py -m scripts.kafka_producer --topic_name OnlineTransaction
```

3. **Run the Flink Job**

```bash
 py -m jobs.Flink_Job --source_topic OnlineTransaction --server_name DESKTOP-5DG0EQD\SQLEXPRESS --database SIA --username Galal --password 123456789 --table MCC_Categories --column MCC
```
