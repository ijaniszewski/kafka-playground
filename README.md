# Kafka Playground: CDC with Kafka

An educational project demonstrating Change Data Capture (CDC) patterns with Apache Kafka, featuring two main scenarios: basic stream processing and database CDC with Debezium.

## Project Structure

```
kafka-playground/
├── docker-compose.yml          # Infrastructure setup
├── requirements.txt            # Python dependencies
├── 01_producer/               # Scenario 1: Basic Kafka Producer
│   ├── producer.py           # Streams events from CSV to Kafka
│   └── data.csv              # Sample user action data
├── 02_cdc/                   # Scenario 2: Change Data Capture
│   ├── setup_postgres.py    # Initialize database and table
│   ├── register_connector.py # Register Debezium CDC connector
│   ├── consumer_cdc.py      # Consume CDC events from Kafka
│   └── insert_data.py       # Generate test data changes
└── common/                   # Shared resources
    └── ksql/                # ksqlDB queries for stream processing
        └── queries.sql
```

## Architecture

This setup runs the following services in Docker:

1.  **Broker (Kafka)**: The core message broker (running in KRaft mode, no Zookeeper).
    - Port: `9092` (exposed to host), `29092` (internal Docker network).
2.  **Schema Registry**: Manages schemas for structured data (Avro, Protobuf, etc.).
3.  **ksqlDB Server**: The engine that executes SQL queries against Kafka topics.
4.  **ksqlDB CLI**: Command-line interface to interact with the server.
5.  **AKHQ (Optional)**: A Web UI to view topics and messages.
    - URL: [http://localhost:8085](http://localhost:8085)

## Getting Started

### 1. Start the Environment

Run the following command in the root of the project:

```bash
docker-compose up -d
```

Wait for all services to start. You can check status with `docker-compose ps`.

### 2. Python Setup

Create and activate a virtual environment, then install dependencies:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

## Scenario 1: Basic Stream Processing

This scenario demonstrates fundamental Kafka concepts: producing events to a topic and processing them with ksqlDB.

### Step 1: Produce Data

Run the producer script to send user actions (views, clicks, purchases) to the `user_actions` topic:

```bash
python 01_producer/producer.py
```

### Step 2: Process with ksqlDB

Connect to the ksqlDB CLI:

```bash
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

Run queries from [common/ksql/queries.sql](common/ksql/queries.sql). For example, create a stream and filter specific actions:

```sql
CREATE STREAM user_actions_stream (
    user_id VARCHAR,
    page_id VARCHAR,
    action VARCHAR,
    timestamp VARCHAR
) WITH (
    KAFKA_TOPIC = 'user_actions',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 1
);

SELECT * FROM user_actions_stream WHERE action='purchase' EMIT CHANGES;
```

---

## Scenario 2: Change Data Capture (CDC) with Debezium

This scenario demonstrates CDC patterns - capturing real-time database changes and streaming them to Kafka using Debezium PostgreSQL connector.

### Step 1: Setup Database

Initialize the PostgreSQL database and create the `users` table with initial data:

```bash
python 02_cdc/setup_postgres.py
```

### Step 2: Register Debezium Connector

Configure Kafka Connect to monitor PostgreSQL changes:

```bash
python 02_cdc/register_connector.py
```

### Step 3: Start CDC Consumer

Listen to the Kafka topic (`dbserver1.public.users`) for database change events:

```bash
python 02_cdc/consumer_cdc.py
```

### Step 4: Modify Data

In a separate terminal, insert new records into PostgreSQL. Changes will appear immediately in the CDC consumer output:

```bash
python 02_cdc/insert_data.py
```

---

## Tools & UI

- **AKHQ Web UI**: [http://localhost:8085](http://localhost:8085) - View topics and messages.

## Cleaning Up

To stop and remove the containers:

```bash
docker-compose down -v
```
