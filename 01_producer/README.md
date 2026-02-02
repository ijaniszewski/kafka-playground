# 01 - Basic Kafka Producer

This section demonstrates the fundamentals of Kafka: producing messages to a topic.

## Files

- **producer.py** - A Python script that reads user action data from CSV and streams it to Kafka
- **data.csv** - Sample dataset containing user actions (views, clicks, purchases)

## Concept

The producer reads sample data and publishes it to a Kafka topic called `user_actions`. This simulates real-time event streaming, which is the foundation of event-driven architectures.

## Running

Make sure Docker services are running, then:

```bash
python 01_producer/producer.py
```

The script will continuously replay the CSV data with random delays to simulate real-time events.

## What Happens

1. Reads CSV file with user action data
2. Adds current timestamp to each record
3. Serializes data to JSON
4. Sends to Kafka topic `user_actions` with user_id as the key
5. Waits randomly between 0.5-2 seconds between messages

This creates a stream of events that can be processed by consumers, transformed with ksqlDB, or used for analytics.
