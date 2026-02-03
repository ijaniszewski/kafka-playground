# 03 - KSQLDB Query Streams

This module demonstrates how to use ksqlDB to process data streams in real-time.

> 📖 **Read the Article**: [Query Streams Explained: Transforming Kafka Data in Real-Time with ksqlDB](https://ijaniszewski.medium.com/query-streams-explained-transforming-kafka-data-in-real-time-with-ksqldb-4002ff3c5239)

## Prerequisites

Ensure your Kafka environment is running:

```bash
docker compose up -d
```

## Step 1: Start Producing Data

We need a data stream to query. We'll use the producer from the first module to populate the `user_actions` topic.

Open a new terminal and run:

```bash
# From the root of the project
python 01_producer/producer.py
```

Leave this running. It will generate random user events.

## Step 2: Connect to KSQL CLI

Open another terminal to access the ksqlDB command line interface:

```bash
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

You should see the ksqlDB logo and prompt.

## Step 3: Run Queries

You can execute the queries defined in `queries.sql` one by one.

### Check Incoming Data

First, verify that data is arriving in the raw Kafka topic:

```sql
PRINT 'user_actions';
```
Press `Ctrl+C` to stop the output.

### Create a Stream

Define the stream schema over the topic:

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
```

### Run a Push Query

See events in real-time filtering for 'view' actions:

```sql
SELECT * 
FROM user_actions_stream 
WHERE action = 'view' 
EMIT CHANGES;
```

### Create a Persistent Query (New Topic)

Create a derived stream that will be backed by a standard Kafka topic (`PAGEVIEWS_STREAM` mostly, or customized):

```sql
CREATE STREAM pageviews_stream AS
SELECT user_id, page_id, timestamp
FROM user_actions_stream
WHERE action = 'view';
```

Now you have a real-time ETL pipeline running!

### Aggregations (Tables)

Count actions per user dynamically:

```sql
CREATE TABLE user_action_counts AS
SELECT user_id, count(*) AS action_count
FROM user_actions_stream
GROUP BY user_id
EMIT CHANGES;
```

Select from the table:

```sql
SELECT * FROM user_action_counts EMIT CHANGES;
```
