# Common Resources

Shared resources used across different scenarios in this playground.

## Contents

### ksql/

Contains ksqlDB queries and stream processing examples.

- **queries.sql** - Sample ksqlDB queries demonstrating:
  - Stream creation from Kafka topics
  - Filtering and transformations
  - Aggregations and windowing
  - Creating derived streams and tables

## ksqlDB Overview

ksqlDB is a stream processing platform that lets you process Kafka data using SQL-like syntax. It's useful for:

- **Real-time transformations** - Filter, enrich, and transform streams
- **Aggregations** - Count events, calculate sums, averages over time windows
- **Joins** - Combine data from multiple Kafka topics
- **Materialized views** - Create queryable tables from streams

## Using ksqlDB

Connect to the CLI:

```bash
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

Then execute queries from `queries.sql` or write your own!

Example commands:

```sql
-- Show all topics
SHOW TOPICS;

-- List all streams
SHOW STREAMS;

-- List all tables
SHOW TABLES;

-- Describe a stream
DESCRIBE user_actions_stream;
```
