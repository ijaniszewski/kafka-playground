# 02 - Change Data Capture (CDC)

This section demonstrates Change Data Capture with Debezium - capturing database changes and streaming them to Kafka in real-time.

> 📖 **Read the Article**: [CDC Explained: Streaming Database Changes with Kafka and Debezium](https://ijaniszewski.medium.com/cdc-explained-streaming-database-changes-with-kafka-and-debezium-cff3e03afae7?postPublishedType=initial)

## Files

- **setup.py** - One-command setup: creates database, registers connector, checks status
- **consumer_cdc.py** - Consumes CDC events from Kafka to verify data flow
- **generate_data.py** - Continuously generates random user data to trigger CDC events

## Concept

CDC (Change Data Capture) is a pattern that captures row-level changes in a database and publishes them as events. This enables:

- **Event-driven architectures** - React to data changes in real-time
- **Data replication** - Sync data across systems
- **Audit trails** - Track all database modifications
- **CQRS patterns** - Separate read and write models

## How It Works

1. **PostgreSQL WAL** - Database configured with `wal_level=logical` to track changes
2. **Debezium Connector** - Monitors the WAL and publishes changes to Kafka
3. **Kafka Topics** - Each table gets its own topic (e.g., `dbserver1.public.users`)
4. **Event Format** - Messages include before/after states and operation type (INSERT/UPDATE/DELETE)

## Running the Flow

### 1. One-Time Setup
```bash
python 02_cdc/setup.py
```

This single command:
- Creates the `users` table and inserts initial records
- Registers the Debezium connector with Kafka Connect
- Verifies connector status

### 2. Consume Events
```bash
python 02_cdc/consumer_cdc.py
```

Listens to the CDC topic and prints change events in real-time.

### 3. Generate Changes
In another terminal:
```bash
python 02_cdc/generate_data.py
```

Continuously inserts random users - you'll see them appear instantly in the consumer!

## Event Structure

Debezium produces events with this structure:

```json
{
  "payload": {
    "before": null,
    "after": {
      "id": 4,
      "name": "David Smith",
      "email": "david@example.com",
      "created_at": "2026-02-02T10:30:00Z"
    },
    "op": "c",
    "ts_ms": 1738493400000
  }
}
```

- **before**: Row state before change (null for INSERT)
- **after**: Row state after change (null for DELETE)
- **op**: Operation type (`c`=create, `u`=update, `d`=delete)
- **ts_ms**: Timestamp of the change
