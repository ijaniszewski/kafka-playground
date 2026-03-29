# 04 - Game Analytics: Data Model Design (Q1)

> Read the Article: [Real-Time Game Analytics: Building a Star Schema Data Warehouse with Kafka](https://ijaniszewski.medium.com/real-time-game-analytics-building-a-star-schema-data-warehouse-with-kafka-a413f8bc556d)

## Business Scenario

**Crystal Quest** - A puzzle-solving mobile game. We have two event streams in Kafka:

1. **Player Activity Events** (`puzzle_game_activity`) - In-game events (sessions, puzzles, rewards)
2. **Transaction Events** (`puzzle_game_transactions`) - In-app purchases (IAP + refunds)

**Goal**: Design and implement an analytical model (Star Schema) for analyzing:
- Player engagement (play duration, retention)
- Monetization (revenue, conversion rate, refund rate)
- Player profiles (device types, countries, behavior patterns)

---

## Data Model Design - Star Schema

### Grain

#### Fact Tables
1. **`fact_session`** - **1 row per session**
   - Tracks engagement metrics (levels played, ads watched, duration)
   
2. **`fact_purchase`** - **1 row per transaction**
   - **IMPORTANT**: Refund is a separate row with the same `purchase_id`, but `is_refund = TRUE`
   - Enables "gross revenue" vs "net revenue" analysis

#### Dimension Tables
1. **`dim_user`** (SCD Type 2) - User profile change history
   - User changes phone? → New row with `effective_start_date`
   - Allows analysis like "this purchase was made on an old Android device"
   
2. **`dim_game`** - Game catalog
3. **`dim_item`** - IAP (in-app purchase) item catalog
4. **`dim_device`** - Device models (Snowflake schema element)
5. **`dim_date`** - Time dimension (for daily/weekly aggregations)

### Key Architectural Decisions

1. **Kafka Partitioning**: By `user_id`
   - Guarantees event ordering for each player
   - Enables joins in Kafka Streams/ksqlDB without repartitioning

2. **Linking Purchases to Sessions**
   - `fact_purchase.session_sk` (optional FK) - if purchase during session
   - Join by: `user_id` + timestamp within range `[session_start_ts, session_end_ts]`

3. **Currency Normalization**
   - `amount_local` - original currency (PLN, EUR, USD)
   - `amount_usd` - converted at transaction date exchange rate
   - **Why**: Global reporting requires common currency

4. **Refund Handling**
   - No UPDATE of original transaction
   - Send new event with `is_refund: true` and same `purchase_id`
   - **Pattern**: Event Sourcing (immutable log)

---

## File Structure

```
04_game_analytics/
├── gameplay_generator.py     # Activity event generator (session_start, puzzle_*, reward_claim)
├── purchase_generator.py     # Transaction event generator (IAP + refunds)
├── producer.py               # Kafka Producer - sends to two topics
├── consumer.py               # Kafka Consumer - ETL from Kafka to PostgreSQL
├── schema.sql                # Star Schema DDL (PostgreSQL)
├── setup.py                  # Setup script (create topics, tables)
└── README.md                 # This documentation
```

---
Quick Start

### Step 1: Start Infrastructure

Ensure Kafka and PostgreSQL are running (docker-compose from root directory):

```bash
cd /Users/gignac/Desktop/Projects/kafka-playground
docker compose up -d
```

### Step 2: Create Database

```bash
docker exec -i postgres psql -U postgres -c "CREATE DATABASE kafka_db;"
```

### Step 3: Create Kafka Topics & Database Schema

```bash
cd 04_game_analytics
python3 setup.py
```

This will:
- Create `puzzle_game_activity` and `puzzle_game_transactions` topics
- Initialize Star Schema tables in PostgreSQL

### Step 4: Install Python Dependencies

```bash
pip install confluent-kafka psycopg2-binary
```

### Step 5: Generate and Send Events

#### Batch Mode (fast - for testing)
```bash
python producer.py --users 20 --sessions 3 --mode batch
```

#### Realtime Mode (simulated with delays, 100x faster than reality)
```bash
python producer.py --users 50 --sessions 5 --mode realtime --speed 100
```

**Parameters**:
- `--users` - number of simulated users
- `--sessions` - number of sessions per user
- `--mode` - `batch` (fast) or `realtime` (with delays)
- `--speed` - speed multiplier (100 = 1 hour of data in 36 seconds)

### Step 6: Consume and Load to Database

```bash
python consumer.py
```

Or limit messages for testing:
```bash
python consumer.py --max-messages 500
```

The consumer will:
- Aggregate gameplay events into sessions
- HandSample Data

### Gameplay Event (`mobile_game_gameplay`)
```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": "u_102334",
  "session_id": "sess_a1b2c3d4e5f6",
  "game_id": "orbital_runner",
  "event_type": "session_start",
  "event_ts": "2026-02-06T23:16:06Z",
  "metadata": {
    "app_version": "2.11.3",
    "os": "ios",
    "os_ver": "17.4",
    "device_model": "iPhone15,2",
    "network_type": "wifi",
    "country": "PL"
  }
}
```

### Purchase Event (`mobile_game_purchases`)
```json
{
  "purchase_id": "tx_774400a1",
  "user_id": "u_102334",
  "game_id": "orbital_runner",
  "transaction_ts": "2026-02-06T23:18:42Z",
  "item_id": "com.orbital_runner.gem_pack_small",
  "item_type": "virtual_currency",
  "quantity": 1,
  "price": 4.99,
  "original_price": 7.99,
  "currency": "USD",
  "platform": "apple_app_store",
  "is_refund": false,
  "sandbox": false
}
```

---

## Verify Data

### Check Kafka Topics
```bash
# List topics
docker exec broker kafka-topics --list --bootstrap-server localhost:9092

# Peek at ing Scenarios

### Scenario 1: Device Change (SCD Type 2)
User `u_100001` changes phone mid-stream (Android → iOS).

**What we're testing**:
- Does `dim_user` have two rows for the same `user_id`?
- Are purchases linked to the correct profile version (by `effective_start_date`)?

**Test query**:
```sql
SELECT user_id, device_sk, effective_start_date, effective_end_date, is_current
FROM dim_user
WHERE user_id = 'u_100001'
ORDER BY effective_start_date;
```

Expected: 2 rows with different `device_sk` and different `effective_start_date`.

### Scenario 2: Refunds
~3% of transactions are refunded (5 min - 24h after purchase).

**What we're testing**:
- Do we have 2 rows in `fact_purchase` (original + refund)?
- Does `is_refund` flag work correctly?

**Test query**:
```sql
SELECT purchase_id, transaction_ts, amount_usd, is_refund
FROM fact_purchase
WHERE purchase_id IN (
  SELECT purchase_id FROM fact_purchase GROUP BY purchase_id HAVING COUNT(*) > 1
)
ORDER BY purchase_id, transaction_ts;
```

Expected: Pairs of rows with same `purchase_id`, one with `is_refund=false`, one with `is_refund=true`.

### Scenario 3: Revenue per Session
**Key business question**: Which session generated the most revenue?

**Query**:
```sql
SELECT * FROM v_session_with_revenue
WHERE session_revenue_usd > 0
ORDER BY session_revenue_usd DESC
LIMIT 10;
```ext Steps (Q2-Q4)

### Q2: SQL Queries
Writing analytical queries:
- Daily revenue by country
- User retention cohorts
- Conversion rate (sessions → purchases)
- Refund rate by item type
- Top spenders, whale analysis

### Q3: Data Pipeline (Advanced)
Advanced ETL/ELT implementation:
- Kafka Streams / ksqlDB - real-time session aggregation
- Time-based window joins (purchases → sessions)
- Automated SCD Type 2 handling
- Stream-table joins for enrichment

### Q4: Data Quality & Monitoring
- Schema validation (Avro/Protobuf with Schema Registry)
- Late-arriving data handling (watermarks)
- Duplicate detection and deduplication
- Data quality metrics (completeness, freshness)

---

## Testing Scenarios

### Scenario 1: Device Change (SCD Type 2)
User `u_100001` changes phone in the middle of the simulation (Android -> iOS).

**What we're testing**:
- Does `dim_user` contain two rows for the same `user_id`?
- Are purchases linked to the correct profile version (after `effective_start_date`)?

**Test query**:
```sql
SELECT user_id, device_sk, effective_start_date, effective_end_date, is_current
FROM dim_user
WHERE user_id = 'u_100001'
ORDER BY effective_start_date;
```

### Scenario 2: Refunds
~3% of transactions are refunded (5 min - 24h after purchase).

**What we're testing**:
- Do we have 2 rows in `fact_purchase` (original + refund)?
- Does the `is_refund` flag work correctly?

**Test query**:
```sql
SELECT purchase_id, transaction_ts, amount_usd, is_refund
FROM fact_purchase
WHERE purchase_id IN (
  SELECT purchase_id FROM fact_purchase GROUP BY purchase_id HAVING COUNT(*) > 1
)
ORDER BY purchase_id, transaction_ts;
```

### Scenario 3: Revenue per Session
**Key business question**: Which session generated the most revenue?

**Query**:
```sql
SELECT * FROM v_session_with_revenue
WHERE session_revenue_usd > 0
ORDER BY session_revenue_usd DESC
LIMIT 10;
```
Design Patterns Used

1. **Event Sourcing** - Refunds as separate events (no UPDATE)
2. **Slowly Changing Dimensions (SCD Type 2)** - Profile change history
3. **Surrogate Keys** - `user_sk`, `session_sk` instead of natural keys
4. **Partitioning by Entity** - `user_id` as partition key in Kafka
5. **Currency Normalization** - Always keep USD for global reporting
6. **Session Aggregation** - Stateful processing in consumer

---

## Common Pitfalls

1. **Refunds in Revenue Queries**
   ```sql
   -- WRONG (counts refunds as revenue)
   SELECT SUM(amount_usd) FROM fact_purchase;
   
   -- CORRECT (net revenue)
   SELECT SUM(CASE WHEN is_refund THEN -amount_usd ELSE amount_usd END)
   FROM fact_purchase
   WHERE NOT is_sandbox;
   ```

2. **SCD Type 2 Joins**
   ```sql
   -- CORRECT (join with snapshot timestamp)
   SELECT u.* FROM dim_user u
   JOIN fact_purchase p ON p.user_sk = u.user_sk
   WHERE u.effective_start_date <= p.transaction_ts
     AND (u.effective_end_date IS NULL OR u.effective_end_date > p.transaction_ts);
   ```

3. **Late-Arriving Purchases**
   - Purchase may arrive after session is closed
   - Need to backfill `fact_purchase.session_sk`
   - Consider using event timestamp for processing time

4. **Session Timeout**
   - Consumer flushes sessions after inactivity
   - Adjust timeout based on expected user behavior
   - Consider using Kafka Streams for proper windowing

---

## Architecture Overview

```
┌─────────────────┐
│  Game Clients   │
│  (iOS/Android)  │
└────────┬────────┘
         │ Events
         ↓
┌─────────────────┐
│    Producer     │ Generates synthetic events
│  (Python)       │ Partitioned by user_id
└────────┬────────┘
         │
         ↓
┌─────────────────────────────────────┐
│          Apache Kafka               │
│  ┌─────────────────────────────┐   │
│  │ mobile_game_gameplay (P:3)  │   │
│  │ mobile_game_purchases (P:3) │   │
│  └─────────────────────────────┘   │
└────────┬────────────────────────────┘
         │
         ↓
┌─────────────────┐
│    Consumer     │ ETL: Aggregate & Transform
│  (Python)       │ SCD Type 2 handling
└────────┬────────┘
         │
         ↓
┌─────────────────────────────────────┐
│        PostgreSQL                   │
│  ┌─────────────────────────────┐   │
│  │  Star Schema:               │   │
│  │  - fact_session             │   │
│  │  - fact_purchase            │   │
│  │  - dim_user (SCD Type 2)    │   │
│  │  - dim_game, dim_item, ...  │   │
│  └─────────────────────────────┘   │
└─────────────────────────────────────┘
         │
         ↓
┌─────────────────┐
│   Analytics     │ BI Tools, SQL queries
│   & Reporting   │ Dashboards
└─────────────────┘
```

---

## Additional Resources

- [Kimball Dimensional Modeling](https://www.kimballgroup.com/)
- [Kafka Streams Windowing](https://kafka.apache.org/documentation/streams/)
- [SCD Type 2 Best Practices](https://en.wikipedia.org/wiki/Slowly_changing_dimension)
- [Event Sourcing Pattern](https://martinfowler.com/eaaDev/EventSourcing.html)

---

**Author**: Created as a data engineering interview challenge simulation  
**Date**: 2026-02-07  
**Task**: Q1 - Data Model Design (Mobile Game Analytics)
