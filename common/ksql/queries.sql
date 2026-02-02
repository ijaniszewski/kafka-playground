-- 1. Create a Stream from the Kafka Topic
-- This assumes the data in 'user_actions' is JSON formatted.
-- We must match the schema of the JSON in the producer.

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

-- 2. Simple Selection
-- Select all events where action is 'purchase'
-- Note: EMIT CHANGES is required for push queries in the CLI

SELECT * 
FROM user_actions_stream 
WHERE action = 'purchase' 
EMIT CHANGES;

-- 3. create a persistent Stream (Transformation)
-- Create a new Kafka topic 'purchases' populated by this query

CREATE STREAM purchases_stream AS
SELECT user_id, page_id, timestamp
FROM user_actions_stream
WHERE action = 'purchase';

-- 4. Aggregation (Table)
-- Count actions per user

CREATE TABLE user_action_counts AS
SELECT user_id, count(*) AS action_count
FROM user_actions_stream
GROUP BY user_id
EMIT CHANGES;

-- 5. Windowed Aggregation
-- Count actions per user in 1-minute windows

CREATE TABLE user_actions_per_minute AS
SELECT user_id, count(*) AS action_count
FROM user_actions_stream
WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY user_id
EMIT CHANGES;
