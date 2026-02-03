-- 0. Check data availability
-- Run this to verify data is arriving in the topic
PRINT 'user_actions';

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

-- 2. Simple Selection (Push Query)
-- Select all events where action is 'purchase'
-- Note: EMIT CHANGES is required for push queries in the CLI
SELECT * 
FROM user_actions_stream 
WHERE action = 'view' 
EMIT CHANGES;

-- 3. Persistent Stream (Transformation)
-- Create a new Kafka topic 'pageviews' populated by this query
CREATE STREAM pageviews_stream AS
SELECT user_id, page_id, timestamp
FROM user_actions_stream
WHERE action = 'view';

-- 4. Create Tables (Aggregations)
-- Count actions per user
CREATE TABLE user_action_counts AS
SELECT user_id, count(*) AS action_count
FROM user_actions_stream
GROUP BY user_id
EMIT CHANGES;
