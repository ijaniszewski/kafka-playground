SET 'auto.offset.reset' = 'earliest';

CREATE STREAM IF NOT EXISTS user_actions_stream (
    user_id VARCHAR,
    page_id VARCHAR,
    action VARCHAR,
    timestamp VARCHAR
) WITH (
    KAFKA_TOPIC = 'user_actions',
    VALUE_FORMAT = 'JSON',
    PARTITIONS = 1
);

CREATE STREAM IF NOT EXISTS pageviews_stream AS
SELECT user_id, page_id, timestamp
FROM user_actions_stream
WHERE action = 'view';

CREATE TABLE IF NOT EXISTS user_action_counts AS
SELECT user_id, count(*) AS action_count
FROM user_actions_stream
GROUP BY user_id;
