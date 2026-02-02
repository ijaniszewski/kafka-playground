import json

from confluent_kafka import Consumer, KafkaException


def consume_cdc():
    conf = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "cdc_reader",
        "auto.offset.reset": "earliest",
    }

    consumer = Consumer(conf)
    topic = "dbserver1.public.users"

    print(f"Subscribing to {topic}...")
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            # Debezium sends complex keys/values.
            # If using JsonConverter with schemas=true (default), the payload is in msg.value()['payload'] usually,
            # but since we read raw bytes with generic Consumer, we need to decode.

            key = msg.key()
            value = msg.value()

            if key:
                print(f"Key: {key.decode('utf-8')}")

            if value:
                # Value is likely a JSON string if JsonConverter is used
                try:
                    val_json = json.loads(value.decode("utf-8"))
                    # Structure usually: {"schema": {...}, "payload": { "before": ..., "after": ..., "op": ... }}
                    payload = val_json.get("payload", {})
                    op = payload.get("op")
                    after = payload.get("after")
                    print(f"Operation: {op}, Data: {after}")
                except:
                    print(f"Value (raw): {value}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    consume_cdc()
