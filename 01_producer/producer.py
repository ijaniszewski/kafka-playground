import csv
import json
import random
import socket
import time

from confluent_kafka import Producer

# Kafka Configuration
conf = {"bootstrap.servers": "localhost:9092", "client.id": socket.gethostname()}

producer = Producer(conf)

topic_name = "user_actions"


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def main():
    print(f"Starting producer. Sending data to topic: {topic_name}")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            with open("01_producer/data.csv", "r") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    # Update timestamp to current time for realism
                    row["timestamp"] = str(int(time.time()))

                    # Serialize to JSON
                    value = json.dumps(row)
                    key = row["user_id"]

                    producer.produce(
                        topic_name, key=key, value=value, callback=delivery_report
                    )

                    # Wait a bit to simulate real-time stream
                    sleep_time = random.uniform(0.5, 2.0)
                    print(f"Sent: {value} - sleeping {sleep_time:.2f}s")
                    time.sleep(sleep_time)

                    # Trigger callbacks
                    producer.poll(0)

            print("--- Replaying CSV data ---")

    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
