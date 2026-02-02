import json
import time

import psycopg2
import requests


def setup_database():
    """Create database table and insert initial data."""
    print("\n=== Setting up PostgreSQL ===")
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="playground",
            user="postgres",
            password="postgres",
            port=5432,
        )
        cur = conn.cursor()

        # Create table
        print("Creating table users...")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                email VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Insert some initial data
        print("Inserting initial data...")
        users = [
            ("Alice", "alice@example.com"),
            ("Bob", "bob@example.com"),
            ("Charlie", "charlie@example.com"),
        ]

        for name, email in users:
            cur.execute(
                "INSERT INTO users (name, email) VALUES (%s, %s)", (name, email)
            )

        conn.commit()
        cur.close()
        conn.close()
        print("✓ Database setup complete.")
        return True

    except Exception as e:
        print(f"✗ Error setting up database: {e}")
        return False


def register_connector():
    """Register Debezium connector with Kafka Connect."""
    print("\n=== Registering Debezium Connector ===")
    url = "http://localhost:8084/connectors"
    headers = {"Content-Type": "application/json"}

    config = {
        "name": "users-connector",
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": "postgres",
            "database.port": "5432",
            "database.user": "postgres",
            "database.password": "postgres",
            "database.dbname": "playground",
            "topic.prefix": "dbserver1",
            "plugin.name": "pgoutput",
            "table.include.list": "public.users",
        },
    }

    try:
        # Check if already exists
        response = requests.get(f"{url}/users-connector")
        if response.status_code == 200:
            print("Connector already exists. Deleting...")
            requests.delete(f"{url}/users-connector")
            time.sleep(2)

        print("Registering connector...")
        response = requests.post(url, headers=headers, data=json.dumps(config))

        if response.status_code in [201, 200]:
            print("✓ Connector registered successfully.")
            return True
        else:
            print(f"✗ Failed to register connector. Status: {response.status_code}")
            print(response.text)
            return False

    except Exception as e:
        print(f"✗ Error registering connector: {e}")
        return False


def check_connector_status():
    """Check the status of the connector."""
    print("\n=== Checking Connector Status ===")
    try:
        response = requests.get(
            "http://localhost:8084/connectors/users-connector/status"
        )
        if response.status_code == 200:
            status = response.json()
            connector_state = status.get("connector", {}).get("state")
            print(f"Connector state: {connector_state}")

            tasks = status.get("tasks", [])
            for task in tasks:
                task_id = task.get("id")
                task_state = task.get("state")
                print(f"  Task {task_id}: {task_state}")
            return True
        else:
            print(f"Could not check status: {response.status_code}")
            return False
    except Exception as e:
        print(f"Error checking status: {e}")
        return False


def main():
    print("=" * 50)
    print("CDC Setup Script")
    print("=" * 50)

    # Step 1: Setup database
    if not setup_database():
        print("\n❌ Setup failed at database configuration.")
        return

    # Step 2: Register connector
    time.sleep(2)  # Give database a moment
    if not register_connector():
        print("\n❌ Setup failed at connector registration.")
        return

    # Step 3: Check status
    time.sleep(3)  # Give connector time to initialize
    check_connector_status()

    print("\n" + "=" * 50)
    print("✓ Setup complete!")
    print("\nNext steps:")
    print("  1. Run: python 02_cdc/consumer_cdc.py")
    print("  2. In another terminal: python 02_cdc/generate_data.py")
    print("=" * 50)


if __name__ == "__main__":
    main()
