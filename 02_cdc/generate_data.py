import time

import psycopg2
from faker import Faker

fake = Faker()


def generate_data():
    """Continuously generate and insert random user data."""
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="playground",
            user="postgres",
            password="postgres",
            port=5432,
        )
        cur = conn.cursor()

        print("Generating random data... (Ctrl+C to stop)")
        while True:
            name = fake.name()
            email = fake.email()

            print(f"Inserting {name} ({email})...")
            cur.execute(
                "INSERT INTO users (name, email) VALUES (%s, %s)", (name, email)
            )
            conn.commit()

            time.sleep(2)

    except KeyboardInterrupt:
        cur.close()
        conn.close()
        print("\nStopped.")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    generate_data()
