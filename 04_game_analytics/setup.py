#!/usr/bin/env python3
"""
Setup script for Game Analytics playground
Creates Kafka topics and initializes PostgreSQL schema
"""

import subprocess
import sys
import time
from pathlib import Path


def run_command(command, description, check=True):
    """Execute a shell command with error handling"""
    print(f"\n{'='*60}")
    print(f"🔧 {description}")
    print(f"{'='*60}")
    print(f"Command: {command}\n")

    result = subprocess.run(command, shell=True, capture_output=True, text=True)

    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)

    if check and result.returncode != 0:
        print(f"Failed: {description}")
        sys.exit(1)

    print(f"Success: {description}")
    return result


def check_docker():
    """Check if Docker is running"""
    result = subprocess.run("docker ps", shell=True, capture_output=True)
    return result.returncode == 0


def create_kafka_topics():
    """Create Kafka topics for gameplay and purchase events"""
    topics = [
        {
            "name": "puzzle_game_activity",
            "partitions": 3,
            "description": "Player activity events (session_start, puzzle_*, reward_claim)",
        },
        {
            "name": "puzzle_game_transactions",
            "partitions": 3,
            "description": "Transaction events (IAP purchases + refunds)",
        },
    ]

    for topic in topics:
        command = f"""docker exec -it broker kafka-topics --create \
            --bootstrap-server localhost:9092 \
            --topic {topic['name']} \
            --partitions {topic['partitions']} \
            --replication-factor 1 \
            --if-not-exists"""

        run_command(
            command,
            f"Creating topic: {topic['name']} ({topic['description']})",
            check=False,  # Don't fail if topic exists
        )


def create_database_schema():
    """Initialize PostgreSQL schema"""
    schema_file = Path(__file__).parent / "schema.sql"

    if not schema_file.exists():
        print(f"Schema file not found: {schema_file}")
        sys.exit(1)

    command = f"docker exec -i postgres psql -U postgres -d kafka_db < {schema_file}"
    run_command(command, "Creating database schema (Star Schema tables)")


def verify_setup():
    """Verify that everything is set up correctly"""
    print(f"\n{'='*60}")
    print("🔍 Verifying Setup")
    print(f"{'='*60}\n")

    # Check Kafka topics
    result = run_command(
        "docker exec broker kafka-topics --list --bootstrap-server localhost:9092",
        "Listing Kafka topics",
        check=False,
    )

    topics = result.stdout.strip().split("\n")
    required_topics = ["mobile_game_gameplay", "mobile_game_purchases"]

    for topic in required_topics:
        if topic in topics:
            print(f"Topic exists: {topic}")
        else:
            print(f"Topic missing: {topic}")

    # Check PostgreSQL tables
    command = """docker exec postgres psql -U postgres -d kafka_db -c "
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name;"
    """

    run_command(command, "Listing PostgreSQL tables", check=False)


def main():
    """Main setup routine"""
    print(
        """
╔═══════════════════════════════════════════════════════════╗
║  🎮 Game Analytics Setup - Crystal Quest                  ║
║  Q1: Data Model Design                                    ║
╚═══════════════════════════════════════════════════════════╝
    """
    )

    # Check Docker
    if not check_docker():
        print("Docker is not running. Please start Docker and try again.")
        print("   Run: docker-compose up -d")
        sys.exit(1)

    print("Docker is running")

    # Create Kafka topics
    try:
        create_kafka_topics()
    except Exception as e:
        print(f"Warning: Failed to create Kafka topics: {e}")
        print("   You may need to create them manually")

    # Create database schema
    try:
        create_database_schema()
    except Exception as e:
        print(f"Failed to create database schema: {e}")
        sys.exit(1)

    # Verify
    verify_setup()

    print(f"\n{'='*60}")
    print("Setup Complete!")
    print(f"{'='*60}\n")
    print("Next steps:")
    print("1. Run the producer:")
    print("   python producer.py --users 20 --sessions 3 --mode batch")
    print("\n2. Check the data:")
    print("   docker exec -it postgres psql -U postgres -d kafka_db")
    print("   kafka_db=# SELECT COUNT(*) FROM fact_session;")
    print("\n3. Run analysis queries from schema.sql")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nSetup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nSetup failed: {e}")
        sys.exit(1)
