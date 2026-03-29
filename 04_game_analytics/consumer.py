"""
Kafka Consumer for Game Analytics
Consumes gameplay and purchase events, processes them, and loads into PostgreSQL Star Schema
"""

import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from confluent_kafka import Consumer, KafkaError
import psycopg2
from psycopg2.extras import execute_values

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GameAnalyticsConsumer:
    """Consumer that processes game analytics events and loads them into PostgreSQL"""
    
    GAMEPLAY_TOPIC = "puzzle_game_activity"
    PURCHASE_TOPIC = "puzzle_game_transactions"
    
    def __init__(
        self,
        kafka_broker: str = "localhost:9092",
        db_host: str = "localhost",
        db_port: int = 5432,
        db_name: str = "kafka_db",
        db_user: str = "postgres",
        db_password: str = "postgres",
        group_id: str = "game-analytics-consumer-group"
    ):
        """Initialize consumer and database connection"""
        
        # Kafka consumer config
        self.consumer = Consumer({
            'bootstrap.servers': kafka_broker,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })
        
        # Subscribe to topics
        self.consumer.subscribe([self.GAMEPLAY_TOPIC, self.PURCHASE_TOPIC])
        logger.info(f"Subscribed to topics: {self.GAMEPLAY_TOPIC}, {self.PURCHASE_TOPIC}")
        
        # PostgreSQL connection
        self.conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        self.conn.autocommit = False
        logger.info(f"Connected to PostgreSQL: {db_name}@{db_host}:{db_port}")
        
        # Session tracking (in-memory aggregation)
        self.active_sessions = {}  # session_id -> session data
        
    def get_or_create_game_sk(self, game_id: str) -> int:
        """Get or create game surrogate key"""
        with self.conn.cursor() as cur:
            cur.execute("SELECT game_sk FROM dim_game WHERE game_id = %s", (game_id,))
            result = cur.fetchone()
            if result:
                return result[0]
            
            # Game should already exist from seed data
            logger.warning(f"Game not found: {game_id}")
            return 1  # Default to first game
    
    def get_or_create_device_sk(self, device_model: str, os: str, os_version: str) -> int:
        """Get or create device surrogate key"""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT device_sk FROM dim_device WHERE device_model = %s AND os = %s AND os_version = %s",
                (device_model, os, os_version)
            )
            result = cur.fetchone()
            if result:
                return result[0]
            
            # Insert new device
            cur.execute(
                """
                INSERT INTO dim_device (device_model, os, os_version, form_factor)
                VALUES (%s, %s, %s, %s)
                RETURNING device_sk
                """,
                (device_model, os, os_version, 'phone')
            )
            self.conn.commit()
            return cur.fetchone()[0]
    
    def get_or_create_user_sk(self, user_id: str, device_sk: int, country: str, event_ts: datetime) -> int:
        """Get or create user surrogate key (handles SCD Type 2)"""
        with self.conn.cursor() as cur:
            # Check if current record exists
            cur.execute(
                """
                SELECT user_sk, device_sk FROM dim_user 
                WHERE user_id = %s AND is_current = TRUE
                """,
                (user_id,)
            )
            result = cur.fetchone()
            
            if result:
                current_user_sk, current_device_sk = result
                
                # Check if device changed (SCD Type 2 trigger)
                if current_device_sk != device_sk:
                    logger.info(f"Device change detected for {user_id}: {current_device_sk} -> {device_sk}")
                    
                    # Close current record
                    cur.execute(
                        """
                        UPDATE dim_user 
                        SET is_current = FALSE, effective_end_date = %s
                        WHERE user_sk = %s
                        """,
                        (event_ts, current_user_sk)
                    )
                    
                    # Create new record
                    cur.execute(
                        """
                        INSERT INTO dim_user (
                            user_id, device_sk, country_code, first_seen_date,
                            effective_start_date, is_current
                        )
                        VALUES (%s, %s, %s, CURRENT_DATE, %s, TRUE)
                        RETURNING user_sk
                        """,
                        (user_id, device_sk, country, event_ts)
                    )
                    self.conn.commit()
                    return cur.fetchone()[0]
                
                return current_user_sk
            
            else:
                # New user
                cur.execute(
                    """
                    INSERT INTO dim_user (
                        user_id, device_sk, country_code, first_seen_date,
                        effective_start_date, is_current
                    )
                    VALUES (%s, %s, %s, CURRENT_DATE, %s, TRUE)
                    RETURNING user_sk
                    """,
                    (user_id, device_sk, country, event_ts)
                )
                self.conn.commit()
                return cur.fetchone()[0]
    
    def get_or_create_item_sk(self, item_id: str) -> int:
        """Get item surrogate key"""
        with self.conn.cursor() as cur:
            cur.execute("SELECT item_sk FROM dim_item WHERE item_id = %s", (item_id,))
            result = cur.fetchone()
            if result:
                return result[0]
            
            logger.warning(f"Item not found: {item_id}, using default")
            return 1
    
    def get_date_sk(self, ts: datetime) -> int:
        """Get or create date dimension key"""
        date_val = ts.date()
        date_sk = int(date_val.strftime('%Y%m%d'))
        
        with self.conn.cursor() as cur:
            cur.execute("SELECT date_sk FROM dim_date WHERE date_sk = %s", (date_sk,))
            if cur.fetchone():
                return date_sk
            
            # Insert if not exists
            cur.execute(
                """
                INSERT INTO dim_date (
                    date_sk, date_actual, day_of_week, day_name, day_of_month,
                    day_of_year, week_of_year, month_num, month_name, quarter, year,
                    is_weekend, is_holiday
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE
                )
                ON CONFLICT (date_sk) DO NOTHING
                """,
                (
                    date_sk, date_val, date_val.isoweekday(), date_val.strftime('%A'),
                    date_val.day, date_val.timetuple().tm_yday, date_val.isocalendar()[1],
                    date_val.month, date_val.strftime('%B'), (date_val.month - 1) // 3 + 1,
                    date_val.year, date_val.weekday() >= 5
                )
            )
            self.conn.commit()
            return date_sk
    
    def process_gameplay_event(self, event: Dict[str, Any]):
        """Process gameplay event (aggregate into sessions)"""
        session_id = event['session_id']
        event_type = event['event_type']
        event_ts = datetime.fromisoformat(event['event_ts'].replace('Z', ''))
        metadata = event['metadata']
        
        # Initialize session if new
        if session_id not in self.active_sessions:
            self.active_sessions[session_id] = {
                'session_id': session_id,
                'user_id': event['user_id'],
                'game_id': event['game_id'],
                'start_ts': event_ts,
                'end_ts': event_ts,
                'country': metadata['country'],
                'device_model': metadata['device_model'],
                'os': metadata['os'],
                'os_ver': metadata['os_ver'],
                'network_type': metadata['network_type'],
                'app_version': metadata['app_version'],
                'levels_started': 0,
                'levels_completed': 0,
                'levels_won': 0,
                'levels_lost': 0,
                'max_level_reached': 0,
                'ads_watched': 0,
            }
        
        session = self.active_sessions[session_id]
        
        # Update session based on event type
        session['end_ts'] = max(session['end_ts'], event_ts)
        
        if event_type == 'puzzle_start':
            session['levels_started'] += 1
        elif event_type == 'puzzle_complete':
            session['levels_completed'] += 1
            puzzle_id = metadata.get('puzzle_id', 0)
            session['max_level_reached'] = max(session['max_level_reached'], puzzle_id)
            if metadata.get('result') == 'solved':
                session['levels_won'] += 1
            else:
                session['levels_lost'] += 1
        elif event_type == 'reward_claim':
            session['ads_watched'] += 1
    
    def flush_session(self, session_id: str):
        """Write session to database"""
        if session_id not in self.active_sessions:
            return
        
        session = self.active_sessions[session_id]
        
        # Get dimension keys
        device_sk = self.get_or_create_device_sk(
            session['device_model'], session['os'], session['os_ver']
        )
        user_sk = self.get_or_create_user_sk(
            session['user_id'], device_sk, session['country'], session['start_ts']
        )
        game_sk = self.get_or_create_game_sk(session['game_id'])
        date_sk = self.get_date_sk(session['start_ts'])
        
        duration = int((session['end_ts'] - session['start_ts']).total_seconds())
        
        # Insert into fact_session
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO fact_session (
                    session_id, user_sk, game_sk, device_sk, start_date_sk,
                    session_start_ts, session_end_ts, duration_seconds,
                    levels_started, levels_completed, levels_won, levels_lost,
                    max_level_reached, ads_watched, country_code, network_type, app_version
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (session_id) DO UPDATE SET
                    session_end_ts = EXCLUDED.session_end_ts,
                    duration_seconds = EXCLUDED.duration_seconds,
                    levels_started = EXCLUDED.levels_started,
                    levels_completed = EXCLUDED.levels_completed,
                    levels_won = EXCLUDED.levels_won,
                    levels_lost = EXCLUDED.levels_lost,
                    max_level_reached = EXCLUDED.max_level_reached,
                    ads_watched = EXCLUDED.ads_watched,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    session['session_id'], user_sk, game_sk, device_sk, date_sk,
                    session['start_ts'], session['end_ts'], duration,
                    session['levels_started'], session['levels_completed'],
                    session['levels_won'], session['levels_lost'],
                    session['max_level_reached'], session['ads_watched'],
                    session['country'], session['network_type'], session['app_version']
                )
            )
            self.conn.commit()
        
        logger.debug(f"Flushed session {session_id} to database")
        del self.active_sessions[session_id]
    
    def process_purchase_event(self, event: Dict[str, Any]):
        """Process purchase event"""
        transaction_ts = datetime.fromisoformat(event['transaction_ts'].replace('Z', ''))
        
        # Get dimension keys
        device_sk = 1  # We don't have device info in purchase events
        user_sk = self.get_or_create_user_sk(event['user_id'], device_sk, event.get('country_code', 'US'), transaction_ts)
        game_sk = self.get_or_create_game_sk(event['game_id'])
        item_sk = self.get_or_create_item_sk(event['item_id'])
        date_sk = self.get_date_sk(transaction_ts)
        
        # Get session_sk if linked
        session_sk = None
        if 'session_id' in event:
            with self.conn.cursor() as cur:
                cur.execute("SELECT session_sk FROM fact_session WHERE session_id = %s", (event['session_id'],))
                result = cur.fetchone()
                if result:
                    session_sk = result[0]
        
        # Insert purchase
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO fact_purchase (
                    purchase_id, user_sk, game_sk, item_sk, device_sk, purchase_date_sk,
                    session_sk, transaction_ts, quantity, amount_local, amount_usd,
                    original_price_local, original_price_usd, is_refund, is_sandbox,
                    currency_code, platform, country_code, refund_reason
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    event['purchase_id'], user_sk, game_sk, item_sk, device_sk, date_sk,
                    session_sk, transaction_ts, event.get('quantity', 1),
                    event['price'], event['price_usd'],
                    event.get('original_price'), event.get('original_price'),
                    event['is_refund'], event['sandbox'],
                    event['currency'], event['platform'], event.get('country_code'),
                    event.get('refund_reason')
                )
            )
            self.conn.commit()
        
        logger.debug(f"Processed purchase {event['purchase_id']}")
    
    def run(self, max_messages: Optional[int] = None):
        """Run consumer loop"""
        logger.info("Starting consumer loop...")
        messages_processed = 0
        
        try:
            while True:
                if max_messages and messages_processed >= max_messages:
                    logger.info(f"Reached max messages limit: {max_messages}")
                    break
                
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    # Flush any remaining sessions after 5 seconds of inactivity
                    if self.active_sessions:
                        logger.info(f"Flushing {len(self.active_sessions)} active sessions...")
                        for session_id in list(self.active_sessions.keys()):
                            self.flush_session(session_id)
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        break
                
                # Process message
                try:
                    event = json.loads(msg.value().decode('utf-8'))
                    topic = msg.topic()
                    
                    if topic == self.GAMEPLAY_TOPIC:
                        self.process_gameplay_event(event)
                    elif topic == self.PURCHASE_TOPIC:
                        self.process_purchase_event(event)
                    
                    messages_processed += 1
                    
                    # Commit offset
                    self.consumer.commit(msg)
                    
                    if messages_processed % 100 == 0:
                        logger.info(f"Processed {messages_processed} messages")
                
                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)
        
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            # Flush remaining sessions
            logger.info(f"Flushing {len(self.active_sessions)} remaining sessions...")
            for session_id in list(self.active_sessions.keys()):
                self.flush_session(session_id)
            
            self.consumer.close()
            self.conn.close()
            logger.info(f"Consumer closed. Total messages processed: {messages_processed}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Game Analytics Kafka Consumer')
    parser.add_argument('--broker', default='localhost:9092', help='Kafka broker')
    parser.add_argument('--db-host', default='localhost', help='PostgreSQL host')
    parser.add_argument('--db-port', type=int, default=5432, help='PostgreSQL port')
    parser.add_argument('--db-name', default='kafka_db', help='Database name')
    parser.add_argument('--db-user', default='postgres', help='Database user')
    parser.add_argument('--db-password', default='postgres', help='Database password')
    parser.add_argument('--max-messages', type=int, help='Max messages to process (for testing)')
    
    args = parser.parse_args()
    
    consumer = GameAnalyticsConsumer(
        kafka_broker=args.broker,
        db_host=args.db_host,
        db_port=args.db_port,
        db_name=args.db_name,
        db_user=args.db_user,
        db_password=args.db_password
    )
    
    consumer.run(max_messages=args.max_messages)


if __name__ == "__main__":
    main()
