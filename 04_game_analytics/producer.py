"""
Kafka Producer for Orbital Runner Game Analytics
Sends gameplay and purchase events to separate Kafka topics
"""

import json
import time
import logging
from datetime import datetime
from typing import List, Dict, Any
from confluent_kafka import Producer, KafkaException

from gameplay_generator import generate_user_sessions, GameplayEventGenerator
from purchase_generator import generate_purchases_for_sessions, simulate_device_change

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GameAnalyticsProducer:
    """Producer for sending game analytics events to Kafka"""
    
    GAMEPLAY_TOPIC = "puzzle_game_activity"
    PURCHASE_TOPIC = "puzzle_game_transactions"
    
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        """Initialize Kafka producer"""
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'game-analytics-producer',
            'acks': 'all',  # Wait for all replicas
            'compression.type': 'snappy',
            'linger.ms': 10,  # Batch messages for efficiency
            'batch.size': 16384
        }
        
        self.producer = Producer(self.config)
        logger.info(f"Producer initialized with broker: {bootstrap_servers}")
    
    def delivery_callback(self, err, msg):
        """Callback for message delivery confirmation"""
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(
                f"Message delivered to {msg.topic()} "
                f"[partition {msg.partition()}] at offset {msg.offset()}"
            )
    
    def send_gameplay_event(self, event: Dict[str, Any]):
        """Send gameplay event to Kafka"""
        try:
            # Use user_id as key for consistent partitioning (events for same user go to same partition)
            key = event['user_id']
            value = json.dumps(event)
            
            self.producer.produce(
                topic=self.GAMEPLAY_TOPIC,
                key=key.encode('utf-8'),
                value=value.encode('utf-8'),
                callback=self.delivery_callback
            )
            
        except BufferError:
            logger.warning("Local producer queue is full, waiting...")
            self.producer.poll(1)
            self.send_gameplay_event(event)  # Retry
        except Exception as e:
            logger.error(f"Error sending gameplay event: {e}")
    
    def send_purchase_event(self, event: Dict[str, Any]):
        """Send purchase event to Kafka"""
        try:
            # Use user_id as key for consistent partitioning
            key = event['user_id']
            value = json.dumps(event)
            
            self.producer.produce(
                topic=self.PURCHASE_TOPIC,
                key=key.encode('utf-8'),
                value=value.encode('utf-8'),
                callback=self.delivery_callback
            )
            
        except BufferError:
            logger.warning("Local producer queue is full, waiting...")
            self.producer.poll(1)
            self.send_purchase_event(event)  # Retry
        except Exception as e:
            logger.error(f"Error sending purchase event: {e}")
    
    def flush(self):
        """Flush all pending messages"""
        remaining = self.producer.flush(timeout=30)
        if remaining > 0:
            logger.warning(f"{remaining} messages were not delivered")
        else:
            logger.info("All messages delivered successfully")
    
    def close(self):
        """Close the producer"""
        self.flush()
        logger.info("Producer closed")


def simulate_realtime_stream(
    producer: GameAnalyticsProducer,
    gameplay_events: List[Dict[str, Any]],
    purchase_events: List[Dict[str, Any]],
    speed_multiplier: float = 100.0
):
    """
    Simulate real-time event stream by sending events in chronological order
    
    Args:
        producer: Kafka producer instance
        gameplay_events: List of gameplay events
        purchase_events: List of purchase events
        speed_multiplier: Speed up replay (100x = 1 hour becomes 36 seconds)
    """
    # Merge and sort all events by timestamp
    all_events = []
    for event in gameplay_events:
        all_events.append(('gameplay', event))
    for event in purchase_events:
        all_events.append(('purchase', event))
    
    all_events.sort(key=lambda x: x[1].get('event_ts') or x[1].get('transaction_ts'))
    
    if not all_events:
        logger.warning("No events to send")
        return
    
    logger.info(f"Starting to send {len(all_events)} events (gameplay: {len(gameplay_events)}, purchases: {len(purchase_events)})")
    logger.info(f"Speed multiplier: {speed_multiplier}x")
    
    # Get start time
    first_event = all_events[0][1]
    first_ts = datetime.fromisoformat(
        (first_event.get('event_ts') or first_event.get('transaction_ts')).replace('Z', '')
    )
    
    start_time = time.time()
    events_sent = 0
    
    for event_type, event in all_events:
        # Calculate when to send this event
        event_ts_str = event.get('event_ts') or event.get('transaction_ts')
        event_ts = datetime.fromisoformat(event_ts_str.replace('Z', ''))
        time_diff = (event_ts - first_ts).total_seconds()
        
        # Wait until it's time to send (adjusted by speed multiplier)
        target_time = start_time + (time_diff / speed_multiplier)
        now = time.time()
        if target_time > now:
            time.sleep(target_time - now)
        
        # Send event
        if event_type == 'gameplay':
            producer.send_gameplay_event(event)
        else:
            producer.send_purchase_event(event)
        
        events_sent += 1
        
        # Poll to handle callbacks
        producer.producer.poll(0)
        
        # Log progress every 100 events
        if events_sent % 100 == 0:
            logger.info(f"Sent {events_sent}/{len(all_events)} events")
    
    logger.info(f"Finished sending all {events_sent} events")


def send_batch(
    producer: GameAnalyticsProducer,
    gameplay_events: List[Dict[str, Any]],
    purchase_events: List[Dict[str, Any]]
):
    """Send all events as fast as possible (batch mode)"""
    logger.info(f"Sending {len(gameplay_events)} gameplay events...")
    for event in gameplay_events:
        producer.send_gameplay_event(event)
        producer.producer.poll(0)
    
    logger.info(f"Sending {len(purchase_events)} purchase events...")
    for event in purchase_events:
        producer.send_purchase_event(event)
        producer.producer.poll(0)
    
    logger.info("All events sent")


def main():
    """Main execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Game Analytics Kafka Producer')
    parser.add_argument(
        '--broker',
        default='localhost:9092',
        help='Kafka bootstrap servers (default: localhost:9092)'
    )
    parser.add_argument(
        '--users',
        type=int,
        default=20,
        help='Number of users to simulate (default: 20)'
    )
    parser.add_argument(
        '--sessions',
        type=int,
        default=3,
        help='Sessions per user (default: 3)'
    )
    parser.add_argument(
        '--mode',
        choices=['realtime', 'batch'],
        default='batch',
        help='Sending mode: realtime (simulated with delays) or batch (fast)'
    )
    parser.add_argument(
        '--speed',
        type=float,
        default=100.0,
        help='Speed multiplier for realtime mode (default: 100x)'
    )
    
    args = parser.parse_args()
    
    # Generate data
    logger.info(f"Generating activity data for {args.users} players with {args.sessions} sessions each...")
    gameplay_events = generate_user_sessions(
        num_users=args.users,
        sessions_per_user=args.sessions
    )
    
    logger.info("Generating purchase data...")
    purchase_events = generate_purchases_for_sessions(
        gameplay_events,
        purchase_probability=0.15,  # 15% of sessions result in purchase
        refund_rate=0.03  # 3% refund rate
    )
    
    # Simulate device change for one user (for SCD Type 2 testing)
    if args.users > 0:
        logger.info("Simulating device change for u_100001...")
        gameplay_events = simulate_device_change("u_100001", gameplay_events)
    
    # Initialize producer
    producer = GameAnalyticsProducer(bootstrap_servers=args.broker)
    
    try:
        if args.mode == 'realtime':
            simulate_realtime_stream(
                producer,
                gameplay_events,
                purchase_events,
                speed_multiplier=args.speed
            )
        else:
            send_batch(producer, gameplay_events, purchase_events)
        
        producer.flush()
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
    finally:
        producer.close()
    
    logger.info("Done!")


if __name__ == "__main__":
    main()
