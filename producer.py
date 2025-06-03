import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleKafkaProducer:
    def __init__(self, bootstrap_servers=['localhost:9092'], topic='user-events'):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Wait for all replicas to acknowledge
            retries=3,
            retry_backoff_ms=1000,
            max_in_flight_requests_per_connection=1,
            enable_idempotence=True,
            compression_type='gzip'
        )
        logger.info(f"Producer initialized for topic: {self.topic}")

    def generate_user_event(self, user_id):
        """Generate a sample user event"""
        events = ['login', 'logout', 'purchase', 'view_product', 'add_to_cart', 'search']
        return {
            'user_id': user_id,
            'event_type': random.choice(events),
            'timestamp': datetime.now().isoformat(),
            'session_id': f"session_{random.randint(1000, 9999)}",
            'metadata': {
                'ip_address': f"192.168.1.{random.randint(1, 255)}",
                'user_agent': 'Mozilla/5.0 (compatible; KafkaBot/1.0)',
                'product_id': random.randint(100, 999) if random.choice([True, False]) else None
            }
        }

    def produce_events(self, num_events=50, delay=1):
        """Produce multiple events"""
        try:
            logger.info(f"Starting to produce {num_events} events to topic '{self.topic}'")
            
            for i in range(num_events):
                user_id = f"user_{random.randint(1, 20)}"
                event = self.generate_user_event(user_id)
                
                # Send message with key for partitioning
                future = self.producer.send(
                    self.topic,
                    key=user_id,
                    value=event
                )
                
                # Add callback for success/error handling
                future.add_callback(self.on_send_success)
                future.add_errback(self.on_send_error)
                
                logger.info(f"Sent event {i+1}/{num_events}: {event['event_type']} for {user_id}")
                time.sleep(delay)
                
        except KeyboardInterrupt:
            logger.info("Producer interrupted by user")
        except Exception as e:
            logger.error(f"Error producing events: {e}")
        finally:
            self.close()

    def on_send_success(self, record_metadata):
        """Callback for successful message delivery"""
        logger.debug(f"Message delivered to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")

    def on_send_error(self, excp):
        """Callback for message delivery errors"""
        logger.error(f"Failed to deliver message: {excp}")

    def close(self):
        """Close the producer"""
        self.producer.flush()
        self.producer.close()
        logger.info("Producer closed")

def main():
    """Main function to run the producer"""
    print("Simple Kafka Producer")
    print("=" * 30)
    
    # Get user input for configuration
    try:
        num_events = int(input("Enter number of events to produce (default 20): ") or "20")
        delay = float(input("Enter delay between events in seconds (default 1): ") or "1")
    except ValueError:
        print("Using default values...")
        num_events = 20
        delay = 1
    
    # Create and run producer
    producer = SimpleKafkaProducer()
    producer.produce_events(num_events=num_events, delay=delay)

if __name__ == "__main__":
    main()