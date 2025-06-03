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

class UserEventProducer:
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
        logger.info(f"Producer initialized for topic: {topic}")

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

    def produce_events(self, num_events=100, delay=1):
        """Produce multiple events"""
        try:
            for i in range(num_events):
                user_id = f"user_{random.randint(1, 50)}"
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

class OrderEventProducer:
    def __init__(self, bootstrap_servers=['localhost:9092'], topic='order-events'):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            compression_type='gzip'
        )
        logger.info(f"Order producer initialized for topic: {topic}")

    def generate_order_event(self):
        """Generate a sample order event"""
        statuses = ['created', 'confirmed', 'shipped', 'delivered', 'cancelled']
        return {
            'order_id': f"order_{random.randint(10000, 99999)}",
            'user_id': f"user_{random.randint(1, 50)}",
            'status': random.choice(statuses),
            'timestamp': datetime.now().isoformat(),
            'items': [
                {
                    'product_id': f"prod_{random.randint(100, 999)}",
                    'quantity': random.randint(1, 5),
                    'price': round(random.uniform(10.0, 500.0), 2)
                }
                for _ in range(random.randint(1, 3))
            ],
            'total_amount': round(random.uniform(50.0, 1000.0), 2),
            'shipping_address': {
                'city': random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']),
                'state': random.choice(['NY', 'CA', 'IL', 'TX', 'AZ']),
                'zip_code': f"{random.randint(10000, 99999)}"
            }
        }

    def produce_orders(self, num_orders=50, delay=2):
        """Produce order events"""
        try:
            for i in range(num_orders):
                order = self.generate_order_event()
                
                future = self.producer.send(
                    self.topic,
                    key=order['order_id'],
                    value=order
                )
                
                future.add_callback(lambda metadata: logger.debug(f"Order sent to partition {metadata.partition}"))
                future.add_errback(lambda e: logger.error(f"Order send failed: {e}"))
                
                logger.info(f"Sent order {i+1}/{num_orders}: {order['order_id']} - {order['status']}")
                time.sleep(delay)
                
        except KeyboardInterrupt:
            logger.info("Order producer interrupted")
        except Exception as e:
            logger.error(f"Error producing orders: {e}")
        finally:
            self.producer.flush()
            self.producer.close()

def main():
    """Main function to demonstrate producers"""
    print("Kafka Producer Demo")
    print("1. User Events")
    print("2. Order Events")
    print("3. Both")
    
    choice = input("Choose option (1-3): ").strip()
    
    if choice == '1':
        producer = UserEventProducer()
        producer.produce_events(num_events=20, delay=0.5)
    elif choice == '2':
        producer = OrderEventProducer()
        producer.produce_orders(num_orders=10, delay=1)
    elif choice == '3':
        import threading
        
        # Run both producers in parallel
        user_producer = UserEventProducer()
        order_producer = OrderEventProducer()
        
        user_thread = threading.Thread(
            target=user_producer.produce_events, 
            kwargs={'num_events': 20, 'delay': 0.5}
        )
        order_thread = threading.Thread(
            target=order_producer.produce_orders, 
            kwargs={'num_orders': 10, 'delay': 1}
        )
        
        user_thread.start()
        order_thread.start()
        
        user_thread.join()
        order_thread.join()
    else:
        print("Invalid choice")

if __name__ == "__main__":
    main()