import json
import time
from collections import defaultdict
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import logging
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class UserEventConsumer:
    def __init__(self, bootstrap_servers=['localhost:9092'], topics=['user-events'], group_id='user-events-group'):
        self.topics = topics
        self.group_id = group_id
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',  # Start from beginning if no offset
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            consumer_timeout_ms=1000  # Timeout after 1 second of no messages
        )
        
        # Analytics storage
        self.event_counts = defaultdict(int)
        self.user_activity = defaultdict(list)
        
        logger.info(f"User event consumer initialized for topics: {topics}, group: {group_id}")

    def process_message(self, message):
        """Process individual user event message"""
        try:
            event = message.value
            user_id = event.get('user_id')
            event_type = event.get('event_type')
            timestamp = event.get('timestamp')
            
            # Update analytics
            self.event_counts[event_type] += 1
            self.user_activity[user_id].append({
                'event': event_type,
                'timestamp': timestamp
            })
            
            logger.info(f"Processed: {event_type} from {user_id} at {timestamp}")
            
            # Simulate processing time
            time.sleep(0.1)
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def consume_events(self, max_messages=None):
        """Consume user events"""
        message_count = 0
        try:
            logger.info("Starting to consume user events...")
            
            for message in self.consumer:
                self.process_message(message)
                message_count += 1
                
                if max_messages and message_count >= max_messages:
                    break
                    
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
        finally:
            self.close()

    def print_analytics(self):
        """Print analytics summary"""
        print("\n" + "="*50)
        print("USER EVENT ANALYTICS")
        print("="*50)
        
        print("\nEvent Type Distribution:")
        for event_type, count in sorted(self.event_counts.items()):
            print(f"  {event_type}: {count}")
        
        print(f"\nTotal Users Active: {len(self.user_activity)}")
        print(f"Total Events Processed: {sum(self.event_counts.values())}")
        
        # Top active users
        user_event_counts = {user: len(events) for user, events in self.user_activity.items()}
        top_users = sorted(user_event_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        
        print("\nTop 5 Active Users:")
        for user, count in top_users:
            print(f"  {user}: {count} events")

    def close(self):
        """Close the consumer"""
        self.consumer.close()
        self.print_analytics()
        logger.info("User event consumer closed")

class OrderEventConsumer:
    def __init__(self, bootstrap_servers=['localhost:9092'], topics=['order-events'], group_id='order-events-group'):
        self.topics = topics
        self.group_id = group_id
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            consumer_timeout_ms=1000
        )
        
        # Order analytics
        self.status_counts = defaultdict(int)
        self.total_revenue = 0.0
        self.city_orders = defaultdict(int)
        self.processed_orders = []
        
        logger.info(f"Order consumer initialized for topics: {topics}, group: {group_id}")

    def process_order(self, message):
        """Process individual order message"""
        try:
            order = message.value
            order_id = order.get('order_id')
            status = order.get('status')
            total_amount = order.get('total_amount', 0)
            city = order.get('shipping_address', {}).get('city', 'Unknown')
            
            # Update analytics
            self.status_counts[status] += 1
            self.city_orders[city] += 1
            self.processed_orders.append(order_id)
            
            if status in ['confirmed', 'delivered']:
                self.total_revenue += total_amount
            
            logger.info(f"Processed order: {order_id} - {status} - ${total_amount}")
            
            # Simulate order processing
            time.sleep(0.2)
            
        except Exception as e:
            logger.error(f"Error processing order: {e}")

    def consume_orders(self, max_messages=None):
        """Consume order events"""
        message_count = 0
        try:
            logger.info("Starting to consume order events...")
            
            for message in self.consumer:
                self.process_order(message)
                message_count += 1
                
                if max_messages and message_count >= max_messages:
                    break
                    
        except KeyboardInterrupt:
            logger.info("Order consumer interrupted")
        except Exception as e:
            logger.error(f"Error consuming orders: {e}")
        finally:
            self.close()

    def print_analytics(self):
        """Print order analytics"""
        print("\n" + "="*50)
        print("ORDER ANALYTICS")
        print("="*50)
        
        print("\nOrder Status Distribution:")
        for status, count in sorted(self.status_counts.items()):
            print(f"  {status}: {count}")
        
        print(f"\nTotal Revenue: ${self.total_revenue:.2f}")
        print(f"Total Orders Processed: {len(self.processed_orders)}")
        
        print("\nTop Cities by Order Count:")
        top_cities = sorted(self.city_orders.items(), key=lambda x: x[1], reverse=True)[:5]
        for city, count in top_cities:
            print(f"  {city}: {count} orders")

    def close(self):
        """Close the consumer"""
        self.consumer.close()
        self.print_analytics()
        logger.info("Order consumer closed")

class MultiTopicConsumer:
    def __init__(self, bootstrap_servers=['localhost:9092'], group_id='multi-topic-group'):
        self.consumer = KafkaConsumer(
            'user-events', 'order-events',
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            consumer_timeout_ms=1000
        )
        
        self.message_counts = defaultdict(int)
        logger.info(f"Multi-topic consumer initialized, group: {group_id}")

    def consume_all(self, max_messages=None):
        """Consume from multiple topics"""
        message_count = 0
        try:
            logger.info("Starting multi-topic consumption...")
            
            for message in self.consumer:
                topic = message.topic
                self.message_counts[topic] += 1
                
                logger.info(f"Received from {topic}: {message.key} at partition {message.partition}")
                
                message_count += 1
                if max_messages and message_count >= max_messages:
                    break
                    
        except KeyboardInterrupt:
            logger.info("Multi-topic consumer interrupted")
        finally:
            self.close()

    def close(self):
        """Close consumer and print stats"""
        self.consumer.close()
        
        print("\n" + "="*50)
        print("MULTI-TOPIC CONSUMPTION STATS")
        print("="*50)
        for topic, count in self.message_counts.items():
            print(f"{topic}: {count} messages")

def main():
    """Main function to demonstrate consumers"""
    print("Kafka Consumer Demo")
    print("1. User Events Consumer")
    print("2. Order Events Consumer") 
    print("3. Multi-Topic Consumer")
    print("4. All Consumers (Parallel)")
    
    choice = input("Choose option (1-4): ").strip()
    
    if choice == '1':
        consumer = UserEventConsumer()
        consumer.consume_events(max_messages=50)
        
    elif choice == '2':
        consumer = OrderEventConsumer()
        consumer.consume_orders(max_messages=25)
        
    elif choice == '3':
        consumer = MultiTopicConsumer()
        consumer.consume_all(max_messages=30)
        
    elif choice == '4':
        # Run multiple consumers in parallel
        user_consumer = UserEventConsumer(group_id='parallel-user-group')
        order_consumer = OrderEventConsumer(group_id='parallel-order-group')
        
        user_thread = threading.Thread(
            target=user_consumer.consume_events,
            kwargs={'max_messages': 25}
        )
        order_thread = threading.Thread(
            target=order_consumer.consume_orders,
            kwargs={'max_messages': 15}
        )
        
        user_thread.start()
        order_thread.start()
        
        user_thread.join()
        order_thread.join()
        
    else:
        print("Invalid choice")

if __name__ == "__main__":
    main()