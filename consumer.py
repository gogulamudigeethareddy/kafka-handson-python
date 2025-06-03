import json
import time
from collections import defaultdict
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleKafkaConsumer:
    def __init__(self, bootstrap_servers=['localhost:9092'], topic='user-events', group_id='user-events-group'):
        self.topic = topic
        self.group_id = group_id
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',  # Start from beginning if no offset
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )
        
        # Analytics storage
        self.event_counts = defaultdict(int)
        self.user_activity = defaultdict(list)
        self.total_messages = 0
        
        logger.info(f"Consumer initialized for topic: {topic}, group: {group_id}")

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
            self.total_messages += 1
            
            logger.info(f"[Message {self.total_messages}] Processed: {event_type} from {user_id}")
            
            # Simulate processing time
            time.sleep(0.1)
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def consume_events(self, max_messages=None, timeout_seconds=60):
        """Consume user events"""
        start_time = time.time()
        
        try:
            logger.info(f"Starting to consume from topic '{self.topic}'...")
            logger.info(f"Max messages: {max_messages or 'Unlimited'}")
            logger.info(f"Timeout: {timeout_seconds} seconds")
            
            for message in self.consumer:
                self.process_message(message)
                
                # Check if we've reached the maximum message limit
                if max_messages and self.total_messages >= max_messages:
                    logger.info(f"Reached maximum message limit: {max_messages}")
                    break
                
                # Check for timeout
                elapsed_time = time.time() - start_time
                if elapsed_time >= timeout_seconds:
                    logger.info(f"Timeout reached: {timeout_seconds} seconds")
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
        
        print(f"\nTotal Messages Processed: {self.total_messages}")
        
        if self.event_counts:
            print("\nEvent Type Distribution:")
            for event_type, count in sorted(self.event_counts.items()):
                percentage = (count / self.total_messages) * 100 if self.total_messages > 0 else 0
                print(f"  {event_type}: {count} ({percentage:.1f}%)")
        
        print(f"\nTotal Unique Users: {len(self.user_activity)}")
        
        # Top active users
        if self.user_activity:
            user_event_counts = {user: len(events) for user, events in self.user_activity.items()}
            top_users = sorted(user_event_counts.items(), key=lambda x: x[1], reverse=True)[:5]
            
            print("\nTop 5 Most Active Users:")
            for user, count in top_users:
                print(f"  {user}: {count} events")
        
        print("="*50)

    def close(self):
        """Close the consumer"""
        self.consumer.close()
        self.print_analytics()
        logger.info("Consumer closed")

def main():
    """Main function to run the consumer"""
    print("Simple Kafka Consumer")
    print("=" * 30)
    
    # Get user input for configuration
    try:
        max_messages = input("Enter max messages to consume (default: unlimited): ").strip()
        max_messages = int(max_messages) if max_messages else None
        
        timeout = input("Enter timeout in seconds (default: 60): ").strip()
        timeout = int(timeout) if timeout else 60
        
    except ValueError:
        print("Using default values...")
        max_messages = None
        timeout = 60
    
    # Create and run consumer
    consumer = SimpleKafkaConsumer()
    consumer.consume_events(max_messages=max_messages, timeout_seconds=timeout)

if __name__ == "__main__":
    main()