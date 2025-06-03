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


class FinancialTransactionProducer:
    def __init__(
        self,
        bootstrap_servers=['localhost:9092'],
        topic='financial-transactions'
    ):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str)
            .encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=5,
            retry_backoff_ms=2000,
            max_in_flight_requests_per_connection=1,  # Set to 1 for idempotence
            enable_idempotence=True,
            compression_type='gzip',
            linger_ms=10,
            security_protocol='PLAINTEXT'  # Use SSL/SASL in prod
        )
        logger.info(f"Producer initialized for topic: {self.topic}")

    def generate_transaction(self, account_id):
        """Generate a sample financial transaction"""
        transaction_types = ['deposit', 'withdrawal', 'trade', 'transfer']
        txn_type = random.choice(transaction_types)
        amount = round(random.uniform(10, 10000), 2)
        currency = random.choice(['USD', 'EUR', 'JPY', 'GBP'])
        instrument = None
        if txn_type == 'trade':
            instrument = random.choice(['AAPL', 'GOOG', 'TSLA', 'BTC', 'ETH'])
        return {
            'transaction_id': f"txn_{random.randint(100000, 999999)}",
            'account_id': account_id,
            'type': txn_type,
            'amount': amount,
            'currency': currency,
            'timestamp': datetime.utcnow().isoformat(),
            'status': 'completed',
            'metadata': {
                'instrument': instrument,
                'ip_address': f"10.0.0.{random.randint(1, 255)}"
            }
        }

    def produce_transactions(self, num_events=50, delay=0.5):
        """Produce multiple financial transactions"""
        try:
            logger.info(
                f"Producing {num_events} transactions to topic '{self.topic}'"
            )
            for i in range(num_events):
                account_id = f"acct_{random.randint(1000, 1020)}"
                txn = self.generate_transaction(account_id)
                future = self.producer.send(
                    self.topic,
                    key=account_id,
                    value=txn
                )
                future.add_callback(self.on_send_success)
                future.add_errback(self.on_send_error)
                logger.info(
                    f"Sent transaction {i+1}/{num_events}: {txn['type']} "
                    f"for {account_id}"
                )
                time.sleep(delay)
        except KeyboardInterrupt:
            logger.info("Producer interrupted by user")
        except Exception as e:
            logger.error(f"Error producing transactions: {e}")
        finally:
            self.close()

    def on_send_success(self, record_metadata):
        """Callback for successful message delivery"""
        logger.debug(
            f"Message delivered to {record_metadata.topic} partition "
            f"{record_metadata.partition} offset {record_metadata.offset}"
        )

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
    print("Financial Transaction Kafka Producer")
    print("=" * 40)
    try:
        num_events = int(
            input(
                "Enter number of transactions to produce (default 20): "
            ) or "20"
        )
        delay = float(
            input(
                "Enter delay between transactions in seconds (default 0.5): "
            ) or "0.5"
        )
    except ValueError:
        print("Using default values...")
        num_events = 20
        delay = 0.5
    producer = FinancialTransactionProducer()
    producer.produce_transactions(num_events=num_events, delay=delay)


if __name__ == "__main__":
    main()