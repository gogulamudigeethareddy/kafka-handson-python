import json
import time
from collections import defaultdict
from kafka import KafkaConsumer
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FinancialTransactionConsumer:
    def __init__(self, bootstrap_servers=['localhost:9092'], topic='financial-transactions', group_id='financial-analytics-group'):
        self.topic = topic
        self.group_id = group_id
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            security_protocol='PLAINTEXT'  # Use SSL/SASL in prod
        )
        self.txn_counts = defaultdict(int)
        self.account_activity = defaultdict(list)
        self.total_amount = defaultdict(float)
        self.total_messages = 0
        logger.info(
            f"Consumer initialized for topic: {topic}, group: {group_id}"
        )

    def process_message(self, message):
        """Process individual financial transaction"""
        try:
            txn = message.value
            account_id = txn.get('account_id')
            txn_type = txn.get('type')
            amount = float(txn.get('amount', 0))
            currency = txn.get('currency')
            timestamp = txn.get('timestamp')
            self.txn_counts[txn_type] += 1
            self.account_activity[account_id].append({
                'type': txn_type,
                'amount': amount,
                'currency': currency,
                'timestamp': timestamp
            })
            self.total_amount[currency] += amount
            self.total_messages += 1
            logger.info(
                f"[Message {self.total_messages}] {txn_type} {amount} "
                f"{currency} for {account_id}"
            )
            time.sleep(0.05)
        except Exception as e:
            logger.error(f"Error processing transaction: {e}")

    def consume_transactions(self, max_messages=None, timeout_seconds=60):
        start_time = time.time()
        try:
            logger.info(f"Consuming from topic '{self.topic}'...")
            for message in self.consumer:
                self.process_message(message)
                if max_messages and self.total_messages >= max_messages:
                    logger.info(
                        f"Reached max message limit: {max_messages}"
                    )
                    break
                if time.time() - start_time >= timeout_seconds:
                    logger.info(
                        f"Timeout reached: {timeout_seconds} seconds"
                    )
                    break
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Error consuming transactions: {e}")
        finally:
            self.close()

    def print_analytics(self):
        print("\n" + "="*50)
        print("FINANCIAL TRANSACTION ANALYTICS")
        print("="*50)
        print(f"\nTotal Transactions Processed: {self.total_messages}")
        if self.txn_counts:
            print("\nTransaction Type Distribution:")
            for txn_type, count in sorted(self.txn_counts.items()):
                pct = (
                    (count / self.total_messages) * 100
                    if self.total_messages > 0 else 0
                )
                print(f"  {txn_type}: {count} ({pct:.1f}%)")
        print("\nTotal Amounts by Currency:")
        for currency, total in self.total_amount.items():
            print(f"  {currency}: {total:.2f}")
        print(f"\nTotal Unique Accounts: {len(self.account_activity)}")
        if self.account_activity:
            acct_counts = {
                acct: len(events)
                for acct, events in self.account_activity.items()
            }
            top_accounts = sorted(
                acct_counts.items(), key=lambda x: x[1], reverse=True
            )[:5]
            print("\nTop 5 Most Active Accounts:")
            for acct, count in top_accounts:
                print(f"  {acct}: {count} transactions")
        print("="*50)

    def close(self):
        self.consumer.close()
        self.print_analytics()
        logger.info("Consumer closed")


def main():
    print("Financial Transaction Kafka Consumer")
    print("=" * 40)
    try:
        max_messages = input(
            "Enter max transactions to consume (default: unlimited): "
        ).strip()
        max_messages = int(max_messages) if max_messages else None
        timeout = input(
            "Enter timeout in seconds (default: 60): "
        ).strip()
        timeout = int(timeout) if timeout else 60
    except ValueError:
        print("Using default values...")
        max_messages = None
        timeout = 60
    consumer = FinancialTransactionConsumer()
    consumer.consume_transactions(
        max_messages=max_messages, timeout_seconds=timeout
    )


if __name__ == "__main__":
    main()