# End-to-End Kafka Project with Python

This project demonstrates a complete Kafka setup with Python producers and consumers, orchestrated using Docker Compose.

## Project Structure

```
kafka-project/
├── docker-compose.yml      # Kafka cluster setup
├── requirements.txt        # Python dependencies
├── producer.py            # Kafka producers
├── consumer.py            # Kafka consumers
└── README.md             # This file
```

## Features

### Kafka Cluster
- **3 Kafka Brokers** for high availability
- **Zookeeper** for cluster coordination  
- **Kafka UI** for monitoring and management
- **Automatic topic creation** enabled

### Python Producers
- **User Event Producer**: Generates user activity events (login, purchase, etc.)
- **Order Event Producer**: Creates order lifecycle events
- **Key-based partitioning** for scalability
- **Error handling and callbacks**
- **Compression and idempotence** enabled

### Python Consumers
- **User Event Consumer**: Processes user events with analytics
- **Order Event Consumer**: Handles order events with revenue tracking
- **Multi-topic Consumer**: Consumes from multiple topics
- **Consumer groups** for parallel processing
- **Automatic offset management**

## Prerequisites

- Docker and Docker Compose
- Python 3.7+
- pip (Python package manager)

## Setup Instructions

### 1. Clone and Setup

```bash
# Create project directory
mkdir kafka-project && cd kafka-project

# Create Python virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install Python dependencies
pip install -r requirements.txt
```

### 2. Start Kafka Cluster

```bash
# Start all services
docker-compose up -d

# Verify services are running
docker-compose ps

# Check logs if needed
docker-compose logs kafka-broker-1
```

### 3. Access Kafka UI

Open your browser and go to: `http://localhost:8080`

The Kafka UI provides:
- Topic management
- Message browsing
- Consumer group monitoring
- Cluster health status

## Usage Examples

### Running Producers

```bash
# Run user event producer
python producer.py
# Choose option 1 for user events

# Run order event producer  
python producer.py
# Choose option 2 for order events

# Run both producers simultaneously
python producer.py
# Choose option 3 for both
```

### Running Consumers

```bash
# In separate terminal windows/tabs

# User event consumer
python consumer.py
# Choose option 1

# Order event consumer
python consumer.py  
# Choose option 2

# Multi-topic consumer
python consumer.py
# Choose option 3

# Parallel consumers
python consumer.py
# Choose option 4
```

## Kafka Topics

The application uses these topics:
- `user-events`: User activity events
- `order-events`: Order lifecycle events

Topics are auto-created when first used, or you can create them manually:

```bash
# Create topics manually (optional)
docker exec -it kafka-broker-1 kafka-topics --create \
  --topic user-events --partitions 3 --replication-factor 2 \
  --bootstrap-server localhost:29092

docker exec -it kafka-broker-1 kafka-topics --create \
  --topic order-events --partitions 3 --replication-factor 2 \
  --bootstrap-server localhost:29092
```

## Monitoring and Debugging

### Check Topic Information
```bash
# List topics
docker exec -it kafka-broker-1 kafka-topics --list \
  --bootstrap-server localhost:29092

# Describe topic
docker exec -it kafka-broker-1 kafka-topics --describe \
  --topic user-events --bootstrap-server localhost:29092
```

### Monitor Consumer Groups
```bash
# List consumer groups
docker exec -it kafka-broker-1 kafka-consumer-groups --list \
  --bootstrap-server localhost:29092

# Describe consumer group
docker exec -it kafka-broker-1 kafka-consumer-groups --describe \
  --group user-events-group --bootstrap-server localhost:29092
```

### View Messages Directly
```bash
# Consume messages from beginning
docker exec -it kafka-broker-1 kafka-console-consumer \
  --topic user-events --from-beginning \
  --bootstrap-server localhost:29092
```

## Configuration Details

### Producer Configuration
- **Acknowledgments**: `acks='all'` for durability
- **Retries**: 3 attempts with backoff
- **Idempotence**: Enabled to prevent duplicates
- **Compression**: GZIP for efficiency
- **Partitioning**: Key-based using user_id/order_id

### Consumer Configuration
- **Offset Reset**: `earliest` to process all messages
- **Auto Commit**: Enabled with 1-second interval
- **Session Timeout**: 30 seconds
- **Consumer Timeout**: 1 second for demo purposes

### Cluster Configuration
- **Replication Factor**: 2 for fault tolerance
- **Min ISR**: 2 for consistency
- **Auto Topic Creation**: Enabled for convenience

## Analytics Features

### User Event Analytics
- Event type distribution
- User activity tracking
- Top active users
- Session analysis

### Order Analytics  
- Order status distribution
- Revenue calculation
- Geographic analysis
- Processing metrics

## Scaling Considerations

### Horizontal Scaling
- Add more Kafka brokers to the cluster
- Increase topic partitions for parallelism  
- Deploy multiple consumer instances

### Monitoring in Production
- Use Kafka UI or tools like Confluent Control Center
- Monitor consumer lag and throughput
- Set up alerts for broker health

## Troubleshooting

### Common Issues

1. **Connection Refused**
   ```bash
   # Check if services are running
   docker-compose ps
   
   # Restart services
   docker-compose restart
   ```

2. **Consumer Not Receiving Messages**
   ```bash
   # Check consumer group status
   docker exec -it kafka-broker-1 kafka-consumer-groups --describe \
     --group your-group-name --bootstrap-server localhost:29092
   ```

3. **Topic Not Found**
   ```bash
   # List available topics
   docker exec -it kafka-broker-1 kafka-topics --list \
     --bootstrap-server localhost:29092
   ```

### Cleanup

```bash
# Stop and remove containers
docker-compose down

# Remove volumes (deletes all data)
docker-compose down -v

# Remove images
docker-compose down --rmi all
```
