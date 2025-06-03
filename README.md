# Real-Time Financial Transactions Pipeline with Kafka and Python

This project demonstrates a real-time financial transaction pipeline using Kafka, Python, and Docker Compose.

## Project Structure

```
kafka-financial/
├── docker-compose.yaml         # Kafka cluster setup
├── producer.py                 # Financial transaction producer
├── consumer.py                 # Financial analytics consumer
├── pyproject.toml              # Python dependencies
├── README.md                   # Project documentation
├── .gitignore
├── .python-version
└── uv.lock
```

## Features

### Kafka Cluster
- **3 Kafka Brokers** for high availability
- **Zookeeper** for cluster coordination
- **Kafka UI** for monitoring and management
- **Automatic topic creation** enabled

### Python Producer
- **Financial Transaction Producer**: Simulates real-time transactions (deposit, withdrawal, trade, transfer)
- **Key-based partitioning** using account_id
- **Idempotence, compression, batching** for reliability and throughput
- **Configurable transaction count and delay**
- **Schema validation and error handling**

### Python Consumer
- **Financial Analytics Consumer**: Processes transactions and computes real-time analytics
- **Aggregates by transaction type, currency, and account**
- **Consumer group** management for scalability
- **Configurable message limits and timeout**
- **Graceful shutdown and summary reporting**

## Best Practices Implemented

- **Idempotent producer** to prevent duplicate transactions
- **Compression** for network efficiency
- **Batching** with `linger_ms` for higher throughput
- **Structured logging** for traceability
- **Schema validation** (extend with libraries like `pydantic` for production)
- **Graceful shutdown** and resource cleanup
- **Security protocol placeholder** (use SSL/SASL in production)
- **Error handling** with retries and logging

## Prerequisites

- Docker and Docker Compose
- Python 3.12+
- pip

## Setup Instructions

### 1. Install Python Dependencies

```zsh
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt  # or use pyproject.toml with pip/uv/poetry
```

### 2. Start Kafka Cluster

```zsh
docker-compose up -d
```

### 3. Access Kafka UI

Open [http://localhost:8080](http://localhost:8080) to monitor topics and messages.

## Usage

### Run the Producer

```zsh
python producer.py
```
- Enter number of transactions and delay as prompted.

### Run the Consumer

```zsh
python consumer.py
```
- Enter max transactions and timeout as prompted.

## Kafka Topic

- `financial-transactions`: All financial events (deposit, withdrawal, trade, transfer)

## Monitoring and Debugging

- Use Kafka UI at [http://localhost:8080](http://localhost:8080)
- Check logs for errors and analytics summaries

## Scaling and Security

- **Scale consumers** by running multiple instances (same group_id)
- **Secure Kafka** with SSL/SASL in production (update `security_protocol`)
- **Validate schemas** with libraries like `pydantic` or `jsonschema`
- **Monitor consumer lag** and broker health

---

For more details, see the code in [`producer.py`](producer.py) and [`consumer.py`](consumer.py).