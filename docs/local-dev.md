# Local Development Setup

This guide covers running EventLineage components individually without Docker Compose.

## Prerequisites

- Go 1.22+
- Node.js 20+
- Python 3.9+
- Neo4j 5.x
- Apache Kafka 3.x

## Running Neo4j Locally

### Option 1: Neo4j Desktop (Recommended)
1. Download [Neo4j Desktop](https://neo4j.com/download/)
2. Create a new database
3. Set password to `eventlineage`
4. Start the database

### Option 2: Homebrew (macOS)
```bash
brew install neo4j
neo4j start
```

### Option 3: Docker (standalone)
```bash
docker run -d \
  --name neo4j \
  -p 7474:7474 -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/eventlineage \
  neo4j:5.15-community
```

## Running Kafka Locally

### Option 1: Homebrew (macOS)
```bash
brew install kafka
zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties &
kafka-server-start /opt/homebrew/etc/kafka/server.properties
```

### Option 2: Docker (standalone)
```bash
# Start Zookeeper
docker run -d --name zookeeper \
  -p 2181:2181 \
  confluentinc/cp-zookeeper:7.5.0

# Start Kafka
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_ZOOKEEPER_CONNECT=host.docker.internal:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  confluentinc/cp-kafka:7.5.0
```

## Running the Go API Server

```bash
cd backend

# Set environment variables
export NEO4J_URI=bolt://localhost:7687
export NEO4J_USER=neo4j
export NEO4J_PASSWORD=eventlineage
export KAFKA_BROKERS=localhost:9092
export KAFKA_GROUP_ID=eventlineage-dev
export PORT=8080

# Run
go run ./cmd/server
```

The API will be available at `http://localhost:8080`.

## Running the React UI

```bash
cd ui

# Install dependencies
npm install

# Start dev server (proxies API to localhost:8080)
npm run dev
```

The UI will be available at `http://localhost:3000`.

## Running the Demo Script

```bash
cd scripts

# Install Python SDK
pip install -e ../sdk/python

# Run demo
python demo.py
```

## Environment Variables Reference

### API Server

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8080` | HTTP server port |
| `NEO4J_URI` | `bolt://localhost:7687` | Neo4j connection URI |
| `NEO4J_USER` | `neo4j` | Neo4j username |
| `NEO4J_PASSWORD` | `password` | Neo4j password |
| `KAFKA_BROKERS` | `localhost:9092` | Kafka bootstrap servers |
| `KAFKA_GROUP_ID` | `eventlineage-ingestion` | Kafka consumer group |

### Python SDK

```python
from eventlineage import LineageProducer, LineageConsumer

producer = LineageProducer(
    bootstrap_servers="localhost:9092",
    service_name="my-service",
    lineage_bootstrap_servers="localhost:9092",  # Optional: separate cluster
    tenant_id="my-tenant",                       # Optional: multi-tenancy
)
```

### Java SDK

```properties
# Required
lineage.service.name=my-service

# Optional
lineage.events.topic=_lineage.events
lineage.bootstrap.servers=localhost:9092
lineage.tenant.id=my-tenant
lineage.payload.preview.enabled=true
lineage.payload.preview.max.length=500
```

## Troubleshooting

### Neo4j connection refused
- Ensure Neo4j is running: `neo4j status`
- Check the Bolt port (7687) is not blocked
- Verify credentials match

### Kafka connection failed
- Ensure Zookeeper is running before Kafka
- Check `KAFKA_ADVERTISED_LISTENERS` matches your connection string
- Create the lineage topic: `kafka-topics --create --topic _lineage.events --bootstrap-server localhost:9092`

### UI can't connect to API
- Ensure API is running on port 8080
- Check browser console for CORS errors
- Verify the Vite proxy config in `vite.config.ts`
