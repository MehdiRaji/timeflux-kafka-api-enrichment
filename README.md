# 🌬️ TimeFlux Technical Assessment - Data Engineering

A real-time data enrichment pipeline for wind turbine telemetry using Kafka, Python microservices, and Docker Compose.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Services Description](#services-description)
- [Configuration](#configuration)
- [Testing & Validation](#testing--validation)
- [Design Choices](#design-choices)
- [Assumptions & Limitations](#assumptions--limitations)
- [Troubleshooting](#troubleshooting)

---

## 🎯 Overview

This project implements a streaming data enrichment service that:

1. **Produces** raw turbine telemetry data to a Kafka topic (`raw-turbine-data`)
2. **Enriches** the data by fetching metadata from a referential API
3. **Publishes** the enriched data to another Kafka topic (`enriched-turbine-data`)

### Input Data Format
```json
{
  "turbine_id": "T123",
  "timestamp": "2025-11-26T12:00:00Z",
  "value": 150.2,
  "unit": "kW"
}
```

### Output Data Format
```json
{
  "turbine_id": "T123",
  "timestamp": "2025-11-26T12:00:00Z",
  "value": 150.2,
  "unit": "kW",
  "country": "France"
}
```

---

## 🏗️ Architecture
```
┌─────────────┐      ┌──────────────────┐      ┌─────────────────────┐
│  Producer   │─────▶│  Kafka           │─────▶│  Enrichment Service │
│             │      │  raw-turbine-data│      │                     │
└─────────────┘      └──────────────────┘      └──────────┬──────────┘
                                                           │
                                                           │ GET /turbine/{id}
                                                           ▼
                                                ┌──────────────────────┐
                                                │  Referential API     │
                                                │  (Mock)              │
                                                └──────────────────────┘
                                                           │
                                                           ▼
                                          ┌────────────────────────────┐
                                          │  Kafka                     │
                                          │  enriched-turbine-data     │
                                          └────────────────────────────┘
```

### Components

- **Zookeeper**: Kafka cluster coordination
- **Kafka**: Message broker for streaming data
- **Producer**: Generates synthetic turbine telemetry data
- **Referential API**: FastAPI service providing turbine metadata
- **Enrichment Service**: Consumes, enriches, and republishes data

---

## 🔧 Prerequisites

- **Docker** >= 20.10
- **Docker Compose** >= 2.0
- **Git**

---

## 🚀 Quick Start

### 1. Clone the repository
```bash
git clone https://github.com/YOUR-USERNAME/timeflux-tech-test.git
cd timeflux-tech-test
```

### 2. Start all services
```bash
docker-compose up --build -d
```

Wait approximately **30 seconds** for Kafka to be fully ready.

### 3. Verify services are running
```bash
docker-compose ps
```

You should see 5 containers running:
- `zookeeper`
- `kafka`
- `producer`
- `referential-api`
- `enrichment-service`

### 4. Check logs
```bash
# Producer logs
docker-compose logs -f producer

# Enrichment service logs
docker-compose logs -f enrichment-service
```

You should see messages like:
```
producer | INFO | producer | Sent data: {'turbine_id': 'T123', ...}
enrichment | INFO | enrichment | Enriched message sent: {..., 'country': 'France'}
```

### 5. Consume enriched messages
```bash
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic enriched-turbine-data \
  --from-beginning
```

Press `Ctrl+C` to stop.

### 6. Stop all services
```bash
docker-compose down -v
```

---

## 📁 Project Structure
```
.
├── docker-compose.yml           # Orchestration of all services
├── .gitignore                   # Ignore patterns
├── README.md                    # This file
│
├── producer/
│   ├── Dockerfile               # Producer container image
│   ├── producer.py              # Kafka message producer
│   └── requirements.txt         # Python dependencies
│
├── referential_api/
│   ├── Dockerfile               # API container image
│   ├── api.py                   # FastAPI application
│   └── requirements.txt         # Python dependencies
│
└── enrichment_service/
    ├── Dockerfile               # Enrichment service container
    ├── enrichment_service.py    # Kafka consumer + enrichment logic
    └── requirements.txt         # Python dependencies
```

---

## 🔍 Services Description

### 1. Producer (`producer/`)

**Purpose**: Simulates IoT sensors generating turbine telemetry data.

**Features**:
- Generates random turbine IDs: `T123`, `T124`, `T125`
- Configurable event rate (default: 12 events/minute)
- Automatic reconnection to Kafka with retry logic
- Structured logging for monitoring

**Environment Variables**:
- `KAFKA_BROKER`: Kafka server address (default: `kafka:9092`)
- `KAFKA_TOPIC`: Output topic (default: `raw-turbine-data`)
- `EVENTS_PER_MINUTE`: Message generation rate (default: `12`)

### 2. Referential API (`referential_api/`)

**Purpose**: Provides metadata for turbines (mock data).

**Endpoints**:
- `GET /turbine/{turbine_id}`: Returns turbine metadata

**Example**:
```bash
curl http://localhost:5000/turbine/T123
# Response: {"turbine_id": "T123", "country": "France"}
```

**Available Turbines**:
- `T123`: France
- `T124`: Belgique
- `T125`: Espagne

### 3. Enrichment Service (`enrichment_service/`)

**Purpose**: Core ETL pipeline for data enrichment.

**Workflow**:
1. Consumes messages from `raw-turbine-data`
2. Validates message structure (checks for `turbine_id`)
3. Calls Referential API to fetch metadata
4. Merges raw data with metadata
5. Publishes enriched data to `enriched-turbine-data`

**Features**:
- Automatic Kafka reconnection with retry logic
- API timeout handling (5 seconds)
- Comprehensive error logging
- Invalid message handling (logs warning and skips)
- Offset management with consumer groups

**Environment Variables**:
- `KAFKA_BROKER`: Kafka server address
- `INPUT_TOPIC`: Source topic (default: `raw-turbine-data`)
- `OUTPUT_TOPIC`: Destination topic (default: `enriched-turbine-data`)
- `REFERENTIAL_API_URL`: API base URL

---

## ⚙️ Configuration

All services are configurable via environment variables in `docker-compose.yml`.

### Adjust Producer Rate

To generate **60 events per minute**:
```yaml
producer:
  environment:
    EVENTS_PER_MINUTE: 60
```

Then restart:
```bash
docker-compose up -d producer
```

### Add New Turbines

Edit `referential_api/api.py`:
```python
TURBINE_METADATA = {
    "T123": {"turbine_id": "T123", "country": "France"},
    "T124": {"turbine_id": "T124", "country": "Belgique"},
    "T125": {"turbine_id": "T125", "country": "Espagne"},
    "T126": {"turbine_id": "T126", "country": "Allemagne"},  # New turbine
}
```

And `producer/producer.py`:
```python
TURBINE_IDS = ["T123", "T124", "T125", "T126"]
```

Then rebuild:
```bash
docker-compose up -d --build producer referential-api
```

---

## ✅ Testing & Validation

### 1. Test API Availability
```bash
curl http://localhost:5000/turbine/T123
```

Expected output:
```json
{"turbine_id":"T123","country":"France"}
```

### 2. Verify Message Flow

Check that messages contain the `country` field:
```bash
docker-compose logs enrichment-service | grep "Enriched message sent"
```

Expected output:
```
enrichment | INFO | Enriched message sent: {'turbine_id': 'T123', ..., 'country': 'France'}
```

### 3. Monitor Kafka Topics

List all topics:
```bash
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Describe a topic:
```bash
docker-compose exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe --topic enriched-turbine-data
```

### 4. End-to-End Test
```bash
# Terminal 1: Watch enrichment logs
docker-compose logs -f enrichment-service

# Terminal 2: Consume enriched messages
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic enriched-turbine-data \
  --from-beginning
```

You should see messages flowing with the `country` field added.

---

## 🧠 Design Choices

### 1. **Synchronous Processing**

- **Choice**: Single-threaded consumer with blocking API calls
- **Rationale**: Simplicity and clarity for a technical assessment; easier to debug and understand
- **Trade-off**: Limited throughput (~12-100 messages/minute acceptable for this use case)
- **Production alternative**: Use async HTTP (aiohttp) or parallel workers for higher throughput

### 2. **Retry Logic with Exponential Backoff**

- **Choice**: Infinite retry with 5-second delays for Kafka connections
- **Rationale**: Ensures service resilience during startup and temporary network issues
- **Implementation**: `while True` loops with `time.sleep(RETRY_DELAY)`
- **Benefit**: Services automatically recover when dependencies become available

### 3. **Error Handling Strategy**

- **Choice**: Log and skip invalid messages (missing `turbine_id` or API failures)
- **Rationale**: Prevent pipeline blocking due to malformed data or API unavailability
- **Trade-off**: Messages are lost if API is down (acceptable for telemetry data)
- **Production alternative**: Implement dead-letter queue for failed enrichments

### 4. **Consumer Groups for Scalability**

- **Choice**: Single consumer group `enrichment-service`
- **Rationale**: Enables horizontal scaling by adding replicas without code changes
- **Example**: Scale to 3 instances with `docker-compose up -d --scale enrichment-service=3`
- **Benefit**: Kafka automatically distributes partitions across consumers

### 5. **Auto-Commit Offsets**

- **Choice**: `enable_auto_commit=True`
- **Rationale**: Simplifies implementation; acceptable for non-critical telemetry data
- **Trade-off**: Potential message loss on crashes (before processing completes)
- **Production alternative**: Manual offset management for exactly-once semantics

### 6. **Structured Logging**

- **Choice**: Consistent log format across all services with timestamp, level, and service name
- **Rationale**: Facilitates debugging and monitoring in distributed systems
- **Format**: `%(asctime)s | %(levelname)s | service_name | %(message)s`

---

## 📌 Assumptions & Limitations

### Assumptions

1. **Mock API**: The referential API contains static data (no database persistence)
2. **Network Reliability**: Services communicate within the same Docker network
3. **Data Volume**: Designed for low-to-medium throughput (<1000 events/min)
4. **Message Format**: All messages follow the expected JSON schema
5. **Telemetry Data**: Data loss is acceptable (not financial or critical transactions)

### Limitations

1. **No Authentication**: API and Kafka are unsecured (not production-ready)
2. **No Persistence**: Referential API data is lost on container restart
3. **Single Partition**: Kafka topics use 1 partition (limits parallelization)
4. **No Monitoring**: Missing Prometheus/Grafana for observability
5. **Synchronous API Calls**: May cause backpressure under high load
6. **No Dead Letter Queue**: Failed messages are logged but not stored for retry
7. **Basic Error Handling**: No circuit breaker or advanced retry strategies

### Potential Production Improvements

**Scalability**:
- Use multiple Kafka partitions for parallel processing
- Implement async HTTP calls with aiohttp
- Add connection pooling for API calls

**Reliability**:
- Add dead-letter queue for failed enrichments
- Implement circuit breaker pattern for API calls
- Use manual offset commits for exactly-once processing

**Observability**:
- Add Prometheus metrics (messages/sec, API latency, errors)
- Integrate with Grafana for dashboards
- Implement distributed tracing (OpenTelemetry)

**Data Quality**:
- Add schema validation (JSON Schema or Avro)
- Implement data quality checks and alerting
- Store enrichment audit trail

**Security**:
- Add Kafka SASL/SSL authentication
- Implement API key authentication
- Use secrets management (Vault, AWS Secrets Manager)

---

## 🐛 Troubleshooting

### Issue: Services fail to start

**Solution**:
```bash
docker-compose down -v
docker-compose up --build -d
```

### Issue: "No partition metadata for topic"

**Cause**: Kafka is still initializing (auto-create topics takes a few seconds).

**Solution**: Wait 30-60 seconds, then check logs:
```bash
docker-compose logs kafka
```

Look for: `INFO Created topic raw-turbine-data`

### Issue: API returns 404 for turbine

**Cause**: Turbine ID not in `TURBINE_METADATA` dictionary.

**Solution**: Add turbine to `referential_api/api.py` and rebuild:
```bash
docker-compose up -d --build referential-api
```

### Issue: Consumer not receiving messages

**Diagnostic steps**:

1. Check producer is sending:
```bash
docker-compose logs producer
```

2. Check topic exists:
```bash
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

3. Consume directly from raw topic:
```bash
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic raw-turbine-data \
  --from-beginning
```

### Issue: Enrichment service crashes

**Check logs**:
```bash
docker-compose logs enrichment-service
```

**Common causes**:
- Kafka not ready → Wait 30+ seconds after startup
- API unreachable → Check `docker-compose ps` shows referential-api as "Up"
- Invalid message format → Check producer output matches expected schema

### Issue: "Metadata not found for turbine"

**Cause**: Producer generated turbine ID not in referential API.

**Solution**: Ensure `TURBINE_IDS` in producer matches keys in `TURBINE_METADATA` in API.

---

## 📚 Additional Resources

- [Kafka Python Documentation](https://kafka-python.readthedocs.io/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Docker Compose Reference](https://docs.docker.com/compose/)
- [Confluent Kafka Best Practices](https://docs.confluent.io/platform/current/kafka/design.html)

---

## 👤 Author

**El Mehdi RAJI**  
Technical Assessment for TimeFlux - Data Engineering Position

---

## 📄 License

This project is created for educational and assessment purposes.