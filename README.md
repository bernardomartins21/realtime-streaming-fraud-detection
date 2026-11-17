# Real-Time Streaming Fraud Detection Pipeline

End-to-end real-time data engineering pipeline designed to ingest, deserialize, validate, and detect high-value financial anomalies using an event-driven architecture.

## Current Architecture (Sprints 1 & 2)

```text
[Python Producer] ──(JSON/Bytes)──> [Apache Kafka (3 Partitions)] ──> [PySpark Structured Streaming] ──> [Risk Alerts]
```

## Tech Stack
* **Streaming & Message Broker:** Apache Kafka (KRaft mode) & Kafka UI orchestrated via Docker Compose
* **Ingestion:** Python (`confluent-kafka`) with deterministic key-based partitioning (`account_id`)
* **Stream Processing:** PySpark Structured Streaming (`3.5.x`) with strict Schema Enforcement (`StructType`) and fault-tolerant state management (`checkpointLocation`)
* **Storage (Upcoming - Sprint 3):** Delta Lake (Medallion Architecture: Bronze, Silver & Gold)

## Running Locally

### Prerequisites
* Docker & Docker Compose
* Python 3.10+ and Java JDK 17 (`JAVA_HOME` configured)

### 1. Start Kafka Infrastructure
```bash
docker compose up -d
```
*Kafka UI is accessible at `http://localhost:8080`.*

### 2. Set Up Python Environment
```bash
python -m venv venv
source venv/bin/activate
pip install confluent-kafka pyspark==3.5.9
```

### 3. Start Transaction Producer (Terminal 1)
```bash
python src/producers/transaction_producer.py
```

### 4. Start PySpark Streaming Risk Detector (Terminal 2)
```bash
python src/consumers/risk_detector.py
```

## Project Roadmap
- [x] **Sprint 1:** Docker Infrastructure (Kafka KRaft + Kafka UI) & Python Event Producer
- [x] **Sprint 2:** Real-Time Stream Processing & Business Rules with PySpark Structured Streaming
- [ ] **Sprint 3:** Medallion Architecture Persistence (Bronze & Silver Layers) with Delta Lake
- [ ] **Sprint 4:** Gold Layer Analytical Data Mart & Window Aggregations
- [ ] **Sprint 5:** Automated Data Quality & Unit Testing (PyTest & Dead Letter Queue)
- [ ] **Sprint 6:** CI/CD Pipeline with GitHub Actions