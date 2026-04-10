# Real-Time Network Event Streaming Pipeline

Real-time data engineering pipeline processing network telemetry events at 500+ events per second — Apache Kafka ingestion, PySpark Structured Streaming, Delta Lake medallion architecture, Apache Airflow orchestration, and Snowflake analytics serving layer.

![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)
![Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=flat)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=flat&logo=apacheairflow&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat&logo=snowflake&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)

---

## Business Context

Network Operations Centers monitor hundreds of devices — routers, switches, and firewalls — that emit telemetry events continuously. Every event carries operational intelligence: a CPU spike may indicate an overloaded switch, a packet loss spike may signal a failing link, and an unusual traffic pattern may indicate a security incident.

Batch processing is insufficient for this workload. A device failure processed in an overnight batch job could mean six hours of undetected downtime. Security incidents grow in severity every minute they go undetected.

This pipeline ingests network telemetry events in real time, processes them through a Bronze/Silver/Gold medallion architecture, and surfaces operational KPIs in Snowflake within seconds of occurrence. A dual micro-batch design separates critical alerts — which need a 10-second response window — from standard analytics, which run on a 60-second cadence.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                    NETWORK DEVICE SIMULATOR                      │
│   36 devices · 4 event types · 500+ events/sec                   │
│   Switches · Routers · Firewalls                                 │
└────────────────────────────────┬─────────────────────────────────┘
                                 │
                                 ▼  Apache Kafka
                                 │  Topic: network-events
                                 │  3 partitions · key = device_id
                                 │
┌────────────────────────────────▼─────────────────────────────────┐
│                         Delta Lake — ADLS Gen2                   │
│                                                                  │
│  ┌─────────────────┐  ┌──────────────────┐  ┌────────────────┐.  │
│  │     BRONZE       │  │      SILVER      │  │     ALERTS    │   │
│  │                  │  │                  │  │               │   │
│  │  Raw JSON events │  │  Typed fields    │  │  CRITICAL only│   │
│  │  30s micro-batch │→ │  Anomaly flags   │  │  10s latency  │   │
│  │  Schema-on-read  │  │  SLA breach flags│  │  Isolated path│   │
│  │                  │  │  60s micro-batch │  │               │   │
│  └─────────────────-┘  └────────┬────────-┘  └───────────────┘   │
│                                │                                 │
│                                ▼  Hourly batch                   │
│                          ┌─────────────────────────────────┐     │
│                          │             GOLD                │     │
│                          │  device_health_kpis             │     │
│                          │  alert_summary                  │     │
│                          │  network_reliability            │     │
│                          │  security_threats               │     │
│                          └─────────────────┬───────────────┘     │
└────────────────────────────────────────────┼───────────────────--┘
                                             │  Airflow DAG (hourly)
                                             │  Kafka health check
                                             │  COPY INTO Snowflake
                                             ▼
                                       Snowflake
                                    4 clustered tables
                                    5 analytics views
                                       Power BI
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Event streaming | Apache Kafka 3.x — 3 partitions, 24h retention |
| Stream processing | PySpark Structured Streaming — micro-batch |
| Storage format | Delta Lake 3.0 — ACID, checkpointing, time travel |
| Orchestration | Apache Airflow 2.8 — hourly DAG with branching |
| Serving layer | Snowflake — clustered tables, RBAC, 5 analytics views |
| Local development | Docker Compose — Kafka, Zookeeper, Kafka UI |
| Language | Python, PySpark, SQL |

---

## Event Types

| Event | Frequency | Key Metrics |
|---|---|---|
| `device_health` | Every 30s per device | CPU %, memory %, temperature °C, uptime |
| `interface_stats` | Every 60s per interface | Packet loss %, bandwidth utilization %, bytes in/out |
| `security_alert` | On detection | Alert type, source IP, protocol, packet count |
| `link_state` | On change | Interface state (up/down/flapping), reason |

---

## Repository Structure

```
├── data/
│   └── kafka_producer.py              # Simulates 36 network devices at 500+ events/sec
│
├── notebooks/
│   ├── 01_kafka_bronze_streaming.py   # Kafka → Bronze Delta (30s micro-batch)
│   ├── 02_silver_enrichment.py        # Parse payload, enrich, detect anomalies
│   └── 03_gold_kpis.py               # Hourly KPI aggregations → 4 Gold tables
│
├── airflow/
│   └── dags/
│       └── noc_pipeline_dag.py        # Hourly DAG — Kafka health check, branching, alerts
│
├── sql/
│   └── snowflake_setup.sql            # DDL, RBAC, 5 analytics views, 5 analytics queries
│
└── docker/
    └── docker-compose.yml             # Local Kafka + Zookeeper + Kafka UI
```

---

## Quick Start — Local Development

```bash
# Start local Kafka environment
cd docker
docker-compose up -d

# Create the topic
docker exec kafka kafka-topics.sh \
  --create --topic network-events \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

# Kafka UI available at http://localhost:8080

# Install Python dependencies
pip install -r requirements.txt

# Start event producer (runs until stopped with Ctrl+C)
python data/kafka_producer.py

# Run Bronze streaming notebook (separate terminal)
python notebooks/01_kafka_bronze_streaming.py

# After Bronze is running — process Silver and Gold
python notebooks/02_silver_enrichment.py
python notebooks/03_gold_kpis.py
```

---

## Key Engineering Decisions

| Decision | Rationale |
|---|---|
| 3 Kafka partitions | Matches Spark executor count. `device_id` as partition key preserves event ordering per device and ensures all events from one device land in the same partition |
| Dual micro-batch design | Main Silver stream at 60 seconds serves analytics. A separate 10-second stream isolates CRITICAL events — device down, DDoS detected — for faster operational response without running the full pipeline at high frequency |
| Delta Lake checkpointing | Exactly-once delivery on job restart. The checkpoint directory stores the last committed Kafka offset. On restart, Spark resumes from that offset — no duplicate records, no missed events |
| Airflow BranchPythonOperator | Checks whether Kafka topic has new messages before triggering Databricks cluster spin-up. At off-peak hours with no new events, the DAG exits cleanly without incurring compute cost |
| Snowflake clustering on `(event_date, location)` | NOC dashboard queries always filter on date range and location — clustering aligns micro-partition layout with the dominant query pattern |
