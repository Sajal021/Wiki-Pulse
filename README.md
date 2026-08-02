# WikiPulse — Real-Time Wikipedia Analytics Pipeline

WikiPulse is a real-time big data engineering platform designed to stream, ingest, process, and analyze edit events from Wikipedia in real time using a **Medallion Architecture** (Bronze → Silver → Gold).

---

## 🏗 System Architecture

```
                                  +-----------------------+
                                  | Wikimedia EventStream |
                                  |   (Server-Sent Events)|
                                  +-----------+-----------+
                                              |
                                              v
                                  +-----------+-----------+
                                  |  WikiPulse Producer   |
                                  |   (Python / SSEClient)|
                                  +-----------+-----------+
                                              |
                                              v
                                  +-----------+-----------+
                                  |     Apache Kafka      |
                                  |  (KRaft Mode Broker)  |
                                  +-----------+-----------+
                                              |
                                              v
                                  +-----------+-----------+
                                  |   Spark Streaming     |
                                  |   (Bronze Ingestion)  |
                                  +-----------+-----------+
                                              |
                                              v
                                  +-----------+-----------+
                                  | MinIO (S3 Lakehouse)  |
                                  |  wikipulse-bronze     |
                                  +-----------------------+
```

---

## 🚀 Completed Progress

### Phase 0: Infrastructure & Skeleton Setup
- **Docker Compose Orchestration**: Environment setup for all streaming, processing, storage, and UI services.
- **Kafka (KRaft Mode)**: Single-broker setup running Apache Kafka without Zookeeper.
- **MinIO Object Storage**: S3-compatible local storage initialized with three buckets: `wikipulse-bronze`, `wikipulse-silver`, `wikipulse-gold`.
- **Spark Cluster**: Apache Spark 3.5.5 Master & Worker cluster configured with Delta Lake and Hadoop AWS S3A support.
- **Airflow & Streamlit Containers**: Standalone Airflow orchestrator and Streamlit web UI placeholders ready for downstream phases.

### Phase 1: Live Wikimedia Event Stream Producer (`producer/producer.py`)
- Connects to Wikimedia's public EventStreams endpoint (`https://stream.wikimedia.org/v2/stream/recentchange`) via Server-Sent Events (SSE).
- Filters for article `edit` events (defaulting to English Wikipedia `enwiki`).
- Implements resilient client retry logic and Wikimedia User-Agent compliance.
- Keyed by article title and published to the `wikipedia-edits` Kafka topic.

### Phase 2: Bronze Layer Streaming Ingestion (`spark_jobs/bronze_ingestion.py`)
- Spark Structured Streaming job consuming continuously from Kafka.
- Strict schema enforcement preserving nested structures (`meta`, `length`, `revision`).
- Micro-batch processing appending raw events into a **Delta Lake** table (`s3a://wikipulse-bronze/edits`) on MinIO.
- Partitioned by `edit_date` with stateful checkpointing (`s3a://wikipulse-bronze/_checkpoints/bronze_ingestion`).

---

## 🛠 Service & Port Directory

| Service | Container Name | Description | Host URL / Port |
| :--- | :--- | :--- | :--- |
| **Kafka** | `wikipulse-kafka` | Message Broker (KRaft Mode) | `localhost:9092` (internal) / `localhost:9094` (external) |
| **MinIO API** | `wikipulse-minio` | S3-Compatible Object Storage | `http://localhost:9000` |
| **MinIO Console** | `wikipulse-minio` | MinIO Storage Web UI | `http://localhost:9001` (`wikipulse` / `wikipulse123`) |
| **Spark Master** | `wikipulse-spark-master` | Spark Cluster Master & Web UI | `http://localhost:8080` (UI) / `spark://spark-master:7077` |
| **Spark Worker** | `wikipulse-spark-worker` | Spark Cluster Worker | Managed by Master |
| **Bronze Ingestion** | `wikipulse-bronze-ingestion` | Spark Structured Streaming Job | Container log monitoring |
| **Airflow UI** | `wikipulse-airflow` | DAG Orchestrator | `http://localhost:8081` |
| **Streamlit UI** | `wikipulse-streamlit` | Real-time Dashboard Placeholder | `http://localhost:8501` |

---

## ⚙️ How to Run & Verify

### 1. Start the Environment
```bash
docker compose up -d --build
```

### 2. Verify Services & Streaming
- **Check Kafka Topics**:
  ```bash
  docker exec -it wikipulse-kafka kafka-topics --bootstrap-server localhost:9092 --list
  ```
  *(Should list `wikipedia-edits`)*

- **Check Producer Stream Logs**:
  ```bash
  docker compose logs -f producer
  ```

- **Check Spark Bronze Ingestion Logs**:
  ```bash
  docker compose logs -f bronze-ingestion
  ```

- **Verify MinIO Delta Lake Buckets**:
  Open `http://localhost:9001` in your browser. Inspect `wikipulse-bronze/edits` for generated Delta log and parquet partition files.

---

## 📌 Architecture Decisions & Highlights

- **KRaft Mode Kafka**: Eliminates Zookeeper dependency for a lighter, modern dev cluster setup.
- **S3A & Delta Lake on MinIO**: Enables enterprise lakehouse patterns locally with zero AWS cloud costs.
- **Partitioning Strategy**:
  - Kafka messages are keyed by **article title** to align partition ordering for windowed edit analytics.
  - Bronze Delta table is partitioned by **`edit_date`** (derived from event timestamp) for optimized query pruning.

---

## 🗺 Roadmap / Architecture Phases

- [x] **Phase 0 — Environment Setup**
  - Docker Compose skeleton: Kafka (KRaft), Spark Master/Worker, Airflow, MinIO (S3-compatible local storage), Streamlit.
  - Ensures container networking, volume mounting, and environment connectivity before business logic.

- [x] **Phase 1 — Ingestion (Kafka Producer)**
  - Lightweight Python producer connecting to Wikipedia's public EventStreams endpoint (`stream.wikimedia.org`, `recentchange` topic) via Server-Sent Events (SSE).
  - Parses events (article title, user, bot flag, timestamp, wiki/language, edit size) and publishes to Kafka (`wikipedia-edits`).
  - **Milestone**: Raw edits flowing into Kafka.

- [x] **Phase 2 — Bronze Layer (Raw Storage)**
  - Spark Structured Streaming job reads from Kafka, writes raw events as-is into a Bronze Delta table on MinIO (`s3a://wikipulse-bronze/edits`).
  - Immutable source of truth / replay buffer partitioned by `edit_date`.

- [ ] **Phase 3 — Bot Detection (scikit-learn)**
  - Train a classifier on edit patterns (edit frequency per account, edit size distribution, time-of-day regularity, comment patterns) to catch unflagged bots missed by raw Wikimedia flags.
  - Batch job (orchestrated by Airflow): periodically retrains on Bronze data, outputs bot-probability scores.
  - **Milestone**: Trained model artifact + scored dataset of "likely bot" vs "likely human" edits.

- [ ] **Phase 4 — Silver Layer (Cleaned + Enriched)**
  - Spark job reads Bronze, joins in bot scores, filters/flags bot noise, standardizes schema (deduplication, null-handling, timezone normalization).
  - Writes to Silver Delta table (`s3a://wikipulse-silver/edits`) — the clean, queryable, trustworthy layer.

- [ ] **Phase 5 — Anomaly Detection (Core Streaming Logic)**
  - Spark Structured Streaming with windowed aggregations: computes rolling edit velocity per article (e.g., 5-minute tumbling/sliding windows).
  - Compares against rolling baselines (e.g., z-score vs. trailing average or EWMA-based control limits).
  - Flags articles crossing anomaly thresholds as "event candidates" into a dedicated events Delta table / fast-access sink.
  - **Milestone**: Simulated edit spikes get flagged automatically within seconds.

- [ ] **Phase 6 — Gold Layer + dbt**
  - dbt models sitting on top of Silver: daily/hourly rollups, top trending articles, historical anomaly logs, per-language breakdowns.
  - Analytical, versioned, testable SQL transformations.
  - **Milestone**: Documented and tested dbt transformation layer.

- [ ] **Phase 7 — Airflow Orchestration**
  - DAG for batch operations: nightly bot-classifier retraining → dbt run → Gold layer refresh → data quality checks.
  - Keeps streaming jobs (Bronze/Silver/Anomaly detection) always-on outside Airflow, orchestrating batch workflows cleanly.

- [ ] **Phase 8 — Streamlit Dashboard**
  - Live feed of flagged anomalies (e.g., *"🔴 Spike detected: Article X, edits up 8x baseline"*).
  - Trending articles chart, edit velocity time series, bot-vs-human split, and historical event log from Gold tables.

