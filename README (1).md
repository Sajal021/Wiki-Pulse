# WikiPulse — Phase 0: Environment Setup

This is the environment skeleton. Nothing here does real work yet — the goal
of Phase 0 is just to get every container up, healthy, and able to talk to
each other before we write any business logic.

## What's included

| Service        | Purpose                                      | Port(s)          |
|----------------|-----------------------------------------------|-------------------|
| `kafka`        | Message broker — `confluentinc/cp-kafka` in KRaft mode, no Zookeeper | 9092, 9094 |
| `minio`        | S3-compatible storage (stand-in for AWS S3)   | 9000 (API), 9001 (console) |
| `minio-init`   | One-shot job that creates the bronze/silver/gold buckets | — |
| `spark-master` | Spark cluster master                          | 8080 (UI), 7077   |
| `spark-worker` | Spark cluster worker                          | —                 |
| `airflow`      | Standalone dev Airflow (batch DAGs only)      | 8081              |
| `streamlit`    | Dashboard placeholder                         | 8501              |

## How to run

```bash
cd wikipulse
docker compose up -d
```

First boot will take a few minutes (pulling images). Watch startup with:

```bash
docker compose logs -f
```

## How to verify Phase 0 is actually working

1. **Kafka is up:**
   ```bash
   docker exec -it wikipulse-kafka kafka-topics --bootstrap-server localhost:9092 --list
   ```
   Should return with no error (empty topic list is fine — we haven't created any yet).

2. **MinIO buckets exist:** open http://localhost:9001 in a browser
   (login: `wikipulse` / `wikipulse123`) — you should see three buckets:
   `wikipulse-bronze`, `wikipulse-silver`, `wikipulse-gold`.

3. **Spark cluster is up:** open http://localhost:8080 — you should see
   1 worker registered under the master.

4. **Airflow is up:** open http://localhost:8081 — the standalone command
   prints an auto-generated admin password to the container logs on first
   run (`docker compose logs airflow | grep password`).

5. **Streamlit placeholder is up:** open http://localhost:8501 — you should
   see the WikiPulse placeholder page.

## Notes / decisions made here

- **Kafka in KRaft mode** (no Zookeeper container) — simpler for a
  single-broker dev setup, and it's the direction Kafka itself is moving
  (Zookeeper is being phased out project-wide), so it's a fine thing to
  mention in an interview.
- **MinIO instead of real AWS S3** for local dev — it speaks the S3 API,
  so the same `boto3`/Spark S3A code will work against real AWS later with
  just a config/env change. No AWS costs during development.
- **Airflow in `standalone` mode with SQLite** — deliberately minimal for
  now. This is fine for a single-machine dev DAG; if this ever needed to
  run production-like with parallel task execution, this is the piece
  we'd swap for Postgres + CeleryExecutor.
- Nothing is training or streaming data yet — that starts in Phase 1
  (the Kafka producer that reads Wikipedia's live edit stream).

## Next: Phase 1

Build the Python producer that connects to Wikipedia's public
`recentchange` EventStream and publishes edits onto a Kafka topic.
