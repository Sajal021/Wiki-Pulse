"""
WikiPulse — Phase 2: Bronze Layer Ingestion
=============================================
Spark Structured Streaming job that reads raw Wikipedia edit events from the
`wikipedia-edits` Kafka topic and writes them, untouched, into a Bronze Delta
table on MinIO (S3-compatible storage).

This is the "Load" step of our ELT pipeline: no cleaning, no filtering, no
bot removal happens here — that's Silver's job (Phase 4). Bronze exists so
that once data has been written here, Kafka's 24-hour retention window no
longer matters — this is the permanent, replayable source of truth.
"""

import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime, to_date
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("wikipulse-bronze")

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "wikipedia-edits"

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "wikipulse"
MINIO_SECRET_KEY = "wikipulse123"

BRONZE_TABLE_PATH = "s3a://wikipulse-bronze/edits"
CHECKPOINT_PATH = "s3a://wikipulse-bronze/_checkpoints/bronze_ingestion"


# Explicit schema for the Wikimedia recentchange 'edit' event, based on the
# actual fields observed in the live stream. Nested `meta`, `length`, and
# `revision` objects are modeled as structs rather than flattened, so the
# raw shape of the source data is preserved as faithfully as possible.
EDIT_SCHEMA = StructType([
    StructField("$schema", StringType()),
    StructField("meta", StructType([
        StructField("uri", StringType()),
        StructField("request_id", StringType()),
        StructField("id", StringType()),
        StructField("domain", StringType()),
        StructField("stream", StringType()),
        StructField("dt", StringType()),
        StructField("topic", StringType()),
        StructField("partition", LongType()),
        StructField("offset", LongType()),
    ])),
    StructField("id", LongType()),
    StructField("type", StringType()),
    StructField("namespace", LongType()),
    StructField("title", StringType()),
    StructField("title_url", StringType()),
    StructField("comment", StringType()),
    StructField("timestamp", LongType()),
    StructField("user", StringType()),
    StructField("bot", BooleanType()),
    StructField("notify_url", StringType()),
    StructField("minor", BooleanType()),
    StructField("length", StructType([
        StructField("old", LongType()),
        StructField("new", LongType()),
    ])),
    StructField("revision", StructType([
        StructField("old", LongType()),
        StructField("new", LongType()),
    ])),
    StructField("server_url", StringType()),
    StructField("server_name", StringType()),
    StructField("server_script_path", StringType()),
    StructField("wiki", StringType()),
    StructField("parsedcomment", StringType()),
])


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("WikiPulse-Bronze-Ingestion")
        # Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # S3A (MinIO) connectivity
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def log_batch_progress(batch_df, batch_id: int) -> None:
    """Called on every micro-batch so we can see progress, the same way the
    producer logs a heartbeat — this is our visibility into whether the job
    is actually doing anything, not just running."""
    count = batch_df.count()
    logger.info("Batch %d: writing %d edit(s) to Bronze", batch_id, count)
    batch_df.write.format("delta").mode("append").partitionBy("edit_date").save(BRONZE_TABLE_PATH)


def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # Spark's own logs are noisy; keep ours visible

    logger.info("Starting Bronze ingestion — reading from Kafka topic '%s'", KAFKA_TOPIC)

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), EDIT_SCHEMA).alias("data"))
        .select("data.*")
        # Partition column: the calendar date the edit happened on, derived
        # from the event's own unix timestamp (not Kafka's ingestion time —
        # we want this to reflect when the edit actually occurred).
        .withColumn("edit_date", to_date(from_unixtime(col("timestamp"))))
    )

    query = (
        parsed.writeStream
        .foreachBatch(log_batch_progress)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="300 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
