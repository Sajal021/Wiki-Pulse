"""
WikiPulse — Phase 1: Wikipedia Edit Stream Producer
====================================================
Connects to Wikipedia's public EventStreams endpoint (Server-Sent Events),
reads every edit happening across all Wikimedia projects in real time, and
publishes each event onto a Kafka topic for downstream processing.

Source: https://stream.wikimedia.org/v2/stream/recentchange
This is a public, unauthenticated endpoint maintained by the Wikimedia
Foundation — no API key needed.
"""

import json
import logging
import os
import time

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from sseclient import SSEClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("wikipulse-producer")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "wikipedia-edits")
WIKIMEDIA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

# Wikimedia requires a descriptive User-Agent identifying the client and a
# contact method — requests without one are rejected with 403. See:
# https://meta.wikimedia.org/wiki/User-Agent_policy
REQUEST_HEADERS = {
    "User-Agent": "WikiPulse/0.1 (portfolio project; contact: replace-with-your-email@example.com)"
}

# Only these wikis, to keep volume manageable while we're building/testing.
# Set to None to take the full, unfiltered global firehose (very high volume).
WIKI_FILTER = {"enwiki"}  # English Wikipedia only, for now


def connect_kafka_producer(retry_delay_seconds: int = 5) -> KafkaProducer:
    """
    Kafka may not be up yet when this container starts — especially after a
    Docker Desktop restart, where containers come back independently rather
    than in dependency order. Retry indefinitely rather than giving up, so
    this container just waits calmly instead of crash-looping.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=5,
            )
            logger.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            if attempt % 6 == 0:  # log roughly once every 30s, not every 5s
                logger.warning(
                    "Kafka still not reachable after %d attempts — still retrying every %ds",
                    attempt, retry_delay_seconds,
                )
            time.sleep(retry_delay_seconds)


def stream_wikipedia_edits(producer: KafkaProducer) -> None:
    """
    Connects to the Wikimedia EventStreams SSE endpoint and publishes each
    relevant edit event to Kafka. Runs forever — SSE streams are long-lived
    and Wikimedia expects consumers to just keep the connection open.
    """
    logger.info("Connecting to Wikimedia event stream: %s", WIKIMEDIA_STREAM_URL)
    messages_sent = 0
    last_log_time = time.time()

    while True:
        try:
            response = requests.get(
                WIKIMEDIA_STREAM_URL,
                stream=True,
                timeout=(10, 60),
                headers=REQUEST_HEADERS,
            )
            logger.info("Stream HTTP response status: %s", response.status_code)

            if response.status_code != 200:
                logger.error(
                    "Non-200 response (%s) — backing off 10s before retry",
                    response.status_code,
                )
                time.sleep(10)
                continue

            client = SSEClient(response)
            for event in client.events():
                if time.time() - last_log_time > 15:
                    logger.info(
                        "Heartbeat — still receiving stream data. Messages sent so far: %d",
                        messages_sent,
                    )
                    last_log_time = time.time()

                if not event.data:
                    continue
                try:
                    change = json.loads(event.data)
                except json.JSONDecodeError:
                    continue

                # We only care about actual page edits (not log actions,
                # new user creations, etc.) for now.
                if change.get("type") != "edit":
                    continue

                wiki = change.get("wiki")
                if WIKI_FILTER is not None and wiki not in WIKI_FILTER:
                    continue

                # Key by article title so all edits to the same article
                # land on the same Kafka partition — this matters later
                # for windowed edit-velocity aggregation in Spark.
                key = change.get("title", "")
                producer.send(KAFKA_TOPIC, key=key, value=change)
                messages_sent += 1

            # If we fall out of the for-loop, the stream ended on its own
            # (no exception) — still worth a short pause before reconnecting.
            logger.warning("Stream ended unexpectedly — reconnecting in 5s")
            time.sleep(5)

        except Exception as exc:  # noqa: BLE001 - we want this loop to never die
            logger.error("Stream connection dropped (%s) — reconnecting in 5s", exc)
            time.sleep(5)


def main() -> None:
    producer = connect_kafka_producer()
    stream_wikipedia_edits(producer)


if __name__ == "__main__":
    main()