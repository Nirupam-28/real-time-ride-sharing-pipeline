import argparse
import json
import logging

from collections import Counter

from kafka import KafkaProducer
from kafka.errors import KafkaError

from producer.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    PRODUCER_CLIENT_ID
)

from producer.event_generator import generate_events
from common.config import Config

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s | %(levelname)s | %(message)s"
# )

# logger = logging.getLogger(__name__)

from common.logging_config import setup_logging

logger = setup_logging("kafka_producer")


def create_producer():

    return KafkaProducer(

        bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,

        client_id=PRODUCER_CLIENT_ID,

        # Serialize ride_id into bytes
        key_serializer=lambda key: key.encode("utf-8"),

        # Serialize Python dictionary into JSON bytes
        value_serializer=lambda value: json.dumps(
            value,
            separators=(",", ":")
        ).encode("utf-8"),

        # Reliability settings
        acks="all",
        enable_idempotence=True,

        # Batching and compression
        linger_ms=10,
        batch_size=32768,
        compression_type="gzip",

        # Producer connection settings
        max_in_flight_requests_per_connection=5
    )


def send_event(producer, event):
    # Keep the key as a string.
    # Kafka's key_serializer will encode it.
    event_key = event["ride_id"]

    try:
        future = producer.send(
            Config.KAFKA_TOPIC,
            key=event_key,
            value=event
        )

        metadata = future.get(timeout=30)

        logger.info(
            "Event published | event_id=%s | partition=%s | offset=%s",
            event["event_id"],
            metadata.partition,
            metadata.offset
        )

    except KafkaError:
        logger.exception(
            "Failed to publish event_id=%s",
            event.get("event_id")
        )
        raise



def publish_events(
    rides_per_minute,
    duration_seconds,
    min_wait_seconds,
    max_wait_seconds,
    min_ride_seconds,
    max_ride_seconds
):

    producer = None

    event_counts = Counter()

    total_events = 0

    try:

        logger.info(
            "Connecting to Kafka: %s",
            KAFKA_BOOTSTRAP_SERVERS
        )

        producer = create_producer()

        logger.info("Kafka producer connected successfully")

        events = generate_events(
            rides_per_minute=rides_per_minute,
            duration_seconds=duration_seconds,
            min_wait_seconds=min_wait_seconds,
            max_wait_seconds=max_wait_seconds,
            min_ride_seconds=min_ride_seconds,
            max_ride_seconds=max_ride_seconds
        )

        for event in events:
            send_event(producer, event)

            event_counts[event["event_type"]] += 1

            total_events += 1

        producer.flush(timeout=30)

        logger.info("All events successfully acknowledged")

        logger.info("========== PRODUCER SUMMARY ==========")

        logger.info("Total events: %s", total_events)

        for event_type, count in event_counts.items():

            logger.info("%s: %s", event_type, count)

        logger.info("======================================")

    except KafkaError:

        logger.exception("Kafka publishing failed")
        raise

    finally:

        if producer is not None:
            producer.close(timeout=10)


def main():

    parser = argparse.ArgumentParser(
        description="Real-Time Ride-Sharing Event Simulator"
    )

    parser.add_argument(
        "--rides-per-minute",
        type=int,
        default=60
    )

    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=60
    )

    parser.add_argument(
        "--min-wait-seconds",
        type=float,
        default=2
    )

    parser.add_argument(
        "--max-wait-seconds",
        type=float,
        default=8
    )

    parser.add_argument(
        "--min-ride-seconds",
        type=float,
        default=10
    )

    parser.add_argument(
        "--max-ride-seconds",
        type=float,
        default=30
    )

    args = parser.parse_args()

    publish_events(
        rides_per_minute=args.rides_per_minute,
        duration_seconds=args.duration_seconds,
        min_wait_seconds=args.min_wait_seconds,
        max_wait_seconds=args.max_wait_seconds,
        min_ride_seconds=args.min_ride_seconds,
        max_ride_seconds=args.max_ride_seconds
    )


if __name__ == "__main__":
    main()
