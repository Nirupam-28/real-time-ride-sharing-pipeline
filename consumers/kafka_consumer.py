
import json
import logging
import signal
from collections import Counter

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from producer.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC
)


# ----------------------------------
# Logging configuration
# ----------------------------------

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s | %(levelname)s | %(message)s"
# )

# logger = logging.getLogger(__name__)

from common.logging_config import setup_logging

logger = setup_logging("kafka_consumer")

# ----------------------------------
# Consumer configuration
# ----------------------------------

CONSUMER_GROUP_ID = "ride-processing-group"

VALID_EVENT_TYPES = {
    "RIDE_REQUESTED",
    "DRIVER_ASSIGNED",
    "RIDE_COMPLETED"
}

REQUIRED_FIELDS = [
    "event_id",
    "ride_id",
    "event_type",
    "customer_id",
    "city",
    "zone",
    "event_timestamp"
]


# ----------------------------------
# Graceful shutdown
# ----------------------------------

running = True


def shutdown_handler(signum, frame):

    global running

    logger.info("Shutdown signal received")

    running = False


signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)


# ----------------------------------
# Create Kafka Consumer
# ----------------------------------

def create_consumer():

    consumer = KafkaConsumer(

        KAFKA_TOPIC,

        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

        group_id=CONSUMER_GROUP_ID,

        # Start from earliest if this group
        # has no previously committed offset
        auto_offset_reset="earliest",

        # Disable automatic commits
        enable_auto_commit=False,

        # Control the number of records per poll
        max_poll_records=100,

        # Wait for messages
        consumer_timeout_ms=1000,

        # Consumer identification
        client_id="ride-python-consumer"
    )

    logger.info("Kafka consumer connected")

    return consumer


# ----------------------------------
# Parse JSON
# ----------------------------------

def parse_event(message):

    try:

        event = json.loads(message.value.decode("utf-8"))

        if not isinstance(event, dict):
            raise ValueError("Event must be a JSON object")

        return event

    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):

        logger.exception(
            "Invalid JSON message | Topic=%s | Partition=%s | Offset=%s",
            message.topic,
            message.partition,
            message.offset
        )

        return None


# ----------------------------------
# Validate event
# ----------------------------------

def validate_event(event):

    if event is None:
        return False

    # Check mandatory fields
    for field in REQUIRED_FIELDS:

        if field not in event or event[field] is None:

            logger.warning(
                "Missing mandatory field: %s",
                field
            )

            return False

    # Validate event type
    if event["event_type"] not in VALID_EVENT_TYPES:

        logger.warning(
            "Invalid event type: %s",
            event["event_type"]
        )

        return False

    # Validate ride ID
    if not isinstance(event["ride_id"], str):

        logger.warning("Invalid ride_id")

        return False

    # Validate event ID
    if not isinstance(event["event_id"], str):

        logger.warning("Invalid event_id")

        return False

    return True


# ----------------------------------
# Process event
# ----------------------------------

def process_event(event):

    event_type = event["event_type"]

    if event_type == "RIDE_REQUESTED":

        logger.info(
            "NEW RIDE | Ride=%s | Zone=%s",
            event["ride_id"],
            event["zone"]
        )

    elif event_type == "DRIVER_ASSIGNED":

        logger.info(
            "DRIVER ASSIGNED | Ride=%s | Driver=%s",
            event["ride_id"],
            event.get("driver_id")
        )

    elif event_type == "RIDE_COMPLETED":

        logger.info(
            "RIDE COMPLETED | Ride=%s",
            event["ride_id"]
        )


# ----------------------------------
# Main consumer application
# ----------------------------------

def consume_events():

    consumer = create_consumer()

    stats = Counter()

    try:

        logger.info(
            "Listening to topic: %s",
            KAFKA_TOPIC
        )

        while running:

            # Fetch a batch of messages
            records = consumer.poll(
                timeout_ms=1000,
                max_records=100
            )

            if not records:
                continue

            batch_processed = 0

            # Process records partition by partition
            for topic_partition, messages in records.items():

                for message in messages:

                    event = parse_event(message)

                    # Invalid messages are temporarily
                    # logged and skipped.
                    if not validate_event(event):

                        stats["invalid"] += 1

                        logger.error(
                            "Invalid event skipped | Partition=%s | Offset=%s",
                            message.partition,
                            message.offset
                        )

                        continue

                    # Process valid event
                    process_event(event)

                    stats[event["event_type"]] += 1

                    batch_processed += 1

            # Commit only after the entire fetched batch
            # has been handled successfully.
            consumer.commit()

            stats["batches"] += 1

            logger.info(
                "Batch completed | Valid events=%s | Total valid=%s",
                batch_processed,
                sum(
                    stats[event_type]
                    for event_type in VALID_EVENT_TYPES
                )
            )

    except KafkaError:

        logger.exception("Kafka consumer error")

        raise

    except Exception:

        # Unexpected processing failure:
        # do not commit the current fetched batch.
        logger.exception("Unexpected processing failure")

        raise

    finally:

        logger.info("Closing Kafka consumer")

        consumer.close()

        logger.info("========== CONSUMER SUMMARY ==========")

        logger.info("Requested: %s", stats["RIDE_REQUESTED"])

        logger.info("Assigned: %s", stats["DRIVER_ASSIGNED"])

        logger.info("Completed: %s", stats["RIDE_COMPLETED"])

        logger.info("Invalid: %s", stats["invalid"])

        logger.info("Batches: %s", stats["batches"])

        logger.info("=======================================")


if __name__ == "__main__":

    consume_events()
