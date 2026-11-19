
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_json,
    to_timestamp,
    struct,
    when
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

# --------------------------------------------------
# 1. Configuration
# --------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SOURCE_TOPIC = "ride_events"
DLQ_TOPIC = "ride_events_dlq"

CHECKPOINT_LOCATION = "spark/checkpoints/ride_validation"

VALID_EVENT_TYPES = [
    "RIDE_REQUESTED",
    "DRIVER_ASSIGNED",
    "RIDE_COMPLETED"
]

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s | %(levelname)s | %(message)s"
# )
# logger = logging.getLogger(__name__)

from common.logging_config import setup_logging

logger = setup_logging("ride_validation")


# --------------------------------------------------
# 2. Define JSON schema
# --------------------------------------------------

ride_schema = StructType([
    StructField("schema_version", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("ride_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("event_timestamp", StringType(), True)
])


# --------------------------------------------------
# 3. Create Spark session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("RideSharingDataValidation")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "3")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

logger.info("Spark session created")


# --------------------------------------------------
# 4. Read from Kafka
# --------------------------------------------------

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", SOURCE_TOPIC)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "500")
    .load()
)


# --------------------------------------------------
# 5. Extract Kafka message information
# --------------------------------------------------

raw_stream = kafka_stream.select(
    col("key").cast("string").alias("kafka_key"),
    col("value").cast("string").alias("json_value"),
    col("topic").alias("source_topic"),
    col("partition").alias("source_partition"),
    col("offset").alias("source_offset"),
    col("timestamp").alias("kafka_timestamp")
)


# --------------------------------------------------
# 6. Parse JSON
# --------------------------------------------------

parsed_stream = raw_stream.withColumn(
    "data",
    from_json(col("json_value"), ride_schema)
)


# --------------------------------------------------
# 7. Apply validation rules
# --------------------------------------------------

validated_stream = parsed_stream.withColumn(
    "error_reason",

    when(
        col("data").isNull(),
        "INVALID_JSON_OR_SCHEMA"
    )
    .when(
        col("data.event_id").isNull(),
        "MISSING_EVENT_ID"
    )
    .when(
        col("data.ride_id").isNull(),
        "MISSING_RIDE_ID"
    )
    .when(
        col("data.event_type").isNull(),
        "MISSING_EVENT_TYPE"
    )
    .when(
        ~col("data.event_type").isin(VALID_EVENT_TYPES),
        "INVALID_EVENT_TYPE"
    )
    .when(
        col("data.customer_id").isNull(),
        "MISSING_CUSTOMER_ID"
    )
    .when(
        col("data.city").isNull(),
        "MISSING_CITY"
    )
    .when(
        col("data.zone").isNull(),
        "MISSING_ZONE"
    )
    .when(
        col("data.event_timestamp").isNull(),
        "MISSING_EVENT_TIMESTAMP"
    )
    .when(
        to_timestamp(col("data.event_timestamp")).isNull(),
        "INVALID_EVENT_TIMESTAMP"
    )
)


# --------------------------------------------------
# 8. Split valid and invalid events
# --------------------------------------------------

valid_events = validated_stream.filter(
    col("error_reason").isNull()
)

invalid_events = validated_stream.filter(
    col("error_reason").isNotNull()
)


# --------------------------------------------------
# 9. Process each micro-batch
# --------------------------------------------------

def process_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    batch_df.cache()

    try:
        valid_batch = batch_df.filter(
            col("error_reason").isNull()
        )

        invalid_batch = batch_df.filter(
            col("error_reason").isNotNull()
        )

        valid_count = valid_batch.count()
        invalid_count = invalid_batch.count()

        logger.info(
            "Batch ID: %s | Valid: %s | Invalid: %s",
            batch_id,
            valid_count,
            invalid_count
        )

        # ------------------------------------------
        # Process valid events
        # ------------------------------------------

        if valid_count > 0:

            print("\n========== VALID EVENTS ==========")

            valid_batch.select(
                col("data.event_id").alias("event_id"),
                col("data.ride_id").alias("ride_id"),
                col("data.event_type").alias("event_type"),
                col("data.city").alias("city"),
                col("data.zone").alias("zone"),
                to_timestamp(
                    col("data.event_timestamp")
                ).alias("event_timestamp")
            ).show(20, truncate=False)

        # ------------------------------------------
        # Send invalid events to Kafka DLQ
        # ------------------------------------------

        if invalid_count > 0:

            print("\n========== INVALID EVENTS ==========")

            invalid_batch.select(
                "json_value",
                "error_reason"
            ).show(20, truncate=False)

            dlq_records = invalid_batch.select(
                col("kafka_key").cast("string").alias("key"),

                to_json(
                    struct(
                        col("json_value").alias("original_payload"),
                        col("error_reason"),
                        col("source_topic"),
                        col("source_partition"),
                        col("source_offset"),
                        col("kafka_timestamp")
                    )
                ).alias("value")
            )

            (
                dlq_records.write
                .format("kafka")
                .option(
                    "kafka.bootstrap.servers",
                    KAFKA_BOOTSTRAP_SERVERS
                )
                .option("topic", DLQ_TOPIC)
                .save()
            )

            logger.info(
                "Sent %s invalid records to DLQ",
                invalid_count
            )

    finally:
        batch_df.unpersist()


# --------------------------------------------------
# 10. Start streaming query
# --------------------------------------------------

query = (
    validated_stream.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(processingTime="5 seconds")
    .start()
)

logger.info("Ride validation streaming application started")

try:
    query.awaitTermination()

except KeyboardInterrupt:
    logger.info("Stopping streaming application")

finally:
    query.stop()
    spark.stop()
