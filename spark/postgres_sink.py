
import logging

import psycopg2

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, lit
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)
from pyspark.storagelevel import StorageLevel

from common.config import Config
from common.retry import retry
from common.logging_config import setup_logging

logger = setup_logging("postgres_sink")

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = Config.KAFKA_BOOTSTRAP_SERVERS
SOURCE_TOPIC = Config.KAFKA_TOPIC

# PostgreSQL JDBC configuration
jdbc_url = (
    f"jdbc:postgresql://{Config.POSTGRES_HOST}:"
    f"{Config.POSTGRES_PORT}/{Config.POSTGRES_DB}"
    "?connectTimeout=5"
)

connection_properties = {
    "user": Config.POSTGRES_USER,
    "password": Config.POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Spark checkpoint
CHECKPOINT_LOCATION = "spark/checkpoints/postgres_sink"

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

logger = setup_logging("postgres_sink")

# --------------------------------------------------
# 2. Event schema
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
# 3. Spark session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("RideSharingPostgresSink")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.sql.session.timeZone", "UTC")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,"
        "org.postgresql:postgresql:42.7.4"
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
# 5. Parse events and preserve Kafka metadata
# --------------------------------------------------

parsed_stream = kafka_stream.select(
    col("value").cast("string").alias("json_value"),
    col("topic").alias("source_topic"),
    col("partition").alias("source_partition"),
    col("offset").alias("source_offset"),
    col("timestamp").alias("kafka_timestamp")
).withColumn(
    "data",
    from_json(col("json_value"), ride_schema)
)


# --------------------------------------------------
# 6. Extract structured fields
# --------------------------------------------------

events = parsed_stream.select(
    col("data.event_id").alias("event_id"),
    col("data.ride_id").alias("ride_id"),
    col("data.event_type").alias("event_type"),
    col("data.customer_id").alias("customer_id"),
    col("data.driver_id").alias("driver_id"),
    col("data.city").alias("city"),
    col("data.zone").alias("zone"),
    col("data.latitude").alias("latitude"),
    col("data.longitude").alias("longitude"),
    col("data.dropoff_latitude").alias("dropoff_latitude"),
    col("data.dropoff_longitude").alias("dropoff_longitude"),
    to_timestamp(
        col("data.event_timestamp")
    ).alias("event_timestamp"),
    col("kafka_timestamp"),
    col("source_topic"),
    col("source_partition"),
    col("source_offset")
)


# --------------------------------------------------
# 7. Filter invalid events
# --------------------------------------------------

valid_events = events.filter(
    col("event_id").isNotNull()
    & col("ride_id").isNotNull()
    & col("event_type").isin(VALID_EVENT_TYPES)
    & col("city").isNotNull()
    & col("zone").isNotNull()
    & col("event_timestamp").isNotNull()
)


# --------------------------------------------------
# 8. Database merge operation
# --------------------------------------------------

@retry(
    attempts=Config.RETRY_ATTEMPTS,
    base_delay=Config.RETRY_BASE_DELAY,
    exceptions=(psycopg2.OperationalError,)
)
def merge_staging_batch(batch_id):

    connection = psycopg2.connect(
        host=Config.POSTGRES_HOST,
        port=Config.POSTGRES_PORT,
        database=Config.POSTGRES_DB,
        user=Config.POSTGRES_USER,
        password=Config.POSTGRES_PASSWORD,
        connect_timeout=5
    )

    try:
        with connection:
            with connection.cursor() as cursor:

                cursor.execute(
                    """
                    INSERT INTO ride_events (
                        event_id,
                        ride_id,
                        event_type,
                        customer_id,
                        driver_id,
                        city,
                        zone,
                        latitude,
                        longitude,
                        dropoff_latitude,
                        dropoff_longitude,
                        event_timestamp,
                        kafka_timestamp,
                        source_topic,
                        source_partition,
                        source_offset
                    )
                    SELECT DISTINCT ON (event_id)
                        event_id,
                        ride_id,
                        event_type,
                        customer_id,
                        driver_id,
                        city,
                        zone,
                        latitude,
                        longitude,
                        dropoff_latitude,
                        dropoff_longitude,
                        event_timestamp,
                        kafka_timestamp,
                        source_topic,
                        source_partition,
                        source_offset
                    FROM ride_events_staging
                    WHERE batch_id = %s
                    ORDER BY event_id, ingested_at DESC
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    (batch_id,)
                )

                inserted_rows = cursor.rowcount

                cursor.execute(
                    """
                    DELETE FROM ride_events_staging
                    WHERE batch_id = %s
                    """,
                    (batch_id,)
                )

        logger.info(
            "Batch %s merged. New records: %s",
            batch_id,
            inserted_rows
        )

    except Exception:
        connection.rollback()
        logger.exception(
            "PostgreSQL merge failed for batch %s",
            batch_id
        )
        raise

    finally:
        connection.close()


# --------------------------------------------------
# 9. Process every Spark micro-batch
# --------------------------------------------------

def process_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        logger.info("Batch %s is empty", batch_id)
        return

    batch_df.persist(StorageLevel.MEMORY_AND_DISK)

    try:
        batch_count = batch_df.count()

        logger.info(
            "Processing batch %s with %s valid events",
            batch_id,
            batch_count
        )

        if batch_count == 0:
            return

        staging_df = batch_df.withColumn(
            "batch_id",
            lit(batch_id).cast("long")
        )

        logger.info("Writing batch %s to staging", batch_id)

        (
            staging_df.write
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "ride_events_staging")
            .options(**connection_properties)
            .mode("append")
            .save()
        )

        logger.info("Batch %s staging write completed", batch_id)

        merge_staging_batch(batch_id)

    except Exception:
        logger.exception(
            "Failed to process PostgreSQL batch %s",
            batch_id
        )
        raise

    finally:
        batch_df.unpersist()


# --------------------------------------------------
# 10. Start streaming query
# --------------------------------------------------

query = (
    valid_events.writeStream
    .queryName("postgres_ride_events_sink")
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(processingTime="5 seconds")
    .start()
)

logger.info("PostgreSQL streaming sink started")

try:
    query.awaitTermination()

except KeyboardInterrupt:
    logger.info("Stopping PostgreSQL streaming sink")

finally:
    query.stop()
    spark.stop()
