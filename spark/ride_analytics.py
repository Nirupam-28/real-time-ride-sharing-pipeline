
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    sum as spark_sum,
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

BASE_CHECKPOINT = "spark/checkpoints/ride_analytics"

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

logger = setup_logging("ride_analytics")


# --------------------------------------------------
# 2. Define the event schema
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
    .appName("RideSharingRealTimeAnalytics")
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

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", SOURCE_TOPIC)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "500")
    .load()
)


# --------------------------------------------------
# 5. Parse JSON and extract columns
# --------------------------------------------------

parsed_stream = raw_stream.select(
    col("value").cast("string").alias("json_value")
).withColumn(
    "data",
    from_json(col("json_value"), ride_schema)
)

events = parsed_stream.select(
    col("data.event_id").alias("event_id"),
    col("data.ride_id").alias("ride_id"),
    col("data.event_type").alias("event_type"),
    col("data.customer_id").alias("customer_id"),
    col("data.driver_id").alias("driver_id"),
    col("data.city").alias("city"),
    col("data.zone").alias("zone"),
    to_timestamp(
        col("data.event_timestamp")
    ).alias("event_timestamp")
)


# --------------------------------------------------
# 6. Filter valid events
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
# 7. Query 1: Overall ride metrics
# --------------------------------------------------

overall_metrics = valid_events.groupBy().agg(

    spark_sum(
        when(col("event_type") == "RIDE_REQUESTED", 1)
        .otherwise(0)
    ).alias("total_requests"),

    spark_sum(
        when(col("event_type") == "DRIVER_ASSIGNED", 1)
        .otherwise(0)
    ).alias("total_assignments"),

    spark_sum(
        when(col("event_type") == "RIDE_COMPLETED", 1)
        .otherwise(0)
    ).alias("total_completed")

).withColumn(
    "active_rides",
    col("total_requests") - col("total_completed")
)


overall_query = (
    overall_metrics.writeStream
    .queryName("overall_ride_metrics")
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", f"{BASE_CHECKPOINT}/overall")
    .trigger(processingTime="5 seconds")
    .start()
)


# --------------------------------------------------
# 8. Query 2: Ride requests by zone
# --------------------------------------------------

zone_metrics = (
    valid_events
    .filter(col("event_type") == "RIDE_REQUESTED")
    .groupBy("zone")
    .count()
)


zone_query = (
    zone_metrics.writeStream
    .queryName("ride_requests_by_zone")
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", f"{BASE_CHECKPOINT}/zone")
    .trigger(processingTime="5 seconds")
    .start()
)


# --------------------------------------------------
# 9. Query 3: Event-time window analytics
# --------------------------------------------------

windowed_metrics = (
    valid_events
    .filter(col("event_type") == "RIDE_REQUESTED")
    .withWatermark("event_timestamp", "10 seconds")
    .groupBy(
        window(col("event_timestamp"), "1 minute"),
        col("zone")
    )
    .count()
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("zone"),
        col("count").alias("ride_requests")
    )
)


window_query = (
    windowed_metrics.writeStream
    .queryName("ride_requests_by_time_window")
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", f"{BASE_CHECKPOINT}/window")
    .trigger(processingTime="5 seconds")
    .start()
)


# --------------------------------------------------
# 10. Keep all queries running
# --------------------------------------------------

logger.info("All three analytics queries started")

try:
    spark.streams.awaitAnyTermination()

except KeyboardInterrupt:
    logger.info("Stopping analytics application")

finally:
    for query in spark.streams.active:
        query.stop()

    spark.stop()
