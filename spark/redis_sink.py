import redis

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    count,
    when,
    sum as spark_sum
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

from common.config import Config
from common.retry import retry
from common.logging_config import setup_logging

logger = setup_logging("redis_sink")

# 1.Configuration
KAFKA_BOOTSTRAP_SERVERS = Config.KAFKA_BOOTSTRAP_SERVERS
SOURCE_TOPIC = Config.KAFKA_TOPIC

CHECKPOINT_LOCATION = "spark/checkpoints/redis_sink"

REDIS_PREFIX = Config.REDIS_NAMESPACE

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

logger = setup_logging("redis_sink")

# --------------------------------------------------
# 2. Redis connection
# --------------------------------------------------

redis_client = redis.Redis(
    host=Config.REDIS_HOST,
    port=Config.REDIS_PORT,
    db=Config.REDIS_DB,
    password=Config.REDIS_PASSWORD,
    decode_responses=True,
    socket_connect_timeout=5,
    socket_timeout=5
)

try:
    redis_client.ping()
    logger.info("Connected to Redis successfully")
except redis.exceptions.RedisError:
    logger.exception("Redis connection failed")
    raise


# --------------------------------------------------
# 3. Define JSON schema
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
# 4. Create Spark session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("RideSharingRedisSink")
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
# 5. Read events from Kafka
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
# 6. Parse JSON
# --------------------------------------------------

parsed_stream = raw_stream.select(
    from_json(
        col("value").cast("string"),
        ride_schema
    ).alias("data")
)

events = parsed_stream.select(
    col("data.event_id").alias("event_id"),
    col("data.ride_id").alias("ride_id"),
    col("data.event_type").alias("event_type"),
    col("data.city").alias("city"),
    col("data.zone").alias("zone"),
    to_timestamp(
        col("data.event_timestamp")
    ).alias("event_timestamp")
)


# --------------------------------------------------
# 7. Filter valid events
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
# 8. Atomic Redis update using Lua
# --------------------------------------------------

REDIS_LUA_SCRIPT = """
if redis.call('EXISTS', KEYS[3]) == 1 then
    return 0
end

local requests = tonumber(ARGV[1])
local assignments = tonumber(ARGV[2])
local completed = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local zone_count = tonumber(ARGV[5])

redis.call('HINCRBY', KEYS[1], 'total_requests', requests)
redis.call('HINCRBY', KEYS[1], 'total_assignments', assignments)
redis.call('HINCRBY', KEYS[1], 'total_completed', completed)

local total_requests =
    tonumber(redis.call('HGET', KEYS[1], 'total_requests'))

local total_completed =
    tonumber(redis.call('HGET', KEYS[1], 'total_completed'))

redis.call(
    'HSET',
    KEYS[1],
    'active_rides',
    total_requests - total_completed
)

local arg_index = 6

for i = 1, zone_count do
    local zone = ARGV[arg_index]
    local zone_requests = tonumber(ARGV[arg_index + 1])

    redis.call(
        'HINCRBY',
        KEYS[2],
        zone,
        zone_requests
    )

    arg_index = arg_index + 2
end

redis.call('SET', KEYS[3], '1', 'EX', ttl)

return 1
"""

# Backward-compatible name for existing imports.
REDIS_UPDATE_SCRIPT = REDIS_LUA_SCRIPT


@retry(
    attempts=3,
    base_delay=1,
    exceptions=(
        redis.exceptions.ConnectionError,
        redis.exceptions.TimeoutError
    )
)
def execute_redis_batch(keys, args):
    return redis_client.eval(
        REDIS_LUA_SCRIPT,
        len(keys),
        *keys,
        *args
    )

# --------------------------------------------------
# 9. Process every Spark micro-batch
# --------------------------------------------------

def process_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        logger.info("Batch %s is empty", batch_id)
        return

    # Calculate overall event counts.
    totals = batch_df.agg(
        spark_sum(
            when(col("event_type") == "RIDE_REQUESTED", 1)
            .otherwise(0)
        ).alias("requests"),

        spark_sum(
            when(col("event_type") == "DRIVER_ASSIGNED", 1)
            .otherwise(0)
        ).alias("assignments"),

        spark_sum(
            when(col("event_type") == "RIDE_COMPLETED", 1)
            .otherwise(0)
        ).alias("completed")
    ).first()

    requests = int(totals["requests"] or 0)
    assignments = int(totals["assignments"] or 0)
    completed = int(totals["completed"] or 0)

    # Calculate ride requests by zone.
    zone_rows = (
        batch_df
        .filter(col("event_type") == "RIDE_REQUESTED")
        .groupBy("zone")
        .agg(count("*").alias("requests"))
        .collect()
    )

    zone_values = []

    for row in zone_rows:
        zone_values.extend([
            row["zone"],
            int(row["requests"])
        ])

    # Use a unique Redis key for this Spark batch.
    metrics_key = f"{REDIS_PREFIX}:metrics"
    zones_key = f"{REDIS_PREFIX}:zones"
    batch_key = f"{REDIS_PREFIX}:batch:{batch_id}"

    # Apply the update atomically.
    keys = [metrics_key, zones_key, batch_key]
    args = [
        requests,
        assignments,
        completed,
        7 * 24 * 60 * 60,
        len(zone_rows),
        *zone_values
    ]
    result = execute_redis_batch(keys, args)

    if result == 1:
        logger.info(
            "Batch %s updated Redis | requests=%s assignments=%s completed=%s",
            batch_id,
            requests,
            assignments,
            completed
        )
    else:
        logger.info(
            "Batch %s already applied. Skipping duplicate update.",
            batch_id
        )


# --------------------------------------------------
# 10. Start streaming query
# --------------------------------------------------

query = (
    valid_events.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(processingTime="5 seconds")
    .start()
)

logger.info("Kafka to Redis streaming application started")

try:
    query.awaitTermination()

except KeyboardInterrupt:
    logger.info("Stopping streaming application")

finally:
    query.stop()
    redis_client.close()
    spark.stop()
