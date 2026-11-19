import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ride_events")
    KAFKA_DLQ_TOPIC = os.getenv(
        "KAFKA_DLQ_TOPIC", "ride_events_dlq"
    )

    # Redis
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB = int(os.getenv("REDIS_DB", "0"))
    REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None

    REDIS_NAMESPACE = os.getenv(
        "REDIS_NAMESPACE", "ride:analytics:v1"
    )

    # PostgreSQL
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_DB = os.getenv("POSTGRES_DB", "rideshare")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "rideuser")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "ridepass")

    # Application
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    RETRY_ATTEMPTS = int(os.getenv("RETRY_ATTEMPTS", "3"))
    RETRY_BASE_DELAY = float(os.getenv("RETRY_BASE_DELAY", "1"))
