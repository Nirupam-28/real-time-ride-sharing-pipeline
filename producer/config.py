
import os

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "ride_events"
)

# Application configuration
PRODUCER_CLIENT_ID = "ride-event-simulator"

# Hyderabad geographical zones
CITY = "Hyderabad"

ZONES = [
    {
        "name": "Madhapur",
        "latitude": 17.4483,
        "longitude": 78.3915,
        "weight": 30
    },
    {
        "name": "Gachibowli",
        "latitude": 17.4401,
        "longitude": 78.3489,
        "weight": 25
    },
    {
        "name": "HITEC City",
        "latitude": 17.4435,
        "longitude": 78.3772,
        "weight": 20
    },
    {
        "name": "Kukatpally",
        "latitude": 17.4849,
        "longitude": 78.4138,
        "weight": 10
    },
    {
        "name": "Jubilee Hills",
        "latitude": 17.4310,
        "longitude": 78.4070,
        "weight": 10
    },
    {
        "name": "Secunderabad",
        "latitude": 17.4399,
        "longitude": 78.4983,
        "weight": 5
    }
]
