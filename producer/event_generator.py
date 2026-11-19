
import heapq
import random
import time
import uuid

from datetime import datetime, timezone

from producer.config import CITY, ZONES


def generate_id(prefix):
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def get_timestamp():
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def select_zone():
    weights = [zone["weight"] for zone in ZONES]

    return random.choices(
        ZONES,
        weights=weights,
        k=1
    )[0]


def generate_coordinates(zone):
    latitude = zone["latitude"] + random.uniform(-0.004, 0.004)
    longitude = zone["longitude"] + random.uniform(-0.004, 0.004)

    return round(latitude, 6), round(longitude, 6)


def create_ride():

    zone = select_zone()

    pickup_lat, pickup_lon = generate_coordinates(zone)

    destination_zone = random.choice(ZONES)

    drop_lat, drop_lon = generate_coordinates(destination_zone)

    return {
        "ride_id": generate_id("RIDE"),
        "customer_id": generate_id("CUST"),
        "driver_id": generate_id("DRIVER"),

        "city": CITY,
        "zone": zone["name"],

        "latitude": pickup_lat,
        "longitude": pickup_lon,

        "dropoff_latitude": drop_lat,
        "dropoff_longitude": drop_lon
    }


def create_event(ride, event_type):

    driver_id = None

    if event_type in ["DRIVER_ASSIGNED", "RIDE_COMPLETED"]:
        driver_id = ride["driver_id"]

    return {
        "schema_version": 1,
        "event_id": generate_id("EVT"),
        "ride_id": ride["ride_id"],
        "event_type": event_type,

        "customer_id": ride["customer_id"],
        "driver_id": driver_id,

        "city": ride["city"],
        "zone": ride["zone"],

        "latitude": ride["latitude"],
        "longitude": ride["longitude"],

        "dropoff_latitude": ride["dropoff_latitude"],
        "dropoff_longitude": ride["dropoff_longitude"],

        "event_timestamp": get_timestamp()
    }


def generate_events(
    rides_per_minute=60,
    duration_seconds=60,
    min_wait_seconds=2,
    max_wait_seconds=8,
    min_ride_seconds=10,
    max_ride_seconds=30
):
    """
    Generate ride lifecycle events at a configurable rate.

    A priority queue schedules driver assignment and
    ride completion events without blocking each ride.
    """

    if rides_per_minute <= 0:
        raise ValueError("rides_per_minute must be positive")

    if duration_seconds <= 0:
        raise ValueError("duration_seconds must be positive")

    if not (0 <= min_wait_seconds <= max_wait_seconds):
        raise ValueError("Invalid driver waiting time range")

    if not (0 <= min_ride_seconds <= max_ride_seconds):
        raise ValueError("Invalid ride duration range")

    interval = 60.0 / rides_per_minute

    start_time = time.monotonic()
    generation_end = start_time + duration_seconds

    next_request_time = start_time

    # (scheduled_time, sequence_number, ride, event_type)
    event_queue = []

    sequence = 0

    while time.monotonic() < generation_end or event_queue:

        now = time.monotonic()

        # Generate new ride requests
        if now < generation_end and now >= next_request_time:

            ride = create_ride()

            yield create_event(ride, "RIDE_REQUESTED")

            wait_time = random.uniform(
                min_wait_seconds,
                max_wait_seconds
            )

            ride_duration = random.uniform(
                min_ride_seconds,
                max_ride_seconds
            )

            assignment_time = now + wait_time

            completion_time = (
                assignment_time + ride_duration
            )

            sequence += 1

            heapq.heappush(
                event_queue,
                (
                    assignment_time,
                    sequence,
                    ride,
                    "DRIVER_ASSIGNED"
                )
            )

            sequence += 1

            heapq.heappush(
                event_queue,
                (
                    completion_time,
                    sequence,
                    ride,
                    "RIDE_COMPLETED"
                )
            )

            next_request_time += interval

        # Publish scheduled lifecycle events when due
        now = time.monotonic()

        while event_queue and event_queue[0][0] <= now:

            _, _, ride, event_type = heapq.heappop(event_queue)

            yield create_event(ride, event_type)

        # Avoid unnecessary CPU usage
        if time.monotonic() < generation_end or event_queue:

            next_times = []

            if time.monotonic() < generation_end:
                next_times.append(next_request_time)

            if event_queue:
                next_times.append(event_queue[0][0])

            sleep_time = max(
                0,
                min(next_times) - time.monotonic()
            )

            time.sleep(min(sleep_time, 0.05))
