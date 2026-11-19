
import os
import logging

import pandas as pd
import plotly.express as px
import redis
import psycopg2
import streamlit as st

from streamlit_autorefresh import st_autorefresh


# --------------------------------------------------
# 1. Configuration
# --------------------------------------------------

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "rideshare")
POSTGRES_USER = os.getenv("POSTGRES_USER", "rideuser")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "ridepass")

REDIS_METRICS_KEY = "ride:analytics:v1:metrics"
REDIS_ZONES_KEY = "ride:analytics:v1:zones"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# from common.logging_config import setup_logging

# logger = setup_logging("Streamlit Dashboard")


# --------------------------------------------------
# 2. Streamlit page configuration
# --------------------------------------------------

st.set_page_config(
    page_title="Ride Sharing Analytics",
    page_icon="🚕",
    layout="wide"
)

st.title("🚕 Real-Time Ride Sharing Analytics")
st.caption("Hyderabad | Live operational metrics and historical analytics")


# --------------------------------------------------
# 3. Database connections
# --------------------------------------------------

@st.cache_resource
def get_redis_connection():
    client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        socket_connect_timeout=3
    )
    client.ping()
    return client


@st.cache_resource
def get_postgres_connection():
    connection = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        connect_timeout=5
    )
    return connection


# --------------------------------------------------
# 4. Read real-time metrics from Redis
# --------------------------------------------------

def get_live_metrics():

    client = get_redis_connection()

    metrics = client.hgetall(REDIS_METRICS_KEY)

    zones = client.hgetall(REDIS_ZONES_KEY)

    def safe_int(value):
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    return {
        "total_requests": safe_int(metrics.get("total_requests")),
        "total_assignments": safe_int(metrics.get("total_assignments")),
        "total_completed": safe_int(metrics.get("total_completed")),
        "active_rides": safe_int(metrics.get("active_rides")),
        "zones": {
            zone: safe_int(count)
            for zone, count in zones.items()
        }
    }


# --------------------------------------------------
# 5. Read historical analytics from PostgreSQL
# --------------------------------------------------

def read_postgres_query(sql, params=None):

    connection = get_postgres_connection()

    with connection.cursor() as cursor:
        cursor.execute(sql, params or ())
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

    return pd.DataFrame(rows, columns=columns)


def get_hourly_ride_trends():

    sql = """
        SELECT
            date_trunc('hour', event_timestamp) AS ride_hour,
            COUNT(*) FILTER (
                WHERE event_type = 'RIDE_REQUESTED'
            ) AS requests,
            COUNT(*) FILTER (
                WHERE event_type = 'RIDE_COMPLETED'
            ) AS completed
        FROM ride_events
        WHERE event_timestamp >= NOW() - INTERVAL '24 hours'
        GROUP BY ride_hour
        ORDER BY ride_hour
    """

    return read_postgres_query(sql)


def get_recent_events():

    sql = """
        SELECT
            ride_id,
            event_type,
            customer_id,
            driver_id,
            city,
            zone,
            event_timestamp
        FROM ride_events
        ORDER BY event_timestamp DESC
        LIMIT 20
    """

    return read_postgres_query(sql)


def get_historical_zone_metrics():

    sql = """
        SELECT
            zone,
            COUNT(*) AS total_requests
        FROM ride_events
        WHERE event_type = 'RIDE_REQUESTED'
        GROUP BY zone
        ORDER BY total_requests DESC
    """

    return read_postgres_query(sql)


# --------------------------------------------------
# 6. Automatic refresh
# --------------------------------------------------

st_autorefresh(
    interval=5000,
    key="dashboard_refresh"
)


# --------------------------------------------------
# 7. Redis live metrics section
# --------------------------------------------------

st.subheader("Live Ride Metrics")

try:

    live = get_live_metrics()

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Total Requests",
        f"{live['total_requests']:,}"
    )

    col2.metric(
        "Driver Assignments",
        f"{live['total_assignments']:,}"
    )

    col3.metric(
        "Completed Rides",
        f"{live['total_completed']:,}"
    )

    col4.metric(
        "Active Rides",
        f"{live['active_rides']:,}"
    )

    st.caption("Live metrics sourced from Redis")

except Exception as error:

    st.error(f"Redis connection or data error: {error}")
    live = {"zones": {}}


# --------------------------------------------------
# 8. Zone demand chart from Redis
# --------------------------------------------------

st.divider()

st.subheader("Live Ride Demand by Zone")

if live["zones"]:

    zone_df = pd.DataFrame(
        list(live["zones"].items()),
        columns=["zone", "requests"]
    )

    zone_df = zone_df.sort_values(
        "requests",
        ascending=False
    )

    fig = px.bar(
        zone_df,
        x="zone",
        y="requests",
        title="Ride Requests by Hyderabad Zone",
        labels={
            "zone": "Zone",
            "requests": "Total Requests"
        },
        text="requests"
    )

    fig.update_layout(
        xaxis_title="Zone",
        yaxis_title="Requests",
        template="plotly_white"
    )

    st.plotly_chart(fig, use_container_width=True)

else:

    st.info(
        "No zone metrics found in Redis yet. "
        "Start the Redis Spark streaming application."
    )


# --------------------------------------------------
# 9. Historical ride trends from PostgreSQL
# --------------------------------------------------

st.divider()

st.subheader("Historical Ride Trends — Last 24 Hours")

try:

    trend_df = get_hourly_ride_trends()

    if not trend_df.empty:

        fig = px.line(
            trend_df,
            x="ride_hour",
            y=["requests", "completed"],
            markers=True,
            title="Hourly Ride Requests vs Completions",
            labels={
                "ride_hour": "Hour",
                "value": "Number of Rides",
                "variable": "Event Type"
            }
        )

        fig.update_layout(template="plotly_white")

        st.plotly_chart(
            fig,
            use_container_width=True
        )

    else:

        st.info("No historical ride data available yet.")

except Exception as error:

    st.error(f"PostgreSQL trend query failed: {error}")


# --------------------------------------------------
# 10. Historical zone analytics
# --------------------------------------------------

st.divider()

st.subheader("Historical Zone Performance")

try:

    historical_zones = get_historical_zone_metrics()

    if not historical_zones.empty:

        st.dataframe(
            historical_zones,
            use_container_width=True,
            hide_index=True
        )

    else:

        st.info("No historical zone data available.")

except Exception as error:

    st.error(f"Zone analytics query failed: {error}")


# --------------------------------------------------
# 11. Recent ride events
# --------------------------------------------------

st.divider()

st.subheader("Recent Ride Events")

try:

    recent_df = get_recent_events()

    if not recent_df.empty:

        st.dataframe(
            recent_df,
            use_container_width=True,
            hide_index=True
        )

    else:

        st.info("No ride events found in PostgreSQL.")

except Exception as error:

    st.error(f"Recent events query failed: {error}")


# --------------------------------------------------
# 12. Footer
# --------------------------------------------------

st.divider()

st.caption(
    "Ride Sharing Data Engineering Pipeline | "
    "Kafka + Spark Structured Streaming + Redis + PostgreSQL + Streamlit"
)
