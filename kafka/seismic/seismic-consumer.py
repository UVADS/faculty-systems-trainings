#!/usr/bin/env python3

"""
Seismic Stream Consumer

Reads earthquake events from the "seismic" Kafka topic and upserts each one
into Redis as a hash (quake:<unid>), where the Streamlit map can read them.
"""

import os
import logging
import redis
from quixstreams import Application

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
TTL_SECONDS = 24 * 60 * 60  # match the topic's 24h retention

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("seismic-consumer.log"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

# Consumer group "seismic-redis"; start from the oldest retained message on first run
app = Application(
    broker_address=KAFKA_BROKER,
    consumer_group="seismic-redis",
    auto_offset_reset="earliest",
    consumer_extra_config={"broker.address.family": "v4"},
)
topic = app.topic("seismic", value_deserializer="json")


# Write one event to Redis; "update" actions overwrite the earlier "create" for the same unid
def to_redis(event):
    try:
        p = event["data"]["properties"]
        key = f"quake:{p['unid']}"
        r.hset(key, mapping={
            "time": p["time"],
            "lat": p["lat"],
            "lon": p["lon"],
            "depth": p["depth"],
            "mag": p["mag"],
            "region": p["flynn_region"],
            "action": event["action"],
        })
        r.expire(key, TTL_SECONDS)
        log.info(f"{event['action']:7} | {p['time']} | Mag {p['mag']} | {p['flynn_region']}")
    except KeyError as e:
        log.warning(f"Skipping malformed event, missing {e}")
    except redis.RedisError as e:
        log.error(f"Redis write failed: {e}")


if __name__ == "__main__":
    sdf = app.dataframe(topic)
    sdf = sdf.update(to_redis)
    log.info(f"Consuming '{topic.name}' -> Redis at {REDIS_HOST}:6379. Press Ctrl+C to stop")
    app.run()
