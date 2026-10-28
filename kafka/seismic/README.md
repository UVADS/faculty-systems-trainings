# Real-time Seismic Activity

Using the public earthquake feed from the [EMSC Seismic Portal](https://www.seismicportal.eu/) (European-Mediterranean
Seismological Centre), which publishes seismic events worldwide over a websocket as they are detected and revised.
Each message describes one event (time, location, depth, magnitude, region) and is either a `create` for a new
event or an `update` to an earlier one. This application reads that stream and plots earthquakes on a map in near real-time.

The solution:

![Seismic Architecture](https://s3.amazonaws.com/uvasds-systems/images/shipping-arch.png)

1. A websocket connection listens to the EMSC event stream.
2. The producer takes each event payload and sends it as a message into the `seismic` Kafka topic, keyed by event ID.
3. Kafka retains events for a specific TTL (1 day) ready for consumers to read.
4. A consumer reads the log continuously, parsing the JSON and writing one Redis entry per event. Updates overwrite the original entry for the same event.
5. A Redis database persists each entry for 24 hours.
6. A Streamlit application reads the database and plots each earthquake, with dot size and color scaled by magnitude.

## Files

- `seismic-producer.py` - Connects to the EMSC websocket, creates the `seismic` topic (3 partitions, 3 replicas, 24h retention) if needed, and produces each event to Kafka.
- `seismic-consumer.py` - Consumes the `seismic` topic and upserts each event into Redis as a `quake:<id>` hash with a 24h expiry.
- `seismic-map.py` - Streamlit app that loads all `quake:*` entries from Redis and renders them on an auto-refreshing map. Run with `streamlit run seismic-map.py`.
- `together.py` - Launcher that starts the producer, consumer, and map as child processes from one terminal, and stops all three on Ctrl+C or if any one exits.
