# Faculty Systems Training

October 28, 2026

<img src="https://s3.amazonaws.com/uvasds-systems/images/kafka-logo-tall.png" alt="Apache Kafka" style="float:right;">

> **[Apache Kafka](https://kafka.apache.org/44/)** is a distributed event streaming platform. Producers write streams of records to it, Kafka stores them durably for a set period, and any number of consumers read them independently, in real time or later.

## Topics

1. **Events and the log**

    An **event** (or record/message) is a small fact about something that happened: "vessel 367001234 was at 37.8N, 122.4W at 14:02:11." Each event has a **key**, a **value** (usually JSON, Avro, or Protobuf), and a **timestamp**.

    ```
    key: 20261028_0000223
    timestamp: 2026-10-28 10:35:47.545
    value: {
        "action": "create",
        "data": {
            "id": "20261028_0000223",
            "properties": {
                "time": "2026-10-28T14:28:50.0Z",
                "flynn_region": "ANTOFAGASTA, CHILE",
                "lat": -21.58,
                "lon": -68.72,
                "depth": 133.1,
                "mag": 2.6
            }
        }
    }
    ```

    Kafka's core idea is the **append-only log**. New events are added to the end; existing events are never modified. Reading does not delete anything. This is the main difference from a traditional message queue:

    | | **Message queue** (SQS, RabbitMQ) | **Kafka** |
    |---|---|---|
    | After a message is read | Removed from the queue | Stays in the log until retention expires |
    | Multiple independent readers | Requires fan-out (e.g. SNS → several queues) | Built in; each reader tracks its own position |
    | Replay past data | Not possible | Rewind and re-read |
    | Ordering | Best-effort, or limited FIFO | Guaranteed within a partition |
    | Throughput | Moderate | Very high (millions of events/sec per cluster) |

    Q: Which of your data sources produce a continuous stream (sensors, instruments, logs, APIs you poll) rather than a file that arrives once?

2. **Topics and retention**

    A **topic** is a named stream of related events, e.g. `ais-positions`. It is roughly analogous to a table in a database, except it is a log rather than a mutable set of rows. Producers write to a topic by name; consumers subscribe by name. Neither needs to know about the other.

    - **Retention** controls how long events are kept, by time (`retention.ms`) or size (`retention.bytes`). The shipping example keeps 24 hours. Retention can also be set to forever.
    - **Compaction** is an alternative to time-based retention: Kafka keeps only the latest event per key. Useful for "current state" topics, e.g. the last known position of each vessel.

3. **Partitions and keys**

    Each topic is split into one or more **partitions**. A partition is the actual log on disk, and it is the unit of both parallelism and ordering.

    - Events with the **same key always go to the same partition**, so they stay in order relative to each other. Keying AIS messages by vessel ID (MMSI) guarantees each ship's positions are read in the order they were sent.
    - Events with no key are spread across partitions, with no ordering guarantee between them.
    - The number of partitions caps how many consumers in a group can read in parallel (see #5). It can be increased later but not decreased, and increasing it changes which partition a key maps to.

    Q: In your data, what is the natural key whose events must stay in order (a sensor, a subject, a device, a user)?

4. **Producers**

    A **producer** is any client that writes events to a topic. It chooses the key, serializes the value, and sends it to Kafka. Producers batch and compress events for throughput.

    - **`acks`** sets durability: `acks=0` (fire and forget), `acks=1` (leader has it), `acks=all` (all in-sync replicas have it; the safe default).
    - Idempotent producers (default in current clients) prevent duplicates when a send is retried.

    ```python
    from quixstreams import Application

    app = Application(broker_address="kafka:9092")
    topic = app.topic("ais-positions", value_serializer="json")

    with app.get_producer() as producer:  # flushes on exit
        out = topic.serialize(key=str(msg["mmsi"]), value=msg)
        producer.produce(topic=topic.name, key=out.key, value=out.value)
    ```

5. **Consumers, consumer groups, and offsets**

    A **consumer** reads events from a topic by polling. Every event in a partition has a sequential **offset** (0, 1, 2, ...). The consumer's position is just "the next offset to read," and it periodically **commits** that offset back to Kafka. If the consumer crashes and restarts, it resumes from the last committed offset.

    - A **consumer group** (`group.id`; `consumer_group` in Quix Streams) is a set of consumers sharing the work. Kafka assigns each partition to exactly one consumer in the group. Add consumers to scale out, up to the number of partitions. If one dies, its partitions are reassigned (a **rebalance**).
    - **Different groups are independent.** The Redis writer in the shipping example is one group; an archiver writing to S3 could be a second group reading the same topic, each with its own offsets.
    - `auto.offset.reset` (`auto_offset_reset` in Quix Streams) decides where a new group starts: `earliest` (replay everything retained) or `latest` (only new events).
    - Delivery semantics depend on when you commit: commit after processing gives **at-least-once** (possible duplicates on restart, so make writes idempotent); commit before processing gives at-most-once (possible loss).

    ```python
    from quixstreams import Application

    app = Application(
        broker_address="kafka:9092",
        consumer_group="redis-writer",
        auto_offset_reset="earliest",
    )
    topic = app.topic("ais-positions", value_deserializer="json")

    sdf = app.dataframe(topic)
    sdf = sdf.update(handle)  # called with each message value
    app.run()                 # polls, processes, and commits offsets after processing
    ```

    Q: If your consumer were down for six hours, what should happen when it comes back: catch up on everything, or skip to now?

6. **Brokers, replication, and the cluster**

    A Kafka **broker** is a server that stores partitions and serves reads and writes. A **cluster** is several brokers. Clients connect to any broker listed in `bootstrap.servers` and discover the rest.

    - Each partition has a **replication factor** (commonly 3). One replica is the **leader** and handles all reads and writes; the others are **followers** that copy it. If the leader's broker fails, a follower is promoted.
    - The **in-sync replicas (ISR)** are the followers that are caught up. With `acks=all`, a write succeeds only once every ISR has it.
    - Cluster metadata (which broker leads which partition) is managed by **KRaft**, Kafka's built-in consensus layer. Older deployments used ZooKeeper, which was removed in Kafka 4.0.
    - On Kubernetes, Kafka is usually run through an operator such as **[Strimzi](https://strimzi.io/)**, which manages brokers, topics, and users as Kubernetes resources.


## Example Projects

### Shipping Data

For a working example see the **[Shipping Tracker](https://shipping.pods.uvarc.io/)** based on free AIS data. In shipping, AIS data 
is the real-time position, speed, and identity information that vessels broadcast 
automatically, which the industry uses to track fleets, manage ports, predict 
arrivals, analyze trade flows, and monitor compliance and emissions.

Architecture:

![Kafka PubSub Architecture](https://s3.amazonaws.com/uvasds-systems/images/
shipping-arch.png)

- A Kafka **publisher** that connects to the
**[AISStream Websocket](https://aisstream.io/)** receives position messages
every few seconds tracking ships within a bounded geographic "box" and publishes
them to a Kafka topic. This service runs in a container.
- Apache Kafka runs in the UVA Research Computing Kubernetes cluster, storing
event messages for up to 24 hours.
- A **consumer** process polls the Kafka topic for new
messages, extracting relevant data and writes it to a Redis database.
- A Streamlit application displays tracking data
from Redis. It filters out locations older than 24 hours and draws a tracking vector
between ship waypoints (if in motion).

### Seismic Data

For example code of a similar project, see the accompanying Python modules and Docker
compose file in [`kafka/`](kafka/) and [`kafka/seismic/`](kafka/seismic/)

- Using Docker, bring up the Kafka cluster: `docker compose up -d`
- Open the Redpanda Kafka UI http://127.0.0.1:8080/
- Run the `seismic-producer.py` module found in `kafka/seismic` and note a new topic with messages as they arrive.
- Run the `seismic-consumer.py` module and messages in Kafka will be read into the Redis database.
- Run the `seismic-map.py` module for a Streamlit visualization available at http://localhost:8501/

To run all modules together, run `together.py`.

## For Teaching / Reference

- [`kafka/docker-compose.yml`](kafka/docker-compose.yml) - a local 3-broker cluster (Redpanda, Kafka API-compatible), the Redpanda Console GUI at http://localhost:8080, and Redis. Start with `docker compose up -d` from `kafka/`.
- [`kafka/seismic/`](kafka/seismic/) - a live earthquake pipeline using the cluster above:
    - [`seismic-producer.py`](kafka/seismic/seismic-producer.py) - reads the [EMSC Seismic Portal](https://www.seismicportal.eu/) websocket and produces each event to the `seismic` topic.
    - [`seismic-consumer.py`](kafka/seismic/seismic-consumer.py) - consumes the topic and writes each quake to Redis with a 24h TTL.
    - [`seismic-map.py`](kafka/seismic/seismic-map.py) - Streamlit map of the quakes in Redis, colored by magnitude.
    - [`together.py`](kafka/seismic/together.py) - runs all three from one terminal (`python together.py`).
- Docs: [Apache Kafka](https://kafka.apache.org/documentation/) · [Kafka on GitHub](https://github.com/apache/kafka) · [Redpanda](https://docs.redpanda.com/) · [Redpanda Console](https://docs.redpanda.com/current/manage/console/)
- Python clients: [Quix Streams](https://quix.io/docs/quix-streams/introduction.html) (used in the snippets above and the seismic demo) · [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python) (lower-level client that Quix Streams builds on)
- Learning: [Confluent Developer courses](https://developer.confluent.io/courses/apache-kafka/events/) · [Kafka 101 (YouTube)](https://www.youtube.com/playlist?list=PLa7VYi0yPIH0KbnJQcMv5N9iW8HkZHztH) · [Apache Kafka Fundamentals (YouTube)](https://www.youtube.com/playlist?list=PLa7VYi0yPIH2PelhRHoFR5iQgflg-y6JA)
