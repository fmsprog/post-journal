# Post Journal Application

> **Warning:** This is a prototype and example system, not production ready. It is intended for demonstration and educational purposes only and requires further development and review before any real-world or production use.

## Project Purpose

This project is a distributed event sourcing and journaling system built on top of Apache Kafka. It is designed to provide reliable, fault-tolerant, and consistent processing of transactional changes across multiple replicas. The system supports high-throughput and low-latency data processing, active-standby failover, snapshotting, and transactional message handling, making it suitable for high-availability event-driven applications.

## How to Run the Sample Application

**First, build the project:**

```sh
./gradlew build
```

Below is a step-by-step guide to running the sample application, including launching Kafka, multiple replicas, consumers, and a producer.

### 1. Start Kafka and Zookeeper with Docker Compose

Navigate to the `docker/kafka` directory and run:

```sh
cd docker/kafka
# Start Kafka and Zookeeper
# This will expose:
# - Zookeeper on port 2181
# - Kafka broker on port 9092
# - Kafka UI (if present) on port 8080 (see: [Kafka UI](http://localhost:8080))
# - Schema Registry (if present) on port 8081
#
# You can access Kafka at localhost:9092
#
docker-compose up -d
```

After this step, Kafka and Zookeeper will be available for the application to use.

You can open [Kafka UI](http://localhost:8080) in your browser to inspect topics and messages.

### 2. Start Sample Application Replicas

You can run multiple replicas (A, B, C) to simulate a distributed environment. Each replica can be started from its corresponding Kotlin file:

In separate terminals, run:
```sh
# Replica A
./gradlew runKotlinMain -PmainClass="io.github.firsmic.postjournal.sample.application.SampleAppReplicaAKt"
```

```sh
# Replica B
./gradlew runKotlinMain -PmainClass="io.github.firsmic.postjournal.sample.application.SampleAppReplicaBKt"
```

```sh
# Replica C
./gradlew runKotlinMain -PmainClass="io.github.firsmic.postjournal.sample.application.SampleAppReplicaCKt"
```

Each replica will join the cluster and participate in leader election and failover.

### 3. Start Consumers

You can start one or more consumers to observe the processed messages:

```sh
# Consumer 1
./gradlew runKotlinMain -PmainClass="io.github.firsmic.postjournal.sample.application.SampleAppConsumer1Kt"
```

```sh
# Consumer 2
./gradlew runKotlinMain -PmainClass="io.github.firsmic.postjournal.sample.application.SampleAppConsumer2Kt"
```

Consumers will connect to Kafka and print out the messages as they are processed by the application.

### 4. Start the Producer

To send messages into the system, start the producer:

```sh
./gradlew runKotlinMain -PmainClass="io.github.firsmic.postjournal.sample.application.SampleAppProducerKt"
```

The producer will publish messages to Kafka, which will then be processed by the active replica.

## High Availability and Failover

If the currently active replica is stopped, another standby replica will automatically become active and continue processing messages without data loss.

## Snapshotting via REST API

You can trigger a snapshot on a standby replica using the REST API (for example, on `SampleAppReplicaA`).

**Note:** Snapshots can only be taken on a standby replica (not the active one).

Example REST call:

```sh
curl -X POST http://localhost:8085/api/v1/snapshot
```

This will create a snapshot of the current state, which can be used for recovery or backup purposes.

---

For more details, see the source code and comments in the respective modules.

---

Copyright 2025 Mikhail Firsov
