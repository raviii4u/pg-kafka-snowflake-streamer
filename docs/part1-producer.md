# How We Built a Production-Grade PostgreSQL CDC Producer with Embedded Debezium and Kafka

## The Problem

Your PostgreSQL database is the source of truth. Your data team lives in Snowflake. Every approach to keeping them in sync has a cost:

- **Nightly batch jobs** — 8-hour stale data windows. Deleted rows never propagate. Your analysts are always looking at yesterday.
- **Polling queries** — hammers your production database. Misses deletes. Needs a `updated_at` column on every table.
- **AWS DMS** — managed, but opaque. Schema drift breaks it silently. Costs scale with row volume.

Change Data Capture (CDC) via PostgreSQL's Write-Ahead Log (WAL) solves all of these. It captures every INSERT, UPDATE, and DELETE the moment it's committed — with no impact on the source database beyond a logical replication slot.

This is Part 1 of a two-part series on how we built **pg-kafka-snowflake-streamer**, a production CDC pipeline from PostgreSQL to Snowflake. This part covers the **producer** side: how we read the WAL and get every change event reliably into Kafka.

---

## Architecture Overview

```
PostgreSQL WAL
     │
     ▼
DebeziumCdcService        ← embedded Debezium engine (one per pod)
  │   └── CustomDateTimeConverter  ← normalises date/time fields
  │
  ▼
KafkaBatchService          ← Kafka producer wrapper
  │   └── auto-creates topics on first send
  │
  ▼
Kafka (AWS MSK)            ← one topic per table
     │                        stream-{schema}-{table}
     ▼
  [Consumer — Part 2]
```

One Spring Boot pod per PostgreSQL database. No shared state between agents. Failure of one agent does not affect any other.

---

## Why Embedded Debezium Instead of Kafka Connect?

The typical CDC setup runs Debezium as a **Kafka Connect plugin** — a separate cluster of worker nodes you manage, scale, and monitor alongside your Kafka cluster. For many teams that's the right choice.

We chose **embedded Debezium** instead. Here's why:

| | Kafka Connect (external) | Embedded Debezium |
|---|---|---|
| Infra footprint | Kafka + ZooKeeper + Connect cluster | One JVM pod |
| Isolation | Shared connector workers | One process per database |
| Failure blast radius | One Connect worker failure can affect multiple connectors | Failure scoped to one pod |
| Operational complexity | REST API management, connector config, distributed mode | Spring Boot properties |
| Replication slot ownership | Connect worker owns the slot | The pod owns the slot |

The isolation argument is the decisive one for us. When you have 40+ databases to replicate, a connector worker crash that takes down 10 databases simultaneously is a bad day. One pod per database means one database's problem stays one database's problem.

---

## The Producer: `DebeziumCdcService`

This is the heart of the producer. It does four things:

1. Initialises the Snowflake connection pool (so the consumer can use it when they share a pod)
2. Configures the Debezium engine
3. Starts it in a single-thread executor
4. Ensures WAL offsets only advance after Kafka confirms receipt

```java
@Service
@ConditionalOnProperty(name = "cdc.producer.enabled", havingValue = "true", matchIfMissing = true)
public class DebeziumCdcService {

    @PostConstruct
    public void start() {
        // Build the Debezium engine
        DebeziumEngine<RecordChangeEvent<SourceRecord>> engine =
            DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(buildConfiguration())
                .notifying(this::handleBatch)
                .build();

        executor.execute(engine);
    }
}
```

The `@ConditionalOnProperty` annotation lets you disable the producer entirely and run a consumer-only pod — more on that at the end.

---

## The Commit Strategy: No Offset Advancement Without Kafka Confirmation

This is the most important design decision in the producer. Debezium maintains a **WAL offset file** on a PersistentVolumeClaim. When the engine processes a batch, it calls your handler. If your handler succeeds, Debezium marks the batch finished and advances the WAL offset. If it fails, the next restart re-reads from the last committed offset.

The problem: if you acknowledge success before Kafka confirms receipt, you can lose events. Debezium advances past them. They're gone.

Our solution:

```java
private void handleBatch(
        List<RecordChangeEvent<SourceRecord>> records,
        DebeziumEngine.RecordCommitter<RecordChangeEvent<SourceRecord>> committer) {

    List<Future<?>> futures = new ArrayList<>();

    for (RecordChangeEvent<SourceRecord> event : records) {
        SourceRecord record = event.record();

        // Skip heartbeats and null-value tombstone events
        if (isHeartbeat(record) || record.value() == null) continue;

        String tableName = ChangeEventParser.getTableName(record);
        String json = serializeToJson(record);
        String key = serializeKey(record);

        // Send to Kafka — returns a Future
        futures.add(kafkaBatchService.send(tableName, json, key));
    }

    // Wait for ALL Kafka sends to confirm before marking batch finished
    for (Future<?> future : futures) {
        future.get(); // throws on failure
    }

    // Only here do we tell Debezium it's safe to advance the WAL offset
    committer.markBatchFinished();
}
```

If any Kafka send fails, `future.get()` throws. The exception propagates. `markBatchFinished()` is never called. The Debezium engine stops. On the next pod restart, it re-reads from the last confirmed WAL position. **No events are lost.**

---

## Event Structure: What Debezium Produces

Every change event is serialised to JSON using Debezium's `JsonConverter`. A typical event looks like this:

```json
{
  "source": {
    "schema": "public",
    "table": "orders",
    "db": "myapp"
  },
  "op": "u",
  "before": {
    "id": 42,
    "status": "pending",
    "updated_at": "2024-01-15T10:30:00.000Z"
  },
  "after": {
    "id": 42,
    "status": "shipped",
    "updated_at": "2024-01-15T11:45:00.000Z"
  }
}
```

`op` is one of:
- `c` — INSERT (create)
- `u` — UPDATE
- `d` — DELETE
- `r` — READ (initial snapshot, not used here — we use a separate snapshot path)

---

## Solving the Date/Time Problem: `CustomDateTimeConverter`

PostgreSQL and Snowflake represent dates and timestamps very differently. Debezium by default emits them as raw integers (epoch milliseconds, epoch days, nanoseconds), which Snowflake can't natively consume. Worse, PostgreSQL's epoch zero (`1970-01-01 00:00:00`) maps to `1969-12-31` in certain US timezones — a silent data corruption bug.

We solve this with a custom Debezium converter that runs inline during serialisation:

```java
public class CustomDateTimeConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    @Override
    public void converterFor(RelationalColumn column,
                             ConverterRegistration<SchemaBuilder> registration) {
        String type = column.typeName().toLowerCase();

        if (type.equals("date")) {
            registration.register(SchemaBuilder.string(), value -> {
                if (value == null) return null;
                int epochDays = (int) value;
                if (epochDays == 0) return null; // Avoid 1969-12-31 CST issue
                return LocalDate.ofEpochDay(epochDays).format(DATE_FORMATTER); // "yyyy-MM-dd"
            });
        }

        if (type.startsWith("timestamp")) {
            registration.register(SchemaBuilder.string(), value -> {
                if (value == null) return null;
                long epochMicros = (long) value;
                if (epochMicros == 0) return null; // Avoid epoch-zero timezone issues
                return Instant.ofEpochSecond(0, epochMicros * 1000)
                              .atZone(ZoneOffset.UTC)
                              .format(TIMESTAMP_FORMATTER); // "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
            });
        }
    }
}
```

The epoch-zero null guard is subtle but important. In production we encountered rows where `created_at` was `1970-01-01 00:00:00 UTC`. After timezone conversion in Snowflake's US-based warehouse region, these became `1969-12-31 18:00:00` — the previous day. Returning `null` for epoch zero avoids this entirely.

The converter is registered in Debezium properties:

```properties
converters=datetime
datetime.type=io.cdcstreamers.converter.CustomDateTimeConverter
```

---

## Kafka Topic Management: `KafkaBatchService`

The producer wrapper handles two jobs: topic creation and event delivery.

**Topic naming** follows a deterministic pattern:

```
{prefix}{schema}-{table}
```

For a table `public.orders` with prefix `stream-`, the topic is `stream-public-orders`. The schema name is included so tables from different PostgreSQL schemas don't collide.

**Auto-creation** happens on first send to a new topic:

```java
public Future<?> send(String tableName, String json, String key) {
    String topic = buildTopicName(tableName);

    // Create topic only once per process lifetime
    if (!createdTopics.contains(topic)) {
        adminClient.createTopics(List.of(
            new NewTopic(topic, 1, (short) 3) // 1 partition, RF=3
        ));
        createdTopics.add(topic);
    }

    return kafkaTemplate.send(topic, key, json);
}
```

One partition per topic is deliberate. CDC events for a single table must be consumed in order to avoid applying a DELETE before the INSERT that created the row. A single partition guarantees ordering.

**Producer configuration** for reliability:

```yaml
spring:
  kafka:
    producer:
      acks: all                    # all replicas must confirm
      properties:
        enable.idempotence: true   # exactly-once producer semantics
        retries: 3
        batch-size: 65536          # 64 KB batching
        linger-ms: 5               # 5ms batching window
        buffer-memory: 134217728   # 128 MB buffer
        compression-type: lz4      # fast compression
```

`acks=all` + `enable.idempotence=true` means Kafka will not acknowledge a message until all in-sync replicas have written it, and duplicate sends from retries are deduplicated by the broker. Combined with our commit-only-after-Kafka pattern, this gives us **at-least-once delivery with no duplicates at the producer side**.

---

## MSK IAM Authentication

On AWS MSK, we use IAM authentication instead of username/password. No credentials to rotate:

```yaml
spring:
  kafka:
    properties:
      security.protocol: SASL_SSL
      sasl.mechanism: AWS_MSK_IAM
      sasl.jaas.config: software.amazon.msk.auth.iam.IAMLoginModule required;
      sasl.client.callback.handler.class: >
        software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

The pod's IAM role is granted `kafka:Connect` and `kafka:DescribeCluster` on the MSK cluster. No secrets management required.

---

## WAL Offset Durability

Debezium stores the current WAL position in a file on disk. In Kubernetes, this lives on a PersistentVolumeClaim mounted at `/var/lib/cdc-offsets`:

```yaml
# deployment-template.yaml
volumeMounts:
  - name: cdc-offsets
    mountPath: /var/lib/cdc-offsets
volumes:
  - name: cdc-offsets
    persistentVolumeClaim:
      claimName: cdc-offsets-pvc
```

The Debezium property:

```properties
offset.storage.file.filename=/var/lib/cdc-offsets/agent-offsets.dat
```

> **Important**: if this PVC is lost, the offset file is gone and Debezium doesn't know where it left off. A full re-snapshot is needed. Back up this file if your PVC storage class doesn't handle it.

---

## Liveness Probe Design

The Kubernetes liveness probe checks `GET /health`. The logic handles two cases:

```java
@GetMapping("/health")
public ResponseEntity<Map<String, String>> health() {
    // During graceful shutdown, report UP so K8s doesn't kill the pod
    // before in-flight Kafka sends complete
    if (shuttingDown) {
        return ResponseEntity.ok(Map.of("status", "UP", "reason", "shutting down"));
    }

    // If producer is disabled, there's no engine to check — report UP
    if (debeziumCdcService == null) {
        return ResponseEntity.ok(Map.of("status", "UP", "producer", "disabled"));
    }

    if (!debeziumCdcService.isEngineRunning()) {
        return ResponseEntity.status(503).body(Map.of("status", "DOWN"));
    }

    return ResponseEntity.ok(Map.of("status", "UP"));
}
```

The `shuttingDown` flag is set in a `@PreDestroy` method. This prevents Kubernetes from killing the pod during graceful shutdown when the engine is stopping cleanly — a subtle but important distinction between "engine stopped because of an error" (should restart) and "engine stopped because we asked it to" (should not).

---

## Producer/Consumer Toggle

You can run a **producer-only pod** (writes to Kafka, no Snowflake connections) or a **consumer-only pod** (reads Kafka, no Debezium, no replication slot):

```yaml
cdc:
  producer:
    enabled: true   # set false for consumer-only pod
  consumer:
    enabled: true   # set false for producer-only pod
```

Both default to `true`, so existing deployments need no changes.

The use case for producer-only: during a large Snowflake backfill, you don't need more Debezium instances (each requires its own replication slot). Run additional consumer-only pods instead to scale throughput on the Snowflake side.

---

## Summary

The producer side of pg-kafka-snowflake-streamer is built around three guarantees:

1. **No WAL advancement without Kafka confirmation** — the commit strategy ensures events are never silently dropped
2. **No date/time corruption** — the custom converter handles epoch-zero and timezone edge cases before events reach Kafka
3. **Failure scoped to one pod** — embedded Debezium means one database's problem never affects another's

In [Part 2](./part2-consumer.md), we cover what happens after the event lands in Kafka: the consumer, the Redis readiness gate, schema drift detection, and the Snowflake temp-table MERGE pattern that makes upserts idempotent.

---

*Source code: [github.com/your-org/pg-kafka-snowflake-streamer](https://github.com/your-org/pg-kafka-snowflake-streamer)*
