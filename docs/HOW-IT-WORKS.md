# How pg-kafka-snowflake-streamer Works

## What It Does

pg-kafka-snowflake-streamer is a Spring Boot application that continuously streams every INSERT, UPDATE, and DELETE from a PostgreSQL database into Snowflake in near real-time. It requires no polling, no full-table scans, and no manual intervention when your PostgreSQL schema changes.

One instance of the application handles exactly one PostgreSQL database. To replicate multiple databases, you deploy multiple pods.

---

## The Big Picture

```
PostgreSQL
  │
  │  Write-Ahead Log (WAL)
  │  logical replication slot
  ▼
┌─────────────────────────────────────────────────┐
│           Spring Boot Pod                       │
│                                                 │
│  ┌─────────────────┐      ┌──────────────────┐  │
│  │  PRODUCER       │      │  CONSUMER        │  │
│  │                 │      │                  │  │
│  │ DebeziumCdc     │      │ KafkaConsumer    │  │
│  │ Service         │      │ Service          │  │
│  │                 │      │  (every 5 min)   │  │
│  │ reads WAL →     │      │                  │  │
│  │ publishes JSON  │      │ polls Kafka →    │  │
│  │ to Kafka        │      │ writes Snowflake │  │
│  └────────┬────────┘      └────────▲─────────┘  │
│           │                        │             │
│           │    ┌───────────────┐   │             │
│           │    │  Redis        │   │             │
│           │    │  DDL Cache    │───┘             │
│           │    │  + Gate       │                 │
│           │    └───────────────┘                 │
└───────────┼─────────────────────────────────────┘
            │
            ▼
        Kafka (AWS MSK)
        topic: stream-public-orders
        topic: stream-public-users
        topic: stream-payments-transactions
            │
            ▼
        Snowflake
        (temp table + MERGE stored procedure)
```

---

## Step-by-Step Flow

### Step 1 — Reading the PostgreSQL WAL

PostgreSQL maintains a Write-Ahead Log (WAL) — a sequential record of every committed database change. The application uses **Debezium**, an open-source CDC library, embedded directly inside the Spring Boot process to tail this log via a logical replication slot.

- No separate Kafka Connect cluster is needed
- Debezium reads every INSERT, UPDATE, DELETE as it is committed
- Each change is serialised to a JSON event

A typical event looks like this:

```json
{
  "source": { "schema": "public", "table": "orders" },
  "op": "u",
  "before": { "id": 42, "status": "pending" },
  "after":  { "id": 42, "status": "shipped" }
}
```

`op` values: `c` = INSERT, `u` = UPDATE, `d` = DELETE, `r` = snapshot read.

Date and timestamp fields are normalised to ISO strings by a custom converter (`CustomDateTimeConverter`) to avoid timezone edge cases in Snowflake.

**WAL offset durability**: Debezium tracks its position in the WAL in a file stored on a Kubernetes PersistentVolumeClaim (`/var/lib/cdc-offsets/`). If the pod restarts, it resumes exactly where it left off — no events are replayed or lost.

---

### Step 2 — Publishing to Kafka

Each change event is published to Kafka by `KafkaBatchService`.

**Topic naming**: `stream-{schema}-{table}`
- `public.orders` → `stream-public-orders`
- `payments.transactions` → `stream-payments-transactions`

Topics are automatically created on first use (1 partition, replication factor 3).

One partition per topic is deliberate — it guarantees that events for a single table are always consumed in the order they were produced.

**Reliability guarantee**: Kafka offsets in the WAL are only advanced *after* Kafka confirms receipt of every event in a batch. If Kafka is unavailable, Debezium stops and will retry from the same WAL position on restart. No events are silently dropped.

Producer settings: `acks=all`, `enable.idempotence=true`, lz4 compression, 128 MB buffer.

---

### Step 3 — The Redis Readiness Gate

Before the consumer starts processing, it checks Redis to confirm that Snowflake tables already exist and their schemas are cached.

This solves the **cold-start problem**: when you first deploy, Kafka starts receiving events immediately but Snowflake has no tables yet. Without this gate, the consumer would try to insert into non-existent tables.

Redis stores the DDL (column names and types) for every table:

```
Key:   public.orders
Value: {"schema":"PUBLIC","table":"orders","columns":[{"name":"id","snowflake_type":"NUMBER(10,0)"},...]}
```

The consumer checks `keys(schema.*)` — if at least one key exists per schema, the gate opens and processing begins. Until then, every scheduled poll is skipped.

The gate is populated by the PG Snapshot seeding path:
- **PG Snapshot** — exports PostgreSQL tables via CSV COPY into Snowflake (for fresh starts)

---

### Step 3b — PostgreSQL Snapshot (Initial Online Load)

Before the Kafka consumer can start, Snowflake tables need to exist and be pre-loaded with the current state of the PostgreSQL database. The PG Snapshot feature handles this — it does a live, online bulk export from PostgreSQL directly into Snowflake without any downtime or locking.

It is enabled via:

```yaml
pg.snapshot:
  enabled: true
  target-schemas: public,payments
  schedule:
    datetime: "2024-01-15 02:00:00"   # when to start the load
  threads: 4                           # parallel table workers
  batch-size: 1500000                  # rows exported per COPY
  temp-dir: /tmp/pg-snapshot           # local staging for gzip files
  target:
    database: MYAPP_DB                 # target Snowflake database
```

#### Scheduling

`PgSnapshotService` runs a lightweight check every 60 seconds. It does two things on each tick:

1. If the Redis DDL cache already has data for the configured schemas (meaning a previous snapshot completed), it skips the export and just ensures the Kafka consumer is started.
2. If the cache is empty and the current time matches `pg.snapshot.schedule.datetime` (to the minute), it triggers the full snapshot run.

This means you set the schedule once in the configmap, deploy the pod, and the snapshot runs automatically at the configured time.

#### Per-Table Snapshot Flow

All tables across the configured schemas are discovered from PostgreSQL's `information_schema.tables`. They are then snapshotted in parallel across a configurable thread pool (default 4 threads).

For each table, the process is:

**1. Create the Snowflake target table**

The schema is read directly from PostgreSQL's `information_schema.columns` and translated to Snowflake types. The table is created with `CREATE OR REPLACE TABLE` so re-runs are safe:

```sql
CREATE OR REPLACE TABLE PUBLIC.ORDERS (
    ID           NUMBER(10,0),
    STATUS       VARCHAR(50),
    CREATED_AT   TIMESTAMP_LTZ,
    AMOUNT       NUMBER(12,2),
    _IS_DELETED  BOOLEAN DEFAULT FALSE,
    _CDC_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
)
```

Two CDC metadata columns are added to every table:
- `_IS_DELETED` — used by the MERGE stored procedure to process deletes
- `_CDC_TIMESTAMP` — timestamp of the last CDC event for auditing

Text and array columns get newline/carriage-return stripping applied during the COPY select (`replace(col, E'\n', ' ')`) to keep CSV output clean.

**2. Export from PostgreSQL via COPY TO STDOUT**

The data is streamed out of PostgreSQL using `COPY ... TO STDOUT (FORMAT CSV, HEADER TRUE)` via pgjdbc's `CopyManager`. The output is written directly to a gzip-compressed file on disk:

```sql
COPY (
    SELECT
        "id", "status", "created_at", "amount",
        false AS _is_deleted,
        to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS _cdc_timestamp
    FROM public.orders
) TO STDOUT (FORMAT CSV, HEADER TRUE)
```

The COPY runs against the **replica first** (the hostname with `-replica` appended), falling back to the primary on two conditions:
- Replica connection fails (replica not available)
- PostgreSQL returns a `conflict with recovery` error (replica is in the middle of applying WAL, which blocks read queries)

This replica-first strategy means the snapshot never competes with production write traffic on the primary.

The gzip file lands in `tempDir` (default `/tmp/pg-snapshot`). The file is deleted immediately after the upload step regardless of success or failure.

**3. PUT and COPY INTO Snowflake**

The gzip file is uploaded to Snowflake's internal user stage and loaded in a single `COPY INTO` statement:

```sql
-- Upload the gzip file to the user stage
PUT file:///tmp/pg-snapshot/public_orders.csv.gz @~/pg_snapshot/
    AUTO_COMPRESS=FALSE OVERWRITE=TRUE

-- Load from stage into the target table
COPY INTO PUBLIC.ORDERS
FROM @~/pg_snapshot/public_orders.csv.gz
FILE_FORMAT = (
    TYPE = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    PARSE_HEADER = TRUE
    COMPRESSION = GZIP
    NULL_IF = ('')
)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PURGE = TRUE
```

`MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE` means column ordering doesn't matter — the CSV header drives the mapping. `PURGE = TRUE` removes the staged file automatically after a successful load.

#### After All Tables Complete

Once every table has been successfully snapshotted, two things happen atomically:

1. **DDLs are cached to Redis** — `DDLSyncService` queries Snowflake's `INFORMATION_SCHEMA.COLUMNS` for all snapshotted tables and bulk-writes the metadata to Redis:
   ```
   Key:   public.orders
   Value: {"schema":"PUBLIC","table":"orders","columns":[...],"last_updated":...}
   ```
   This is the same cache format the CDC consumer uses for schema drift detection. The consumer is now fully informed about every table's column layout.

2. **Kafka consumer is started** — `kafkaConsumerService.startConsumerAfterClone()` is called. The Redis readiness gate is now open. CDC events that have been accumulating in Kafka since pod startup are processed from the earliest offset.

If any table fails, the Redis cache is **not** populated and the consumer is **not** started. The pod logs the failed tables and exits the snapshot run cleanly. Fix the issue and restart the pod — the scheduler will re-trigger at the next matching minute.

#### Snapshot Flow Summary

```
PgSnapshotService (every 60s check)
  │
  ├── Redis already has DDLs? → start consumer, done
  │
  └── scheduleDateTime matches now?
        │
        └── run()
              │
              ├── resolveTables()  ← queries PG information_schema
              │
              ├── parallel (4 threads):
              │     └── per table:
              │           ├── ensureTargetTable()
              │           │     ├── openPostgres (replica → primary fallback)
              │           │     ├── read PG information_schema.columns
              │           │     ├── mapPgTypeToSnowflake()
              │           │     └── CREATE OR REPLACE TABLE in Snowflake
              │           │
              │           ├── copyToGzip()
              │           │     ├── COPY TO STDOUT → GZIPOutputStream
              │           │     └── standby conflict → retry on primary
              │           │
              │           └── putAndLoad()
              │                 ├── PUT file → @~/pg_snapshot/
              │                 └── COPY INTO ... MATCH_BY_COLUMN_NAME
              │
              ├── all tables succeeded?
              │     ├── YES → fetchAllTableDDLFromSnowflake()
              │     │         cacheTableDDLsToRedis()
              │     │         startConsumerAfterClone()
              │     └── NO  → log failures, skip Redis + consumer start
```

### Step 4 — The Kafka Consumer

`KafkaConsumerService` runs on a **5-minute schedule** (configurable). Each run:

1. Checks that the Debezium producer is still running
2. Checks the Redis readiness gate
3. Polls Kafka for up to 60 seconds
4. Filters out any excluded tables
5. Sends the batch to `SnowflakeSink`
6. **Only if Snowflake succeeds**: commits Kafka offsets

The critical design: Kafka offsets are committed **after** Snowflake confirms success. If Snowflake fails, offsets are not committed. On the next run, the same events are re-read from Kafka and re-processed. This gives **at-least-once delivery** — events are never silently lost.

If the batch fails, the consumer shuts itself down (but the producer keeps running). Events accumulate safely in Kafka until the consumer is restarted.

---

### Step 5 — Writing to Snowflake

`SnowflakeSink` handles everything from schema management to the final write.

#### 5a. Schema Drift Detection

Before writing, every batch is checked against the Redis DDL cache. If a column exists in the event payload but not in the cache, it means a new column was added to PostgreSQL.

The sink handles this automatically, without pausing the pipeline:

```sql
ALTER TABLE public.orders ADD COLUMN IF NOT EXISTS discount_code VARCHAR
```

The Redis DDL cache is updated immediately after the ALTER succeeds. The next batch sees the new column and processes normally.

#### 5b. Deduplication

If the same row was updated multiple times within a 5-minute window, only the final state needs to be written. The sink deduplicates by primary key using a `LinkedHashMap` — the last event for each primary key in Kafka order wins.

Example: a row updated 5 times in one window → 1 write to Snowflake instead of 5.

#### 5c. Temp Table Creation

For each table in the batch, a temporary table is created in Snowflake:

```sql
CREATE TEMPORARY TABLE TEMP_ORDERS_1705312200000_4821
  LIKE PUBLIC.ORDERS INCLUDING ALL
```

Records are inserted using parameterized `PreparedStatement` batches (safe from SQL injection). Two extra columns are added:
- `_IS_DELETED` — `TRUE` for DELETE events, `FALSE` for INSERT/UPDATE
- `_CDC_TIMESTAMP` — the time the event was processed

#### 5d. The MERGE

A stored procedure performs an atomic MERGE from the temp table into the target:

```sql
MERGE INTO PUBLIC.ORDERS tgt
USING TEMP_ORDERS_... src ON (tgt.ID = src.ID)
WHEN MATCHED AND src._IS_DELETED = FALSE
    THEN UPDATE SET tgt.STATUS = src.STATUS, tgt.UPDATED_AT = src.UPDATED_AT, ...
WHEN NOT MATCHED AND src._IS_DELETED = FALSE
    THEN INSERT (ID, STATUS, ...) VALUES (src.ID, src.STATUS, ...)
WHEN MATCHED AND src._IS_DELETED = TRUE
    THEN DELETE
```

The stored procedure discovers columns dynamically via `DESCRIBE TABLE` — no hardcoded column lists. Schema drift is transparent. The temp table is dropped at the end of the procedure.

Tables within a batch are processed in parallel (20-thread pool). If any table fails, all others are cancelled and the entire batch is re-tried on the next run — no partial commits.

#### 5e. Circuit Breaker

Three consecutive batch failures open a 5-minute circuit breaker. This prevents runaway Snowflake calls during an outage. The counter resets on any success.

---

## Startup Sequence

```
1. Spring Boot starts

2. SnowflakeSink initialises
   └── HikariCP connection pool warms up (30 max connections)
   └── Registers pool metrics with Prometheus

3. DebeziumCdcService starts (if cdc.producer.enabled=true)
   └── Connects to PostgreSQL replication slot
   └── Begins tailing the WAL
   └── Events flow to Kafka immediately

4. Redis availability checked
   └── If Redis is down → consumer disabled, pod logs a FATAL error
   └── Producer keeps running regardless

5. PgSnapshotService runs (if pg.snapshot.enabled=true)
   └── Checks every 60s until scheduleDateTime matches
   └── Skips entirely if Redis DDL cache already populated (re-deploy safe)
   └── Connects to replica (falls back to primary on failure)
   └── For each table in parallel:
         CREATE OR REPLACE TABLE in Snowflake (from PG schema)
         COPY TO STDOUT → gzip file → PUT → COPY INTO Snowflake
   └── On full success: caches all DDLs to Redis, starts consumer
   └── On any failure: logs failed tables, does NOT start consumer

6. KafkaConsumerService starts (if cdc.consumer.enabled=true AND cdc.mode=BATCH)
   └── Redis gate is now open
   └── Begins processing from earliest Kafka offset
```

---

## Producer / Consumer Split

You can run the producer and consumer independently by setting properties:

```yaml
cdc:
  producer:
    enabled: true   # false = consumer-only pod (no Debezium, no replication slot)
  consumer:
    enabled: true   # false = producer-only pod (no Snowflake connections)
```

Both default to `true`. Use cases:
- **Producer-only**: write to Kafka without reading. Useful during large Snowflake backfills where you want to scale consumers separately.
- **Consumer-only**: read from Kafka and write to Snowflake without a local Debezium engine. Useful for scaling write throughput across multiple consumer pods sharing one Kafka topic.

---

## Health and Observability

### Health Endpoints

| Endpoint | Purpose | Returns 503 when |
|---|---|---|
| `GET /health` | Kubernetes liveness | Debezium engine stopped unexpectedly |
| `GET /ready` | Kubernetes readiness | Never (always 200) |
| `GET /health/cdc` | Detailed CDC status | Engine stopped or PostgreSQL unreachable |

The liveness probe returns `200 UP` during graceful shutdown (pod receiving SIGTERM) to prevent Kubernetes from killing in-flight work before it completes.

### Metrics

Prometheus metrics are exposed at `/actuator/prometheus`, including:
- HikariCP connection pool stats (active, pending, idle connections)
- Spring Boot default JVM and HTTP metrics
- Kafka producer metrics

### Error Tracking

FATAL-level errors are logged with structured log entries for:
- Consumer batch failure
- Debezium engine stopped unexpectedly
- Redis unavailable at startup

---

## Kubernetes Deployment

Each pod corresponds to one PostgreSQL database. The deployment model is:

```
Pod: streamer-myapp-db
  ConfigMap: pg-kafka-snowflake-streamer-myapp-db   ← non-secret config
  Secret:    pg-kafka-snowflake-streamer-myapp-db   ← passwords, keys
  PVC:       cdc-offsets-pvc                         ← Debezium WAL offset file
```

Resources per pod: 100m–1000m CPU, 512Mi–1Gi memory.

The `scripts/` directory contains tooling to generate and deploy YAML for many agents at once from a configuration list.

---

## Configuration Reference

### Core Properties

| Property | Default | Description |
|---|---|---|
| `cdc.mode` | `BATCH` | `BATCH` = Kafka-buffered. `REALTIME` = direct Snowflake writes |
| `cdc.producer.enabled` | `true` | Enable/disable the Debezium producer |
| `cdc.consumer.enabled` | `true` | Enable/disable the Kafka consumer |
| `cdc.batch-timeout-ms` | `300000` | Consumer poll interval (5 minutes) |
| `cdc.batch-size` | `500` | Max records per batch |

### PostgreSQL Snapshot

| Property | Default | Description |
|---|---|---|
| `pg.snapshot.enabled` | `false` | Enable the initial PG→Snowflake load |
| `pg.snapshot.target-schemas` | — | Comma-separated list of PostgreSQL schemas to snapshot |
| `pg.snapshot.schedule.datetime` | — | When to run: `"yyyy-MM-dd HH:mm:ss"`. Checked every 60s |
| `pg.snapshot.threads` | `4` | Parallel table worker threads |
| `pg.snapshot.batch-size` | `1500000` | Rows per COPY export |
| `pg.snapshot.temp-dir` | `/tmp/pg-snapshot` | Local staging directory for gzip files |
| `pg.snapshot.target.database` | (snowflake.database) | Target Snowflake database override |

### Snowflake

| Property | Description |
|---|---|
| `snowflake.url` | JDBC URL |
| `snowflake.username` | Service account username |
| `snowflake.private-key` | Base64-encoded PKCS8 private key |
| `snowflake.database` | Target database |
| `snowflake.warehouse` | Compute warehouse |
| `snowflake.schema-mapping` | Map of PostgreSQL schema → Snowflake schema overrides |

### Debezium (PostgreSQL source)

| Property | Description |
|---|---|
| `debezium.properties.database.hostname` | PostgreSQL host |
| `debezium.properties.database.dbname` | Source database name |
| `debezium.properties.schema.include.list` | Schemas to capture |
| `debezium.properties.slot.name` | Logical replication slot name |
| `debezium.properties.snapshot.mode` | `never` (snapshots handled separately) |

### Kafka

| Property | Default | Description |
|---|---|---|
| `kafka.topic` | `stream-` | Topic name prefix |
| `spring.kafka.bootstrap-servers` | — | MSK bootstrap URLs |


Works great for small to medium scale databases and applications.