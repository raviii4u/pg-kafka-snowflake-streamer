# pg-kafka-snowflake-streamer

A Spring Boot application for streaming Change Data Capture (CDC) events from PostgreSQL to Snowflake using embedded Debezium. Each running instance is a standalone agent dedicated to one PostgreSQL database.

---

⭐ **If this project has been useful to you or your organization, please consider [starring the repo](../../stargazers) — it helps others discover it.**

We'd also love to hear how you're using it. Head over to [GitHub Discussions](../../discussions) and share how pg-kafka-snowflake-streamer helped your team. Your feedback shapes future development.

---

## Architecture

```
PostgreSQL (WAL) → Debezium (embedded) → Kafka (MSK) → Scheduled Consumer → Snowflake
```

## Features:

```
 1. Postgres -> Kafka (with Embedded debezium).
 2. One time data snapshot loads from PG -> Snowflake with built in schema conversion.
 3. Near Real time CDC to Kafka (For Application integrations)
 4. Runs Scheduled micro-batches to move data from Kafka to Snowflake (~5 min, ~10 min configurable)
 5. Built in Schema evolution (New Columns or New table changes created on snowflake target)
```

### One Agent per Database

Each Spring Boot pod handles one PostgreSQL source. This gives complete isolation between databases — independent failure handling, scaling, and configuration.

### Kafka as Buffer

Debezium publishes change events to Kafka topics. The consumer polls on a configurable schedule (default 5 min) and writes a deduplicated microbatch to Snowflake in a single transaction.

### Two CDC Modes

| Mode | Description |
|---|---|
| `BATCH` (default) | Debezium → Kafka → scheduled microbatch → Snowflake |
| `REALTIME` | Debezium → direct Snowflake writes (Kafka consumer not started) |

### Initial Data Seeding (optional)

- **PG Snapshot** (`PG_SNAPSHOT_ENABLED=true`): Bulk-loads PostgreSQL tables via `COPY TO STDOUT` → gzip → Snowflake `PUT`/`COPY INTO`.

After seeding, DDL metadata is cached in Redis and the Kafka consumer activates.

---

## Prerequisites

### PostgreSQL Setup

```sql
-- Enable logical replication (requires restart)
ALTER SYSTEM SET wal_level = logical;

-- Create replication slot
SELECT pg_create_logical_replication_slot('your_slot_name', 'pgoutput');

-- Create publication
CREATE PUBLICATION your_publication FOR ALL TABLES;

-- Grant permissions
GRANT REPLICATION ON DATABASE your_database TO your_user;
```

### Snowflake Setup

```sql
-- Create target schema
CREATE SCHEMA YOUR_SCHEMA;

-- Create dedicated role
CREATE ROLE CDC_ROLE;
GRANT USAGE ON DATABASE YOUR_DATABASE TO ROLE CDC_ROLE;
GRANT USAGE ON SCHEMA YOUR_SCHEMA TO ROLE CDC_ROLE;
GRANT CREATE TABLE ON SCHEMA YOUR_SCHEMA TO ROLE CDC_ROLE;
GRANT INSERT, UPDATE, DELETE, SELECT ON ALL TABLES IN SCHEMA YOUR_SCHEMA TO ROLE CDC_ROLE;
GRANT INSERT, UPDATE, DELETE, SELECT ON FUTURE TABLES IN SCHEMA YOUR_SCHEMA TO ROLE CDC_ROLE;
GRANT USAGE ON WAREHOUSE YOUR_WAREHOUSE TO ROLE CDC_ROLE;
GRANT ROLE CDC_ROLE TO USER your_snowflake_user;
```

### Snowflake Stored Procedure

The MERGE logic lives in a Snowflake JavaScript stored procedure. Deploy it before running the agent:

```sql
-- See MERGE_CDC_BATCH_WITH_HISTORY_SIMPLE.sql
-- Replace <YOUR_DATABASE> and <YOUR_ROLE> before executing
```

### Redis

Redis caches DDL metadata (table schemas) as the readiness gate for the Kafka consumer.

```bash
docker-compose up -d   # starts Redis 7 on port 6379
```

---

## Configuration

All configuration is driven by environment variables. Key ones:

```bash
# PostgreSQL source (passed through Debezium properties)
debezium.properties.database.hostname=your-rds-endpoint.amazonaws.com
debezium.properties.database.port=5432
debezium.properties.database.dbname=your_database
debezium.properties.database.user=your_user
debezium.properties.database.password=your_password
debezium.properties.schema.include.list=public
debezium.properties.slot.name=your_replication_slot
debezium.properties.publication.name=your_publication
debezium.properties.offset.storage.file.filename=/var/lib/cdc-offsets/your-agent-offsets.dat

# Kafka (AWS MSK with IAM auth)
KAFKA_BOOTSTRAP_SERVERS=your-msk-broker:9098
KAFKA_TOPIC_PREFIX=stream-
KAFKA_CONSUMER_TOPIC_PATTERN=stream-.*
KAFKA_CONSUMER_GROUP_ID=cdc-consumer-group
KAFKA_CONSUMER_CLIENT_ID=cdc-client

# Snowflake
SNOWFLAKE_URL=jdbc:snowflake://your-account.snowflakecomputing.com
SNOWFLAKE_USERNAME=your_user
SNOWFLAKE_PRIVATE_KEY=<base64-encoded-pkcs8-private-key>
SNOWFLAKE_DATABASE=YOUR_DATABASE
SNOWFLAKE_WAREHOUSE=YOUR_WAREHOUSE
SNOWFLAKE_ROLE=CDC_ROLE

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=

# CDC mode and batching
CDC_MODE=BATCH
CDC_BATCH_TIMEOUT=300000   # 5 minutes in ms
KAFKA_MAX_POLL_RECORDS=500

# Optional: schema mapping (if Snowflake schema name differs from PostgreSQL schema)
# snowflake.schema-mapping.public=MY_TARGET_SCHEMA

# Optional: override default log level
```

For Kafka with plain (non-IAM) auth, override:
```bash
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
KAFKA_SASL_MECHANISM=PLAIN
```

---

## Running

### Local Testing (PostgreSQL → Kafka)

The quickest way to test the full producer pipeline locally without needing Snowflake.

**1. Start the local stack**

```bash
docker-compose up -d
```

This starts PostgreSQL 16, Kafka 3.7 (KRaft), Kafka UI, and Redis. PostgreSQL comes pre-loaded with sample `customers` and `orders` tables and a Debezium replication slot.

**2. Start the app with the local profile**

```bash
./gradlew bootRun --args='--spring.profiles.active=local'
```

The `local` profile:
- Points Debezium at the local PostgreSQL container
- Uses PLAINTEXT Kafka (no MSK IAM auth)
- Sets replication factor to 1 (single broker)
- Disables the Snowflake consumer so no Snowflake credentials are needed
- Uses dummy Snowflake values so Spring resolves placeholders without crashing

**3. Fire some changes**

```bash
docker exec -it cdc-postgres psql -U postgres -d testdb -c "
  INSERT INTO customers (name, email) VALUES ('Alice', 'alice@test.com');
  UPDATE customers SET name = 'Alice Updated' WHERE email = 'alice@test.com';
  DELETE FROM customers WHERE email = 'bob@example.com';
"
```

**4. Verify events landed in Kafka**

```bash
# List topics (should see stream-public-customers, stream-public-orders)
docker exec cdc-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Read messages from the customers topic
docker exec cdc-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic stream-public-customers \
  --from-beginning --max-messages 10 --timeout-ms 5000
```

Or browse visually at **http://localhost:8090** (Kafka UI).

Each message is a JSON CDC event with `op: c` (insert), `u` (update), or `d` (delete):

```json
{
  "op": "u",
  "before": {"id": 1, "name": "Alice"},
  "after":  {"id": 1, "name": "Alice Updated"},
  "source": {"schema": "public", "table": "customers"}
}
```

**5. Stop everything**

```bash
docker-compose down
```

---

### Full Pipeline (PostgreSQL → Kafka → Snowflake)

Fill in your Snowflake credentials in `.env.local`, then:

```bash
export $(grep -v '#' .env.local | xargs)
# Remove the cdc.consumer.enabled=false line or set it to true in application-local.yml
./gradlew bootRun --args='--spring.profiles.active=local'
```

---

### Build JAR

```bash
./gradlew build
java --add-opens=java.base/java.nio=ALL-UNNAMED \
     -jar build/libs/pg-kafka-snowflake-streamer-0.0.1-SNAPSHOT.jar
```

### Docker (example Dockerfile)

```dockerfile
FROM eclipse-temurin:21-jre
COPY build/libs/pg-kafka-snowflake-streamer-0.0.1-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "--add-opens=java.base/java.nio=ALL-UNNAMED", "-jar", "/app.jar"]
```

### Kubernetes

See `k8s/` for deployment, configmap, and secret templates. Each agent needs:
- A `ConfigMap` with its database-specific properties
- A `Secret` with credentials
- A `PersistentVolumeClaim` mounted at `/var/lib/cdc-offsets` for Debezium WAL position storage

> The PVC is critical — if lost, the Debezium offset is lost and you must re-snapshot.

---

## Health Endpoints

| Endpoint | Description |
|---|---|
| `GET /health` | Liveness: 503 if Debezium engine is down |
| `GET /ready` | Readiness: always 200 |
| `GET /health/cdc` | Detailed CDC status including PostgreSQL TCP reachability |
| `GET /actuator/prometheus` | Prometheus metrics (HikariCP, JVM, etc.) |

---

## Key Design Decisions

**At-least-once + application dedup** — Kafka offsets are committed only after the Snowflake transaction commits. Within each microbatch, records are deduplicated by primary key (last-write-wins using `LinkedHashMap` in Kafka order).

**Schema drift handling** — New columns in CDC events automatically trigger `ALTER TABLE ADD COLUMN IF NOT EXISTS` in Snowflake within the same batch, with the Redis DDL cache updated. Zero manual intervention needed.

**Circuit breaker** — Three consecutive Snowflake batch failures open the circuit for 5 minutes, preventing runaway error loops.

**Parallel table processing** — Each batch processes tables concurrently on a 20-thread pool using `CompletableFuture.allOf()` with fail-fast semantics.

**Graceful degradation** — If Redis is unavailable at startup, Debezium (the producer) keeps running and Kafka buffers events. The consumer won't start until Redis is available.

---

## Project Structure

```
src/main/java/io/cdcstreamers/
├── CdcStreamersApplication.java       Entry point
├── config/                            Spring configuration beans
│   ├── BatchingConfiguration.java     CDC mode, batch size/timeout
│   ├── KafkaAdminConfig.java          KafkaAdmin for topic management
│   ├── KafkaConfig.java               ConsumerFactory (BATCH mode only)
│   ├── KafkaConfiguration.java        Kafka connection properties
│   ├── PgSnapshotProperties.java      PG snapshot feature flags
│   ├── RedisConfig.java               Jedis pool + availability check
│   └── SnowflakeConfiguration.java    Snowflake JDBC + RSA auth
├── controller/
│   └── HealthController.java          /health, /ready, /health/cdc
├── converter/
│   └── CustomDateTimeConverter.java   PG epoch values → ISO strings
├── model/
│   ├── CdcMode.java                   REALTIME | BATCH
│   └── CdcRecord.java                 Simple value + key holder
├── service/
│   ├── DDLCacheChecker.java           Checks Redis for schema readiness
│   ├── DDLSyncService.java            PG→Snowflake type mapping, Redis DDL cache
│   ├── DebeziumCdcService.java        Embedded Debezium engine, Kafka publisher
│   ├── KafkaBatchService.java         Kafka producer wrapper, auto topic creation
│   ├── KafkaConsumerService.java      Scheduled microbatch consumer
│   ├── PgSnapshotService.java         Bulk PG snapshot via COPY TO STDOUT
│   └── SnowflakeSink.java             Core batch processor (dedup, MERGE, circuit breaker)
└── util/
    ├── ChangeEventParser.java         Debezium JSON event parsing
    ├── CompressionUtil.java           GZIP compress/decompress utility
    ├── RedisSchemaUtil.java           Schema-readiness gate check
```

---

## PostgreSQL → Snowflake Type Mapping

| PostgreSQL | Snowflake |
|---|---|
| `smallint` / `int2` | `NUMBER(5,0)` |
| `integer` / `int4` | `NUMBER(10,0)` |
| `bigint` / `int8` | `NUMBER(19,0)` |
| `numeric(p,s)` / `decimal` | `NUMBER(p,s)` or `NUMBER(38,10)` |
| `real` / `float4` | `FLOAT4` |
| `double precision` / `float8` | `FLOAT8` |
| `boolean` | `BOOLEAN` |
| `date` | `DATE` |
| `time` | `TIME` |
| `timestamp` | `TIMESTAMP_NTZ` |
| `timestamptz` | `TIMESTAMP_TZ` |
| `uuid` | `VARCHAR(36)` |
| `json` / `jsonb` / `array` | `VARCHAR(16777216)` |
| `varchar(n)` / `text` | `VARCHAR(n)` or `VARCHAR(16777216)` |

Every CDC-managed table also gets two system columns: `_IS_DELETED BOOLEAN DEFAULT FALSE` and `_CDC_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP`.

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
