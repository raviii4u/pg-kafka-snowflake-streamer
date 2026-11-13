# How We Built a Production-Grade PostgreSQL CDC Consumer: Kafka to Snowflake with Schema Drift and Idempotent Merges

## Recap

In [Part 1](./part1-producer.md), we covered the producer side of our CDC pipeline: embedded Debezium reading the PostgreSQL WAL and reliably publishing every change event to Kafka. By the time an event reaches Kafka it is durable, ordered within its table's topic, and confirmed by all in-sync replicas.

Now the hard part begins.

Getting data *into* Kafka is relatively straightforward. Getting it *correctly into Snowflake* — idempotently, with schema drift handled automatically, with at-least-once delivery guarantees, and without ever silently corrupting data — is where most CDC pipelines fail in production.

This is Part 2: the consumer.

---

## Architecture Overview

```
Kafka (AWS MSK)
  │  topic: stream-public-orders
  │  topic: stream-public-users
  │  topic: stream-payments-transactions
  │
  ▼
KafkaConsumerService           ← scheduled every 5 min, manual commit
  │   └── Redis readiness gate  ← won't start until DDL cache is ready
  │
  ▼
SnowflakeSink.processBatch()
  │
  ├── handleSchemaDrift()       ← detects new columns, ALTER TABLE
  │     └── DDLSyncService      ← Redis DDL cache, Snowflake INFORMATION_SCHEMA
  │
  └── processAllTables()        ← 20-thread parallel, fail-fast
        └── per table:
              ├── dedup by PK (LinkedHashMap, last-write-wins)
              ├── CREATE TEMP TABLE with parameterized VALUES
              └── CALL MERGE_CDC_BATCH_WITH_HISTORY(...)
```

---

## The Consumer: `KafkaConsumerService`

The consumer is a **raw Kafka consumer** — not a Spring `@KafkaListener`. This is deliberate. Spring's listener abstraction hides offset commit control, and for our use case that control is critical: we commit offsets only after Snowflake confirms the data was written. Spring's auto-commit would commit them on poll, which would lose data on a Snowflake failure.

```java
@Service
@ConditionalOnProperty(name = "cdc.mode", havingValue = "BATCH")
@ConditionalOnExpression("'${cdc.consumer.enabled:true}'.equals('true')")
public class KafkaConsumerService {

    @Scheduled(fixedDelayString = "${cdc.batch-timeout-ms:300000}")
    public void processScheduledBatch() {
        // 1. Safety check — if the producer stopped unexpectedly, stop consumer too
        if (debeziumCdcService != null && !debeziumCdcService.isEngineRunning()) {
            stopConsumer();
            log.error("FATAL: Debezium engine stopped");
            return;
        }

        // 2. Redis readiness gate — don't process until DDL cache is populated
        if (snapshotEnabled && !redisSchemaUtil.areSchemasAlreadyCloned(snapshotTargetSchemas)) {
            log.info("Schema snapshot not yet completed, consumer not started");
            return;
        }

        // 3. Poll Kafka
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeoutMs));

        // 4. Filter excluded tables
        List<CdcRecord> filtered = filterExcluded(records);

        // 5. Write to Snowflake
        snowflakeSink.processBatch(filtered);

        // 6. Only commit if Snowflake succeeded (no exception thrown above)
        consumer.commitSync();
    }
}
```

The method is scheduled with `fixedDelay` — meaning the 5-minute clock starts *after* the previous batch completes. A slow batch doesn't stack up calls.

### The Stopped-Consumer Pattern

When `processBatch()` throws, the consumer shuts itself down without committing Kafka offsets:

```java
try {
    snowflakeSink.processBatch(filtered);
    consumer.commitSync();
} catch (Exception e) {
    log.error("Batch failed. Stopping consumer.", e);
    stopConsumer(); // closes the Kafka consumer
    // Debezium keeps running — events keep accumulating in Kafka
}
```

This is intentional. Events in Kafka are safe — they have a configurable retention period (we use 7 days). The producer keeps writing. Manual investigation and a pod restart picks up exactly where it left off. No data loss.

---

## The Redis Readiness Gate

This pattern is underappreciated in CDC literature and it's one of the most elegant parts of our design.

**The cold-start problem**: when you first deploy the pipeline, Kafka starts receiving events immediately. But your Snowflake tables don't exist yet, and the consumer doesn't know their column schemas. If the consumer starts processing before the initial data load is complete, it will try to insert into tables that don't exist, or worse, insert with wrong column mappings.

**The solution**: Redis.

Every table's DDL (column names and Snowflake types) is cached in Redis with the key `{schema}.{table}`. Before the consumer starts processing, it checks for the presence of these keys:

```java
public boolean areSchemasAlreadyCloned(String sourceSchemas) {
    for (String schema : sourceSchemas.split(",")) {
        try {
            Set<String> keys = redisTemplate.keys(schema.trim() + ".*");
            if (keys == null || keys.isEmpty()) return false;
        } catch (Exception e) {
            log.warn("Redis check failed for schema {}. Returning false.", schema);
            return false; // fail-safe: if Redis is down, don't process
        }
    }
    return true;
}
```

Redis is not a message queue here — it's a metadata store and gate. The startup sequence is:

```
1. DebeziumCdcService starts → events flow to Kafka

2. PgSnapshotService runs
   └── Creates Snowflake tables from PostgreSQL schema
   └── Caches DDLs to Redis  (schema.table → columns)
   └── Calls startConsumerAfterClone()

3. KafkaConsumerService starts
   └── Redis gate is now open
   └── Begins processing events from Kafka offset 0
```

Events that arrived in Kafka between steps 1 and 3 are safe — they're still there, with `AUTO_OFFSET_RESET=earliest`. Nothing is lost.

---

## Schema Drift: Handling New Columns Without Stopping the Pipeline

This is the problem most self-built CDC pipelines solve badly. A developer runs `ALTER TABLE orders ADD COLUMN discount_code VARCHAR(50)` on PostgreSQL. The next CDC event for that table has a `discount_code` field. Your MERGE fails.

We detect and handle this before processing the batch:

```java
private void handleSchemaDrift(Connection conn, Map<String, List<CdcRecord>> byTable) {
    for (Map.Entry<String, List<CdcRecord>> entry : byTable.entrySet()) {
        String table = entry.getKey();
        List<CdcRecord> records = entry.getValue();

        // Load cached DDL from Redis
        Map<String, Object> cachedDdl = ddlSyncService.loadTableDDL(table);

        if (cachedDdl.isEmpty()) {
            // Brand new table — create it in Snowflake from PostgreSQL schema
            ddlSyncService.handleNewTable(conn, table);
            continue;
        }

        // Find columns in the batch that aren't in the cache
        Set<String> cachedColumns = extractColumnNames(cachedDdl);
        Set<String> batchColumns = extractColumnsFromRecords(records);
        Set<String> newColumns = Sets.difference(batchColumns, cachedColumns);

        if (!newColumns.isEmpty()) {
            batchApplySchemaDrift(conn, table, newColumns);
        }
    }
}

private void batchApplySchemaDrift(Connection conn, String table, Set<String> newColumns) {
    for (String column : newColumns) {
        // IF NOT EXISTS makes this idempotent — concurrent drift is safe
        conn.createStatement().execute(
            "ALTER TABLE " + table + " ADD COLUMN IF NOT EXISTS " + column + " VARCHAR"
        );
    }

    // Re-fetch DDL from Snowflake INFORMATION_SCHEMA and update Redis
    ddlSyncService.refreshDDLFromSnowflake(conn, table);
}
```

The `IF NOT EXISTS` guard makes this idempotent — if two pods try to add the same column simultaneously (possible in a multi-consumer deployment), both succeed without error.

New columns default to `VARCHAR` because we can't know the type from a Debezium JSON event alone. Type inference from JSON is unreliable. The column exists in Snowflake, data flows through, and a human can alter the type later if needed.

---

## The DDL Cache: `DDLSyncService`

The DDL cache in Redis stores structured column metadata per table:

```json
{
  "schema": "PUBLIC",
  "table": "orders",
  "columns": [
    {"name": "id",         "snowflake_type": "NUMBER(10,0)", "nullable": false},
    {"name": "status",     "snowflake_type": "VARCHAR(50)",  "nullable": true},
    {"name": "created_at", "snowflake_type": "TIMESTAMP_LTZ","nullable": true}
  ],
  "last_updated": 1705312200000
}
```

`DDLSyncService` handles the full lifecycle:

- **`loadTableDDL(tableName)`** — reads from Redis key `schema.table` (lowercase)
- **`cacheTableDDLsToRedis(map)`** — bulk `MSET` after initial clone/snapshot
- **`fetchTableDDLFromSnowflake(conn, schema, table)`** — queries `INFORMATION_SCHEMA.COLUMNS`
- **`fetchTableDDLFromPostgres(tableName)`** — direct JDBC to PostgreSQL `information_schema.columns`
- **`mapDataType(pgType, ...)`** — full PostgreSQL → Snowflake type mapping

The type mapping handles 30+ PostgreSQL types. A few non-obvious ones:

| PostgreSQL | Snowflake | Note |
|---|---|---|
| `numeric(p,s)` | `NUMBER(p,s)` | Precision/scale preserved |
| `numeric` (unconstrained) | `NUMBER(38,9)` | Default precision |
| `jsonb` / `json` | `VARIANT` | Snowflake semi-structured |
| `uuid` | `VARCHAR(36)` | Snowflake has no UUID type |
| `bytea` | `VARCHAR` | Binary as hex string |
| `timestamp with time zone` | `TIMESTAMP_LTZ` | Local timezone |
| `timestamp without time zone` | `TIMESTAMP_NTZ` | No timezone |

---

## The Snowflake Sink: `SnowflakeSink`

The sink handles everything from schema drift through to the final MERGE. It uses a **dual-connection design** that is non-obvious but correct:

```
Outer connection (autoCommit=false)
  └── Used for: ALTER TABLE (schema drift), explicit conn.commit()

Per-table connections (autoCommit=true)
  └── Used for: CREATE TEMP TABLE, CALL MERGE procedure
  └── Note: Snowflake stored procedures CANNOT run inside explicit transactions
```

The outer connection batches all `ALTER TABLE` statements for schema drift into one transaction. The per-table connections are separate because Snowflake's stored procedure executor requires autocommit mode.

### Deduplication: Last-Write-Wins

Before building the temp table, we deduplicate records by primary key. A row updated 5 times in a 5-minute window only needs its final state written to Snowflake:

```java
private List<CdcRecord> getFinalInsertsUpdatesForTable(List<CdcRecord> records) {
    // LinkedHashMap preserves insertion order, later puts overwrite earlier ones
    LinkedHashMap<String, CdcRecord> deduped = new LinkedHashMap<>();

    for (CdcRecord record : records) {
        String pk = extractPrimaryKeyHash(record);
        deduped.put(pk, record); // last write for this PK wins
    }

    return new ArrayList<>(deduped.values());
}
```

This is correct because Kafka preserves per-partition order, and each table has exactly one partition. The last event for a primary key in Kafka order is the latest committed state in PostgreSQL.

### Building the Temp Table

Rather than generating dynamic SQL strings (which would be vulnerable to injection), we use parameterized `PreparedStatements`:

```java
private void createTempTableWithData(Connection conn, String tableName,
                                      List<CdcRecord> records, Map<String, Object> ddl) {
    String tempTable = "TEMP_" + tableName
                       + "_" + System.currentTimeMillis()
                       + "_" + ThreadLocalRandom.current().nextInt(10000);

    // CREATE TEMP TABLE matching the target table's schema
    conn.createStatement().execute(
        "CREATE TEMPORARY TABLE " + tempTable +
        " LIKE " + tableName + " INCLUDING ALL"
    );

    // INSERT with parameterized VALUES — no string interpolation of user data
    String insertSql = buildParameterizedInsert(tempTable, columns);
    PreparedStatement ps = conn.prepareStatement(insertSql);

    for (CdcRecord record : records) {
        Map<String, Object> fields = parseRecord(record);
        int i = 1;
        for (String col : columns) {
            ps.setObject(i++, sanitizeValue(fields.get(col), col, ddl));
        }
        ps.setBoolean(i++, "d".equals(getOp(record))); // _IS_DELETED
        ps.setTimestamp(i++, new Timestamp(System.currentTimeMillis())); // _CDC_TIMESTAMP
        ps.addBatch();
    }

    ps.executeBatch();
}
```

The temp table name includes a timestamp and random suffix to avoid collisions when multiple tables are processed concurrently on the connection pool.

### The MERGE Stored Procedure

After the temp table is populated, a single stored procedure call handles the MERGE:

```java
CallableStatement cs = conn.prepareCall(
    "CALL " + database + ".PUBLIC.MERGE_CDC_BATCH_WITH_HISTORY(?, ?, ?, ?)"
);
cs.setString(1, targetTable);   // "PUBLIC.ORDERS"
cs.setString(2, tempTable);     // "TEMP_ORDERS_1705312200000_4821"
cs.setString(3, keyColumns);    // "ID"
cs.setString(4, database);      // "MYAPP_DB"
cs.execute();

String result = cs.getString(1);
if (result.startsWith("ERROR")) {
    throw new RuntimeException("MERGE failed: " + result);
}
```

Inside the stored procedure:

```sql
CREATE OR REPLACE PROCEDURE MERGE_CDC_BATCH_WITH_HISTORY(
    TARGET_TABLE VARCHAR,
    TEMP_TABLE   VARCHAR,
    KEY_COLUMNS  VARCHAR,
    DATABASE_NAME VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS $$
    // Discover columns dynamically — no hardcoded column lists
    var cols = snowflake.execute({sqlText: "DESCRIBE TABLE " + TEMP_TABLE});
    var columnList = [];
    while (cols.next()) { columnList.push(cols.getColumnValue(1)); }

    var keyList    = KEY_COLUMNS.split(',');
    var joinClause = keyList.map(k => `tgt.${k} = src.${k}`).join(' AND ');
    var updateSet  = columnList
        .filter(c => !keyList.includes(c))
        .map(c => `tgt.${c} = src.${c}`)
        .join(', ');

    var mergeSql = `
        MERGE INTO ${TARGET_TABLE} tgt
        USING ${TEMP_TABLE} src ON (${joinClause})
        WHEN MATCHED AND src._IS_DELETED = FALSE
            THEN UPDATE SET ${updateSet}
        WHEN NOT MATCHED AND src._IS_DELETED = FALSE
            THEN INSERT (${columnList.join(', ')})
            VALUES (${columnList.map(c => 'src.' + c).join(', ')})
        WHEN MATCHED AND src._IS_DELETED = TRUE
            THEN DELETE
    `;

    snowflake.execute({sqlText: mergeSql});
    snowflake.execute({sqlText: "DROP TABLE IF EXISTS " + TEMP_TABLE});

    return 'SUCCESS (v5.5): merged into ' + TARGET_TABLE;
$$;
```

Key design decisions:

- **`DESCRIBE TABLE` at runtime** — column discovery is dynamic. No hardcoded column lists. Schema drift is handled transparently.
- **`_IS_DELETED` flag** — `DELETE` operations set this flag in the temp table, and the MERGE translates it to a physical delete in the target.
- **`_CDC_TIMESTAMP`** — every row has the timestamp of the last CDC event that touched it. Useful for incremental downstream queries.
- **`DROP TABLE IF EXISTS`** — the stored procedure cleans up after itself. Temp tables never accumulate.

---

## Value Sanitization

Raw Debezium JSON can contain values that Snowflake rejects silently or with confusing errors. `sanitizeValue()` handles the known edge cases:

```java
private Object sanitizeValue(Object value, String column, Map<String, Object> ddl) {
    if (value == null) return null;

    String snowflakeType = getColumnType(column, ddl);

    // Snowflake NUMBER columns reject NaN and Infinity
    if (isNumericType(snowflakeType) && value instanceof String s) {
        if ("NaN".equals(s) || "Infinity".equals(s) || "-Infinity".equals(s)) return null;
    }

    // VARCHAR has a max of 16,777,216 characters in Snowflake
    if (isVarcharType(snowflakeType) && value instanceof String s) {
        if (s.length() > 16_777_216) return s.substring(0, 16_777_216);
    }

    // TIMESTAMP_NTZ columns must not include timezone offset
    if (snowflakeType.contains("TIMESTAMP_NTZ") && value instanceof String s) {
        return s.replace("Z", "").replaceAll("\\+\\d{2}:\\d{2}$", "");
    }

    return value;
}
```

---

## Parallel Processing with Fail-Fast

Tables in a batch are processed concurrently on a 20-thread pool:

```java
private void processAllTables(Connection outerConn,
                               Map<String, List<CdcRecord>> byTable) throws Exception {
    List<CompletableFuture<Void>> futures = new ArrayList<>();

    for (Map.Entry<String, List<CdcRecord>> entry : byTable.entrySet()) {
        CompletableFuture<Void> future = CompletableFuture.runAsync(
            () -> processTableWithTempAndMerge(entry.getKey(), entry.getValue()),
            executorService
        );
        futures.add(future);
    }

    // Wait for all — if any fails, cancel the rest
    try {
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
    } catch (ExecutionException e) {
        futures.forEach(f -> f.cancel(true));
        throw e; // rethrows — triggers no-commit in consumer
    }
}
```

If `stream-public-orders` fails, futures for all other tables are cancelled. The consumer catches the exception, does not commit Kafka offsets, and shuts down. All tables are re-processed from their last committed Kafka offset on the next run.

A partial commit — where some tables succeed but one fails — would leave the pipeline in an inconsistent state that's hard to detect and painful to recover from.

---

## Circuit Breaker

Three consecutive `processBatch()` failures open the circuit for 5 minutes:

```java
private int consecutiveFailures = 0;
private long circuitOpenedAt = 0;
private static final int FAILURE_THRESHOLD = 3;
private static final long COOLDOWN_MS = 300_000; // 5 minutes

public void processBatch(List<CdcRecord> records) {
    if (consecutiveFailures >= FAILURE_THRESHOLD) {
        long elapsed = System.currentTimeMillis() - circuitOpenedAt;
        if (elapsed < COOLDOWN_MS) {
            throw new RuntimeException("Circuit open. Cooldown: "
                + (COOLDOWN_MS - elapsed) / 1000 + "s remaining");
        }
        consecutiveFailures = 0; // cooldown passed, try again
    }

    try {
        doProcessBatch(records);
        consecutiveFailures = 0; // reset on success
    } catch (Exception e) {
        if (++consecutiveFailures >= FAILURE_THRESHOLD) {
            circuitOpenedAt = System.currentTimeMillis();
        }
        throw e;
    }
}
```

This prevents runaway Snowflake calls during a warehouse outage — a tight retry loop would exhaust connection credits and delay recovery.

---

## HikariCP Pool Configuration

The Snowflake connection pool is tuned for the parallel processing model:

```java
HikariConfig config = new HikariConfig();
config.setMaximumPoolSize(30);         // 20 table threads + outer conn + headroom
config.setMinimumIdle(5);
config.setConnectionTimeout(30_000);
config.setIdleTimeout(600_000);
config.setMaxLifetime(1_800_000);      // rotate connections every 30 min

// Required for multi-statement batches
config.addDataSourceProperty("MULTI_STATEMENT_COUNT", "0");

// Expose pool metrics to Prometheus at /actuator/prometheus
config.setMetricsTrackerFactory(new MicrometerMetricsTrackerFactory(meterRegistry));
```

The Micrometer integration means pool saturation, connection acquisition time, and pending thread counts all appear in `/actuator/prometheus` — valuable for diagnosing slow batches.

---

## Graceful Shutdown

The consumer coordinates shutdown carefully to avoid incomplete batches:

```java
@PreDestroy
public void shutdown() {
    shutdownRequested = true;

    // Wait for any in-progress batch to complete (up to 30 seconds)
    long deadline = System.currentTimeMillis() + 30_000;
    while (batchInProgress && System.currentTimeMillis() < deadline) {
        Thread.sleep(100);
    }

    consumer.close();
}
```

The Kafka consumer is closed only after the in-flight batch finishes. A pod receiving a SIGTERM during a batch will complete the batch, commit Kafka offsets, and then shut down cleanly — no re-processing on the next startup.

---

## End-to-End Guarantees

| Guarantee | How it's achieved |
|---|---|
| No events lost | Kafka offsets committed only after Snowflake confirms |
| No duplicates in Snowflake | MERGE ON primary key — idempotent upserts |
| In-batch duplicates handled | LinkedHashMap dedup — last-write-wins per PK |
| Schema drift handled | Redis DDL cache + `ALTER TABLE IF NOT EXISTS` per batch |
| New tables handled | Auto-create from PostgreSQL `INFORMATION_SCHEMA` |
| Snowflake outage handled | Circuit breaker + consumer stops, producer keeps running |
| Partial batch prevented | Fail-fast `CompletableFuture` cancellation |
| Cold start handled | Redis readiness gate — consumer waits for DDL cache |

---

## Summary

The consumer side of pg-kafka-snowflake-streamer is where the operational complexity lives. The patterns that make it production-grade:

1. **Manual Kafka offset commits** — never commit before Snowflake confirms
2. **Redis readiness gate** — elegant solution to the cold-start ordering problem
3. **Per-batch schema drift detection** — no pipeline pause for `ALTER TABLE ADD COLUMN`
4. **Parameterized temp table + stored procedure MERGE** — idempotent, injection-safe, history-preserving
5. **Fail-fast parallel processing** — no partial commits, clean retry semantics

Together these give you a pipeline that handles the real-world messiness of production PostgreSQL — schema changes, late events, Snowflake outages — without data loss or manual intervention.

---

*Source code: [github.com/your-org/pg-kafka-snowflake-streamer](https://github.com/your-org/pg-kafka-snowflake-streamer)*
