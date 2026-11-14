package io.pgkafkasnowflakestreamer.service;

import io.pgkafkasnowflakestreamer.config.PgSnapshotProperties;
import io.pgkafkasnowflakestreamer.config.SnowflakeConfiguration;
import io.pgkafkasnowflakestreamer.util.RedisSchemaUtil;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.fn.common.debezium.DebeziumProperties;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.zip.GZIPOutputStream;

@Service
@ConditionalOnProperty(name = "pg.snapshot.enabled", havingValue = "true")
public class PgSnapshotService {

    private static final Logger log = LoggerFactory.getLogger(PgSnapshotService.class);
    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter TS  = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static String ts(long epochMs) {
        return TS.format(java.time.Instant.ofEpochMilli(epochMs).atZone(ZoneId.systemDefault()));
    }

    private final PgSnapshotProperties props;
    private final SnowflakeConfiguration snowflakeConfig;
    private final DebeziumProperties debeziumProperties;

    @Autowired
    private DDLSyncService ddlSyncService;

    @Autowired(required = false)
    private KafkaConsumerService kafkaConsumerService;

    @Autowired
    private RedisSchemaUtil redisSchemaUtil;

    @Value("${pg.snapshot.schedule.datetime:}")
    private String scheduleDateTime;

    public PgSnapshotService(PgSnapshotProperties props, SnowflakeConfiguration snowflakeConfig, DebeziumProperties debeziumProperties) {
        this.props = props;
        this.snowflakeConfig = snowflakeConfig;
        this.debeziumProperties = debeziumProperties;
    }

    @Scheduled(fixedRate = 60000)
    public void checkAndExecuteSnapshot() {
        // If already snapshotted (DDLs in Redis), just ensure Kafka is started
        if (redisSchemaUtil.areSchemasAlreadyCloned(props.getTargetSchemas())) {
            if (kafkaConsumerService != null) {
                kafkaConsumerService.startConsumerAfterClone();
            }
            return;
        }

        if (scheduleDateTime == null || scheduleDateTime.isBlank()) {
            log.warn("[SNAPSHOT] pg.snapshot.schedule.datetime not set, skipping");
            return;
        }

        ZoneId zone = ZoneId.systemDefault();
        ZonedDateTime now = ZonedDateTime.now(zone);
        LocalDateTime scheduled = LocalDateTime.parse(scheduleDateTime, FMT);
        ZonedDateTime scheduledTime = scheduled.atZone(zone);

        log.info("[SNAPSHOT] Scheduled at: {} | Now: {}", scheduledTime, now);

        if (now.truncatedTo(ChronoUnit.MINUTES).equals(scheduledTime.truncatedTo(ChronoUnit.MINUTES))) {
            log.info("[SNAPSHOT] Schedule matched, starting snapshot at {}", ts(System.currentTimeMillis()));
            run();
        }
    }

    public void run() {
        log.info("[SNAPSHOT] Starting PG snapshot");
        long startTime = System.currentTimeMillis();
        try {
            Files.createDirectories(Paths.get(props.getTempDir()));
            List<String[]> tables = resolveTables();
            if (tables.isEmpty()) {
                log.warn("[SNAPSHOT] No tables found for schemas: {}", props.getTargetSchemas());
                return;
            }
            int threads = Math.min(tables.size(), props.getThreads());
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            log.info("[SNAPSHOT] Processing {} tables with {} threads", tables.size(), threads);

            Set<String> failedTables = ConcurrentHashMap.newKeySet();

            List<CompletableFuture<Void>> futures = tables.stream()
                    .map(t -> CompletableFuture.runAsync(() -> {
                        if (!snapshotTable(t[0], t[1])) {
                            failedTables.add((snowflakeConfig.getTargetSchema(t[0]) + "." + t[1]).toLowerCase());
                        }
                    }, executor))
                    .toList();

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            executor.shutdown();

            if (!failedTables.isEmpty()) {
                log.error("[SNAPSHOT] {} tables failed: {} — aborting Redis cache and Kafka start", failedTables.size(), failedTables);
                return;
            }

            // Cache DDLs to Redis only after ALL tables snapshotted successfully
            List<String> tableKeys = tables.stream()
                    .map(t -> (snowflakeConfig.getTargetSchema(t[0]) + "." + t[1]).toLowerCase())
                    .toList();
            try (Connection sfConn = openSnowflake()) {
                Map<String, Map<String, Object>> allDDLs = ddlSyncService.fetchAllTableDDLFromSnowflake(sfConn, tableKeys);
                ddlSyncService.cacheTableDDLsToRedis(allDDLs);
                log.info("[SNAPSHOT] Cached {} DDLs to Redis", allDDLs.size());
            }

            // Start Kafka consumer
            if (kafkaConsumerService != null) {
                kafkaConsumerService.startConsumerAfterClone();
                log.info("[SNAPSHOT] Kafka consumer started");
            }

            long elapsedMs = System.currentTimeMillis() - startTime;
            log.info("[SNAPSHOT] Completed {} tables | start: {} | end: {} | total duration: {}s",
                    tables.size(), ts(startTime), ts(System.currentTimeMillis()), elapsedMs / 1000.0);
        } catch (Exception e) {
            log.error("[SNAPSHOT] Failed after {}s: {}", (System.currentTimeMillis() - startTime) / 1000.0, e.getMessage(), e);
        }
    }

    // ── resolve tables ────────────────────────────────────────────────────────

    private List<String[]> resolveTables() throws SQLException {
        List<String[]> result = new ArrayList<>();
        String schemaIncludeList = debeziumProperties.getProperties().get("schema.include.list");
        if (schemaIncludeList == null || schemaIncludeList.isBlank()) {
            throw new IllegalStateException("debezium.properties.schema.include.list is not configured");
        }
        List<String> schemaList = Arrays.stream(schemaIncludeList.split(",")).map(String::trim).toList();
        String placeholders = String.join(",", Collections.nCopies(schemaList.size(), "?"));

        try (Connection pg = openPostgres();
             PreparedStatement ps = pg.prepareStatement(
                     "SELECT schemaname, tablename FROM pg_tables WHERE schemaname IN (" + placeholders + ")")) {
            for (int i = 0; i < schemaList.size(); i++) ps.setString(i + 1, schemaList.get(i));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) result.add(new String[]{rs.getString(1), rs.getString(2)});
            }
        }
        return result;
    }

    // ── per-table snapshot ────────────────────────────────────────────────────

    private boolean snapshotTable(String pgSchema, String table) {
        String targetSchema = snowflakeConfig.getTargetSchema(pgSchema);
        String qualifiedTarget = targetSchema + "." + table;
        long startTime = System.currentTimeMillis();
        log.info("[SNAPSHOT] Starting {}.{} -> {} at {}", pgSchema, table, qualifiedTarget, ts(startTime));

        try (Connection sfConn = openSnowflake()) {
            String copyColumnList = ensureTargetTable(sfConn, pgSchema, table, qualifiedTarget);

            Path gzipFile = Paths.get(props.getTempDir(), pgSchema + "_" + table + ".csv.gz");
            try {
                long extractStart = System.currentTimeMillis();
                copyToGzip(pgSchema, table, gzipFile, copyColumnList);
                long fileSize = Files.size(gzipFile);
                log.info("[SNAPSHOT] {}: extracted in {}s | gzip size: {}MB",
                        qualifiedTarget,
                        (System.currentTimeMillis() - extractStart) / 1000.0,
                        String.format("%.2f", fileSize / (1024.0 * 1024.0)));
                long putStart = System.currentTimeMillis();
                putAndLoad(sfConn, gzipFile, qualifiedTarget);
                log.info("[SNAPSHOT] {}: PUT+LOAD took {}s", qualifiedTarget, (System.currentTimeMillis() - putStart) / 1000.0);
            } finally {
                Files.deleteIfExists(gzipFile);
            }

            long endTime = System.currentTimeMillis();
            log.info("[SNAPSHOT] Completed {} | start: {} | end: {} | duration: {}s",
                    qualifiedTarget, ts(startTime), ts(endTime), (endTime - startTime) / 1000.0);
            return true;
        } catch (Exception e) {
            log.error("[SNAPSHOT] Failed {}.{} after {}s: {}", pgSchema, table,
                    (System.currentTimeMillis() - startTime) / 1000.0, e.getMessage(), e);
            return false;
        }
    }

    // ── COPY TO STDOUT → gzip file ────────────────────────────────────────────

    private void copyToGzip(String schema, String table, Path outFile, String columnList) throws SQLException, IOException {
        String sql = String.format(
                "COPY (SELECT %s, false AS _is_deleted, to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS _cdc_timestamp FROM %s.%s) TO STDOUT (FORMAT CSV, HEADER TRUE)",
                columnList, schema, table);
        try {
            copyToGzipWithConnection(openPostgres(), sql, outFile);
        } catch (SQLException e) {
            if (!e.getMessage().contains("conflict with recovery")) throw e;
            log.warn("[SNAPSHOT] {}.{} standby conflict, switching to primary", schema, table);
            copyToGzipWithConnection(openPostgresPrimary(), sql, outFile);
        }
    }

    private void copyToGzipWithConnection(Connection pg, String sql, Path outFile) throws SQLException, IOException {
        try (pg;
             OutputStream fileOut = Files.newOutputStream(outFile);
             GZIPOutputStream gzipOut = new GZIPOutputStream(fileOut)) {
            CopyManager copyManager = new CopyManager(pg.unwrap(BaseConnection.class));
            copyManager.copyOut(sql, gzipOut);
        }
        log.debug("[SNAPSHOT] COPY TO gzip done: {}", outFile.getFileName());
    }

    // ── PUT + COPY INTO Snowflake ─────────────────────────────────────────────

    private void putAndLoad(Connection sfConn, Path gzipFile, String qualifiedTarget) throws SQLException {
        try (Statement stmt = sfConn.createStatement()) {
            stmt.execute(String.format(
                    "PUT file://%s @~/pg_snapshot/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE",
                    gzipFile.toAbsolutePath()));

            stmt.execute(String.format(
                    "COPY INTO %s FROM @~/pg_snapshot/%s " +
                            "FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='\"' ESCAPE=NONE ESCAPE_UNENCLOSED_FIELD=NONE PARSE_HEADER=TRUE COMPRESSION=GZIP NULL_IF=('')) " +
                            "PURGE=TRUE MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE",
                    qualifiedTarget.toUpperCase(), gzipFile.getFileName()));
        }
    }

    // ── ensure target table exists in Snowflake ───────────────────────────────

    private String ensureTargetTable(Connection sfConn, String pgSchema, String table, String qualifiedTarget) throws SQLException {
        String targetDb = (props.getTarget().getDatabase() != null && !props.getTarget().getDatabase().isBlank())
                ? props.getTarget().getDatabase() : snowflakeConfig.getDatabase();
        String targetSchema = snowflakeConfig.getTargetSchema(pgSchema);
        try (Statement stmt = sfConn.createStatement()) {
            stmt.execute("CREATE SCHEMA IF NOT EXISTS " + targetDb.toUpperCase() + "." + targetSchema.toUpperCase());
        }

        StringBuilder ddl = new StringBuilder("CREATE OR REPLACE TABLE ")
                .append(qualifiedTarget.toUpperCase()).append(" (");
        List<String> copyCols = new ArrayList<>();

        try (Connection pg = openPostgres();
             PreparedStatement ps = pg.prepareStatement(
                     "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale " +
                             "FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position")) {
            ps.setString(1, pgSchema);
            ps.setString(2, table);
            try (ResultSet rs = ps.executeQuery()) {
                boolean first = true;
                while (rs.next()) {
                    String colName = rs.getString("column_name");
                    String dataType = rs.getString("data_type");
                    if (!first) ddl.append(", ");
                    ddl.append(colName.toUpperCase())
                            .append(" ")
                            .append(mapPgTypeToSnowflake(dataType,
                                    rs.getObject("character_maximum_length", Integer.class),
                                    rs.getObject("numeric_precision", Integer.class),
                                    rs.getObject("numeric_scale", Integer.class)));
                    String quotedCol = '"' + colName + '"';
                    boolean isArray = "ARRAY".equalsIgnoreCase(dataType);
                    boolean isText = dataType.toLowerCase().matches("text|character varying|varchar|character|char|name");
                    boolean isJson = dataType.toLowerCase().matches("json|jsonb");
                    if (isArray || isText) {
                        String expr = isArray ? quotedCol + "::TEXT" : quotedCol;
                        copyCols.add("replace(replace(" + expr + ", E'\\n', ' '), E'\\r', ' ') AS " + quotedCol);
                    } else if (isJson) {
                        copyCols.add("replace(replace(" + quotedCol + "::TEXT, E'\\n', ' '), E'\\r', ' ') AS " + quotedCol);
                    } else {
                        copyCols.add(quotedCol);
                    }
                    first = false;
                }
            }
        }

        ddl.append(", _IS_DELETED BOOLEAN DEFAULT FALSE");
        ddl.append(", _CDC_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP");
        ddl.append(")");

        try (Statement sfStmt = sfConn.createStatement()) {
            sfStmt.execute(ddl.toString());
            log.info("[SNAPSHOT] Created table {}", qualifiedTarget);
        }
        return String.join(", ", copyCols);
    }


    // ── PostgreSQL → Snowflake type mapping ───────────────────────────────────

    private String mapPgTypeToSnowflake(String pgType, Integer charLen, Integer numPrecision, Integer numScale) {
        if (pgType == null) return "VARCHAR(16777216)";
        switch (pgType.toLowerCase()) {
            case "smallint", "int2":                          return "NUMBER(5,0)";
            case "integer", "int", "int4":                    return "NUMBER(10,0)";
            case "bigint", "int8":                            return "NUMBER(19,0)";
            case "numeric", "decimal":                        return (numPrecision != null && numScale != null)
                    ? "NUMBER(" + numPrecision + "," + numScale + ")"
                    : "NUMBER(38,9)";
            case "real", "float4":                            return "FLOAT4";
            case "double precision", "float8":               return "FLOAT8";
            case "boolean":                                   return "BOOLEAN";
            case "date":                                      return "DATE";
            case "time", "time without time zone":            return "TIME";
            case "timestamp", "timestamp without time zone": return "TIMESTAMP_NTZ";
            case "timestamptz", "timestamp with time zone":  return "TIMESTAMP_TZ";
            case "uuid":                                      return "VARCHAR(36)";
            case "json", "jsonb":                             return "VARCHAR(16777216)";
            case "array":                                     return "VARCHAR(16777216)";
            case "character varying", "varchar", "text",
                 "character", "char", "name":                return (charLen != null)
                    ? "VARCHAR(" + charLen + ")"
                    : "VARCHAR(16777216)";
            default:                                          return "VARCHAR(16777216)";
        }
    }

    // ── connections ───────────────────────────────────────────────────────────

    private String getSchemas() {
        String schemas = props.getTargetSchemas();
        if (schemas == null || schemas.isBlank()) {
            throw new IllegalStateException("pg.snapshot.target-schemas is not configured");
        }
        return schemas;
    }
    private Connection openPostgres() throws SQLException {
        Map<String, String> dbzProps = debeziumProperties.getProperties();
        String primaryHost = dbzProps.get("database.hostname");
        String replicaHost = primaryHost.replaceFirst("(\\.c[a-z0-9]+\\.)", "-replica$1");
        String port = dbzProps.get("database.port");
        String database = dbzProps.get("database.dbname");
        String user = dbzProps.get("database.user");
        String password = dbzProps.get("database.password");
        String replicaUrl = String.format("jdbc:postgresql://%s:%s/%s", replicaHost, port, database);
        try {
            Connection conn = DriverManager.getConnection(replicaUrl, user, password);
            log.info("[SNAPSHOT] Connected to replica: {}", replicaHost);
            return conn;
        } catch (SQLException e) {
            log.warn("[SNAPSHOT] Replica connection failed ({}), falling back to primary", replicaHost);
            Connection conn = DriverManager.getConnection(String.format("jdbc:postgresql://%s:%s/%s", primaryHost, port, database), user, password);
            log.info("[SNAPSHOT] Connected to primary: {}", primaryHost);
            return conn;
        }
    }

    private Connection openPostgresPrimary() throws SQLException {
        Map<String, String> dbzProps = debeziumProperties.getProperties();
        String host = dbzProps.get("database.hostname");
        String url = String.format("jdbc:postgresql://%s:%s/%s", host, dbzProps.get("database.port"), dbzProps.get("database.dbname"));
        Connection conn = DriverManager.getConnection(url, dbzProps.get("database.user"), dbzProps.get("database.password"));
        log.info("[SNAPSHOT] Connected to primary (standby conflict fallback): {}", host);
        return conn;
    }

    private Connection openSnowflake() throws Exception {
        String targetDb = (props.getTarget().getDatabase() != null && !props.getTarget().getDatabase().isBlank())
                ? props.getTarget().getDatabase() : snowflakeConfig.getDatabase();
        String url = snowflakeConfig.getUrl()
                + (snowflakeConfig.getUrl().contains("?") ? "&" : "?")
                + "warehouse=" + snowflakeConfig.getWarehouse()
                + "&db=" + targetDb
                + "&MULTI_STATEMENT_COUNT=0";
        if (snowflakeConfig.getRole() != null && !snowflakeConfig.getRole().isBlank())
            url += "&role=" + snowflakeConfig.getRole();
        return DriverManager.getConnection(url, snowflakeConfig.buildConnectionProperties());
    }
}
