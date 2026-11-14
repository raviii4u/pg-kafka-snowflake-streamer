package io.pgkafkasnowflakestreamer.service;

import com.fasterxml.jackson.databind.JsonNode;
import io.pgkafkasnowflakestreamer.config.SnowflakeConfiguration;
import io.pgkafkasnowflakestreamer.model.CdcRecord;
import io.pgkafkasnowflakestreamer.util.ChangeEventParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.metrics.micrometer.MicrometerMetricsTrackerFactory;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

@Service
public class SnowflakeSink {
    private static final Logger log = LoggerFactory.getLogger(SnowflakeSink.class);
    private final SnowflakeConfiguration config;
    private final DDLSyncService ddlSyncService;
    private int consecutiveFailures = 0;
    private long lastFailureTime = 0;
    private static final int MAX_CONSECUTIVE_FAILURES = 3;
    private static final long CIRCUIT_BREAKER_TIMEOUT = 300000; // 5 minutes
    private HikariDataSource dataSource;
    private final ExecutorService executorService;
    private final MeterRegistry meterRegistry;
    private volatile boolean initialized = false;

    public SnowflakeSink(SnowflakeConfiguration config, DDLSyncService ddlSyncService, MeterRegistry meterRegistry) {
        this.config = config;
        this.ddlSyncService = ddlSyncService;
        this.meterRegistry = meterRegistry;
        this.executorService = Executors.newFixedThreadPool(20);
    }

    public void initialize() throws Exception {
        if (initialized) return;
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDataSourceClassName("net.snowflake.client.jdbc.SnowflakeBasicDataSource");
        String url = config.getUrl() + (config.getUrl().contains("?") ? "&" : "?") + "MULTI_STATEMENT_COUNT=0";
        hikariConfig.addDataSourceProperty("url", url);
        hikariConfig.addDataSourceProperty("user", config.getUsername());
        hikariConfig.addDataSourceProperty("privateKey", config.buildConnectionProperties().get("privateKey"));
        hikariConfig.addDataSourceProperty("warehouse", config.getWarehouse());
        hikariConfig.addDataSourceProperty("databaseName", config.getDatabase());
        hikariConfig.addDataSourceProperty("schema", "PUBLIC");
        if (config.getRole() != null && !config.getRole().trim().isEmpty()) {
            hikariConfig.addDataSourceProperty("role", config.getRole());
        }
        hikariConfig.setMaximumPoolSize(30);
        hikariConfig.setMinimumIdle(5);
        hikariConfig.setConnectionTimeout(60000);  // Increased to 60 seconds
        hikariConfig.setMaxLifetime(1800000);
        hikariConfig.setLeakDetectionThreshold(300000);  // 5 minutes - allow for longer operations
        this.dataSource = new HikariDataSource(hikariConfig);
        this.dataSource.setMetricsTrackerFactory(new MicrometerMetricsTrackerFactory(meterRegistry));
        initialized = true;
        log.info("Connection pool initialized with schema: PUBLIC (for temp tables)");
    }

    @PostConstruct
    public void init() {
        try {
            initialize();
        } catch (Exception e) {
            log.error("Failed to initialize Snowflake connection pool: {}. Application will continue without Snowflake connectivity.", e.getMessage(), e);
            // dataSource remains null; batch operations will fail at runtime
        }
    }

    private String sanitizeValue(String value, String fieldName, Map<String, Object> tableDDL) {
        if (value == null) return null;
        
        // Get column info from DDL
        Map<String, Object> columnInfo = getColumnInfo(fieldName, tableDDL);
        String dataType = columnInfo != null ? (String) columnInfo.get("snowflake_type") : null;
        Boolean isNullable = columnInfo != null ? (Boolean) columnInfo.get("nullable") : true;
        
        // Check column length and truncate if value exceeds DDL size
        Integer maxLength = getColumnMaxLength(fieldName, tableDDL);
        if (maxLength != null && value.length() > maxLength) {
            log.warn("Truncating value for field {}: length {} > max {}", fieldName, value.length(), maxLength);
            value = value.substring(0, maxLength);
        }
        
        // Only sanitize numeric values for numeric columns
        if (dataType != null && isNumericType(dataType)) {
            // Handle invalid numeric values that Snowflake doesn't recognize
            if ("NAN".equalsIgnoreCase(value) || "nan".equals(value)) {
                log.warn("Sanitizing invalid numeric value 'NAN' to NULL for field: {}", fieldName);
                return isNullable != null && isNullable ? null : "0";
            }
            if ("INFINITY".equalsIgnoreCase(value) || "-INFINITY".equalsIgnoreCase(value) || 
                "inf".equalsIgnoreCase(value) || "-inf".equalsIgnoreCase(value)) {
                log.warn("Sanitizing invalid numeric value '{}' for field: {}", value, fieldName);
                return isNullable != null && isNullable ? null : "0";
            }
        }
        
        // Handle timestamp conversion for TIMESTAMP_NTZ columns
        if (isTimestampNtzColumn(fieldName, tableDDL)) {
            // Convert TIMESTAMP_TZ to TIMESTAMP_NTZ by removing timezone info
            return value.replaceAll("([+-]\\d{2}:?\\d{2}|Z)$", "");
        }
        return value;
    }
    
    private Map<String, Object> getColumnInfo(String columnName, Map<String, Object> tableDDL) {
        if (tableDDL == null) return null;
        
        try {
            Object columns = tableDDL.get("columns");
            if (columns instanceof List) {
                List<Map<String, Object>> columnList = (List<Map<String, Object>>) columns;
                for (Map<String, Object> col : columnList) {
                    if (columnName.equalsIgnoreCase((String) col.get("name"))) {
                        return col;
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Could not get column info for {}: {}", columnName, e.getMessage());
        }
        return null;
    }
    
    private boolean isNumericType(String dataType) {
        if (dataType == null) return false;
        String type = dataType.toUpperCase();
        return type.startsWith("NUMBER") || type.startsWith("DECIMAL") || 
               type.startsWith("NUMERIC") || type.startsWith("INT") || 
               type.startsWith("BIGINT") || type.startsWith("SMALLINT") || 
               type.startsWith("TINYINT") || type.startsWith("FLOAT") || 
               type.startsWith("DOUBLE") || type.startsWith("REAL");
    }

    private boolean isTimestampNtzColumn(String columnName, Map<String, Object> tableDDL) {
        if (tableDDL == null) {
            return false;
        }
        
        try {
            Object columns = tableDDL.get("columns");
            if (columns instanceof List) {
                List<Map<String, Object>> columnList = (List<Map<String, Object>>) columns;
                for (Map<String, Object> col : columnList) {
                    if (columnName.equalsIgnoreCase((String) col.get("name"))) {
                        String snowflakeType = (String) col.get("snowflake_type");
                        return snowflakeType != null && snowflakeType.startsWith("TIMESTAMP_NTZ");
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Could not check timestamp type for {}: {}", columnName, e.getMessage());
        }
        return false;
    }

    public void processBatch(List<CdcRecord> records) {
        if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
            long timeSinceLastFailure = System.currentTimeMillis() - lastFailureTime;
            if (timeSinceLastFailure < CIRCUIT_BREAKER_TIMEOUT) {
                log.warn("Circuit breaker open - skipping batch");
                throw new RuntimeException("Circuit breaker open");
            }
            consecutiveFailures = 0;
        }
        try (Connection conn = dataSource.getConnection()) {
            // Session parameters already set via connection properties
            conn.setAutoCommit(false);
            // Filter out records with null keys
            List<CdcRecord> validRecords = records.stream()
                .filter(record -> record.key != null)
                .collect(Collectors.toList());
            int skippedRecords = records.size() - validRecords.size();
            if (skippedRecords > 0) {
                log.warn("Skipped {} records with null keys from batch", skippedRecords);
            }
            if (validRecords.isEmpty()) {
                log.warn("No valid records with keys in batch - skipping processing");
                conn.commit();
                return;
            }
            // Group records by table for bulk operations
            Map<String, List<CdcRecord>> recordsByTable = validRecords.stream()
                .collect(Collectors.groupingBy(this::getTableName));
            long batchStartTime = System.currentTimeMillis();
            int networkCallCount = 0;
            log.info("[BATCH START] Processing {} valid records (skipped {}) across {} tables", validRecords.size(), skippedRecords, recordsByTable.size());
            
            // Log detailed batch breakdown
            int totalOriginalRecords = validRecords.size();
            recordsByTable.forEach((table, tableRecords) -> {
                log.info("[BATCH BREAKDOWN] {}: {} records", table, tableRecords.size());
            });
            // Handle schema drift once per table
            int schemaCalls = handleSchemaDrift(conn, recordsByTable);
            networkCallCount += schemaCalls;
            // Process all tables directly
            int tableCalls = processAllTables(conn, recordsByTable);
            networkCallCount += tableCalls;
            conn.commit();
            consecutiveFailures = 0;
            long totalBatchTime = System.currentTimeMillis() - batchStartTime;
            
            // Calculate and log batch summary
            int totalCommittedRecords = recordsByTable.values().stream()
                .mapToInt(tableRecords -> {
                    try {
                        return getFinalInsertsUpdatesForTable(getTableName(tableRecords.get(0)), recordsByTable).size();
                    } catch (Exception e) {
                        return 0;
                    }
                })
                .sum();
            
            log.info("[BATCH SUMMARY] Total: {} pulled → {} valid → {} committed across {} tables in {}ms", 
                records.size(), validRecords.size(), totalCommittedRecords, recordsByTable.size(), totalBatchTime);
        } catch (Exception e) {
            consecutiveFailures++;
            lastFailureTime = System.currentTimeMillis();
            log.error("[BATCH FAILED] Error processing {} records: {}", records.size(), e.getMessage());
            throw new RuntimeException("Batch processing failed - Kafka offsets will NOT be committed", e);
        }
    }

    private String getTableName(CdcRecord record) {
        try {
            JsonNode event = ChangeEventParser.parseEvent(record.value);
            return ChangeEventParser.getTableName(event);
        } catch (Exception e) {
            log.warn("Failed to extract table name from record: {}", e.getMessage());
            return "unknown";
        }
    }

    private int handleSchemaDrift(Connection conn, Map<String, List<CdcRecord>> recordsByTable) throws Exception {
        Map<String, Set<String>> tablesWithDrift = new HashMap<>();
        log.info("[DRIFT CHECK] Checking schema drift for {} tables", recordsByTable.size());
        // Collect all tables with schema drift
        for (Map.Entry<String, List<CdcRecord>> entry : recordsByTable.entrySet()) {
            String tableName = entry.getKey();
            String[] parts = tableName.split("\\.");
            String targetSchema = config.getTargetSchema(parts[0]);
            String targetTableName = targetSchema + "." + parts[1];
            
            Map<String, Object> tableDDL = ddlSyncService.loadTableDDL(targetTableName);
            Set<String> cachedColumns = getSavedTableColumns(tableDDL).stream()
                    .map(String::toLowerCase)
                    .collect(Collectors.toSet());

            // Collect union of all column names from batch payload
            Set<String> batchColumns = new HashSet<>();
            for (CdcRecord record : entry.getValue()) {
                JsonNode event = ChangeEventParser.parseEvent(record.value);
                String operation = event.get("op").asText();
                JsonNode recordData = "c".equals(operation) || "u".equals(operation) || "r".equals(operation) ? event.get("after") : event.get("before");
                if (recordData == null) continue;
                recordData.fieldNames().forEachRemaining(batchColumns::add);
            }

            // Any column in batch not in cached DDL = drift
            batchColumns.removeAll(cachedColumns);
            if (!batchColumns.isEmpty()) {
                log.info("[DRIFT] New columns detected for {}: {}", targetTableName, batchColumns);
                tablesWithDrift.put(tableName, batchColumns);
            }
        }
        
        if (!tablesWithDrift.isEmpty()) {
            log.info("Schema drift detected for {} tables. Applying batch schema changes...", tablesWithDrift.size());
            batchApplySchemaDrift(conn, tablesWithDrift);
        } else {
            log.info("[DRIFT CHECK] Completed - no drift found across {} tables", recordsByTable.size());
        }
        
        return tablesWithDrift.isEmpty() ? 0 : 1;
    }
    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    
    private void batchApplySchemaDrift(Connection conn, Map<String, Set<String>> tablesWithDrift) throws Exception {
        StringBuilder batchAlterSQL = new StringBuilder();
        Map<String, Map<String, Object>> updatedDDLs = new HashMap<>();
        
        for (Map.Entry<String, Set<String>> entry : tablesWithDrift.entrySet()) {
            String tableName = entry.getKey();
            Set<String> newColumns = entry.getValue();
            
            try {
                String[] parts = tableName.split("\\.");
                String targetSchema = config.getTargetSchema(parts[0]);
                String targetTableName = targetSchema + "." + parts[1];
                
                // Check if table exists, create if not
                Map<String, Object> tableDDL = ddlSyncService.loadTableDDL(targetTableName);
                if (tableDDL.isEmpty()) {
                    ddlSyncService.handleNewTable(conn, tableName);
                    log.info("Created new table: {}", targetTableName);
                } else {
                    String alterSQL = buildAlterStatementsForTable(tableName, newColumns);
                    if (alterSQL != null && !alterSQL.trim().isEmpty()) {
                        log.info("[DRIFT ALTER] Generated for {}: {}", targetTableName, alterSQL);
                        if (batchAlterSQL.length() > 0) {
                            batchAlterSQL.append(";\n");
                        }
                        batchAlterSQL.append(alterSQL);
                    }
                }
            } catch (Exception e) {
                log.error("Failed to prepare schema changes for table {}: {}", tableName, e.getMessage());
            }
        }
        
        // Execute ALTER statements individually
        if (batchAlterSQL.length() > 0) {
            try (Statement stmt = conn.createStatement()) {
                String[] statements = batchAlterSQL.toString().split(";\\n");
                for (String sql : statements) {
                    if (!sql.trim().isEmpty()) {
                        log.info("[DRIFT ALTER] Executing: {}", sql.trim());
                        stmt.execute(sql.trim());
                    }
                }
                log.info("Applied schema changes for {} tables", tablesWithDrift.size());
            }
            
            // Fetch updated DDLs from Snowflake after ALTER statements are applied
            for (String tableName : tablesWithDrift.keySet()) {
                String[] parts = tableName.split("\\.");
                String targetSchema = config.getTargetSchema(parts[0]);
                String targetTableName = targetSchema + "." + parts[1];
                Map<String, Object> updatedDDL = fetchUpdatedDDL(tableName);
                if (updatedDDL != null) {
                    updatedDDLs.put(targetTableName.toLowerCase(), updatedDDL);
                }
            }
            
            // Update Redis cache with new DDLs
            ddlSyncService.cacheTableDDLsToRedis(updatedDDLs);
        }
    }
    
    private String buildAlterStatementsForTable(String tableName, Set<String> newColumns) throws Exception {
        String[] parts = tableName.split("\\.");
        String targetSchema = config.getTargetSchema(parts[0]);
        String targetTableName = targetSchema + "." + parts[1];
        
        StringBuilder alterSQL = new StringBuilder();
        Map<String, Object> currentDDL = ddlSyncService.fetchTableDDLFromPostgres(tableName);
        List<Map<String, Object>> columns = (List<Map<String, Object>>) currentDDL.get("columns");
        
        for (String columnName : newColumns) {
            Map<String, Object> columnInfo = columns.stream()
                .filter(col -> columnName.equalsIgnoreCase((String) col.get("name")))
                .findFirst()
                .orElse(null);
            
            if (columnInfo != null) {
                String dataType = (String) columnInfo.get("snowflake_type");
                if (alterSQL.length() > 0) {
                    alterSQL.append(";\n");
                }
                alterSQL.append(String.format("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s",
                    targetTableName.toUpperCase(), columnName.toUpperCase(), dataType));
            }
        }
        
        return alterSQL.toString();
    }
    
    private Map<String, Object> fetchUpdatedDDL(String tableName) throws Exception {
        String[] parts = tableName.split("\\.");
        String targetSchema = config.getTargetSchema(parts[0]);
        try (Connection conn = dataSource.getConnection()) {
            return ddlSyncService.fetchTableDDLFromSnowflake(conn, targetSchema, parts[1]);
        }
    }
    

    private Set<String> getSavedTableColumns(Map<String, Object> ddl) {
        Object columns = ddl.get("columns");
        if (columns instanceof List) {
            return ((List<Map<String, Object>>) columns).stream()
                .map(col -> (String) col.get("name"))
                .collect(Collectors.toSet());
        }
        return new HashSet<>();
    }
    

    
    private int processAllTables(Connection conn, Map<String, List<CdcRecord>> recordsByTable) throws Exception {
        List<String> tableNames = new ArrayList<>(recordsByTable.keySet());
        int totalRecords = tableNames.stream().mapToInt(t -> recordsByTable.getOrDefault(t, new ArrayList<>()).size()).sum();
        
        log.info("Processing {} tables with {} total records using multi-threading", tableNames.size(), totalRecords);
        
        // Process each table in parallel
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        for (String tableName : tableNames) {
            List<CdcRecord> records = recordsByTable.get(tableName);
            if (records == null || records.isEmpty()) continue;
            
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    processTableWithTempAndMerge(tableName, records, recordsByTable);
                } catch (Exception e) {
                    log.error("[TABLE FAILED] Error processing table {}: {}", tableName, e.getMessage());
                    // Log the actual data we're trying to send
                    try {
                        List<CdcRecord> failedRecords = getFinalInsertsUpdatesForTable(tableName, recordsByTable);
                        logFailedRecords(failedRecords, tableName);
                    } catch (Exception logEx) {
                        log.error("Failed to log failed records for {}: {}", tableName, logEx.getMessage());
                    }
                    throw new RuntimeException("Table processing failed: " + tableName, e);
                }
            }, executorService);
            
            futures.add(future);
        }
        
        // Wait for all tables to complete - fail fast if any table fails
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            log.info("All {} tables completed successfully", tableNames.size());
        } catch (Exception e) {
            log.error("[BATCH FAILED] One or more tables failed - cancelling remaining operations");
            // Cancel remaining futures
            futures.forEach(f -> f.cancel(true));
            throw new RuntimeException("Batch processing failed - some tables failed", e);
        }
        
        return tableNames.size(); // Each table = 1 network call
    }
    

    

    
    private boolean createAllTempTablesInBatch(Connection conn, List<String> tableNames, Map<String, List<CdcRecord>> recordsByTable, Map<String, String> tempTableNames) throws Exception {
        try (Statement stmt = conn.createStatement()) {
            for (String tableName : tableNames) {
                List<CdcRecord> records = recordsByTable.get(tableName);
                if (records == null || records.isEmpty()) continue;
                
                String[] parts = tableName.split("\\.");
                String tempTableName = "TEMP_" + parts[1].toUpperCase() + "_" + System.currentTimeMillis() + "_" + ThreadLocalRandom.current().nextInt(100000, 999999);
                tempTableNames.put(tableName, tempTableName);
                
                String[] targetParts = tableName.split("\\.");
                String targetSchema = config.getTargetSchema(targetParts[0]);
                String targetTableName = targetSchema + "." + targetParts[1];
                Map<String, Object> tableDDL = ddlSyncService.loadTableDDL(targetTableName);
                
                List<CdcRecord> finalRecords = getFinalInsertsUpdatesForTable(tableName, recordsByTable);
                createTempTableWithData(conn, tempTableName, finalRecords, tableDDL);
            }
            
            if (stmt.executeBatch().length > 0) {
                log.info("Created {} temp tables in single batch", tableNames.size());
                return true;
            }
        }
        return false;
    }
    

    
    private boolean executeAllMergesInBatch(Connection conn, List<String> tableNames, Map<String, List<CdcRecord>> recordsByTable, Map<String, String> tempTableNames) throws Exception {
        // Stored procedures need autocommit mode
        conn.commit();
        conn.setAutoCommit(true);
        
        try (Statement stmt = conn.createStatement()) {
            int procedureCount = 0;
            
            for (String tableName : tableNames) {
                List<CdcRecord> records = recordsByTable.get(tableName);
                if (records == null || records.isEmpty()) continue;
                
                String tempTableName = tempTableNames.get(tableName);
                if (tempTableName == null) continue;
                
                String[] parts = tableName.split("\\.");
                String targetSchema = config.getTargetSchema(parts[0]);
                String targetTableName = targetSchema + "." + parts[1];
                
                List<CdcRecord> finalInsertsUpdates = getFinalInsertsUpdatesForTable(tableName, recordsByTable);
                if (!finalInsertsUpdates.isEmpty()) {
                    JsonNode firstEvent = ChangeEventParser.parseEvent(finalInsertsUpdates.get(0).value);
                    JsonNode firstKey = finalInsertsUpdates.get(0).key != null ? ChangeEventParser.parseKey(finalInsertsUpdates.get(0).key) : null;
                    
                    if (firstKey != null) {
                        List<String> keyColumnNames = new ArrayList<>();
                        firstKey.fieldNames().forEachRemaining(keyColumnNames::add);
                        String keyColumns = String.join(",", keyColumnNames);
                        
                        String procCall = String.format("CALL %s.PUBLIC.MERGE_CDC_BATCH_WITH_HISTORY('%s', '%s', '%s', '%s')",
                            config.getDatabase(), targetTableName, tempTableName, keyColumns, config.getDatabase());
                        
                        log.info("[MERGE] Executing stored procedure for table {}: {}", targetTableName, procCall);
                        
                        // Execute and validate each procedure individually
                        try (ResultSet rs = stmt.executeQuery(procCall)) {
                            if (rs.next()) {
                                String result = rs.getString(1);
                                log.info("[MERGE] Stored procedure response for {}: {}", targetTableName, result);
                                if (result != null && result.startsWith("ERROR")) {
                                    log.error("[MERGE] Stored procedure failed for {}: {}", targetTableName, result);
                                    throw new SQLException("Stored procedure failed: " + result);
                                }
                            }
                        }
                        procedureCount++;
                    }
                }
            }
            
            log.info("[MERGE] Executed {} stored procedures with response validation", procedureCount);
            
            return true;
        } finally {
            // Resume transaction mode
            conn.setAutoCommit(false);
        }
    }
    


    
    private List<CdcRecord> getFinalInsertsUpdatesForTable(String tableName, Map<String, List<CdcRecord>> recordsByTable) throws Exception {
        List<CdcRecord> records = recordsByTable.get(tableName);
        if (records == null || records.isEmpty()) return new ArrayList<>();
        
        int originalCount = records.size();
        
        // Kafka order maintained - latest operation wins per key
        Map<String, CdcRecord> tempTableData = new LinkedHashMap<>();
        
        for (CdcRecord record : records) {
            try {
                JsonNode keyNode = record.key != null ? ChangeEventParser.parseKey(record.key) : null;
                if (keyNode != null) {
                    String keyHash = buildKeyHash(keyNode);
                    tempTableData.put(keyHash, record);
                }
            } catch (Exception e) {
                log.warn("Failed to process record: {}", e.getMessage());
            }
        }
        
        List<CdcRecord> finalRecords = new ArrayList<>(tempTableData.values());
        int dedupedCount = originalCount - finalRecords.size();
        
        if (dedupedCount > 0) {
            log.info("[TABLE DEDUP] {}: {} original → {} final (deduped {})", tableName, originalCount, finalRecords.size(), dedupedCount);
        }
        
        return finalRecords;
    }
    
    private void createTempTableWithData(Connection conn, String tempTableName, List<CdcRecord> records, Map<String, Object> tableDDL) throws Exception {
        if (records.isEmpty()) return;
        
        // Collect all unique columns from all records
        Set<String> allColumns = new LinkedHashSet<>();
        for (CdcRecord record : records) {
            JsonNode event = ChangeEventParser.parseEvent(record.value);
            JsonNode afterData = event.get("after");
            JsonNode beforeData = event.get("before");
            
            if (afterData != null) {
                afterData.fieldNames().forEachRemaining(fieldName -> allColumns.add(fieldName.toUpperCase()));
            }
            if (beforeData != null) {
                beforeData.fieldNames().forEachRemaining(fieldName -> allColumns.add(fieldName.toUpperCase()));
            }
        }
        
        List<String> columns = new ArrayList<>(allColumns);
        columns.add("_IS_DELETED");
        columns.add("_CDC_TIMESTAMP");
        
        // Build CREATE TABLE AS SELECT with VALUES and parameters
        StringBuilder sql = new StringBuilder("CREATE TEMPORARY TABLE ").append(tempTableName)
            .append(" AS SELECT * FROM VALUES ");
        
        // Add parameter placeholders for each row
        List<String> rowPlaceholders = new ArrayList<>();
        for (int i = 0; i < records.size(); i++) {
            rowPlaceholders.add("(" + String.join(",", Collections.nCopies(columns.size(), "?")) + ")");
        }
        
        sql.append(String.join(", ", rowPlaceholders))
           .append(" AS t(").append(String.join(", ", columns)).append(")");
        
        // Execute with parameters - single network call
        try (PreparedStatement pstmt = conn.prepareStatement(sql.toString())) {
            int paramIndex = 1;

            for (CdcRecord record : records) {
                JsonNode event = ChangeEventParser.parseEvent(record.value);
                String op = event.get("op").asText();
                JsonNode afterData = event.get("after");
                JsonNode beforeData = event.get("before");

                // Build UPPERCASE → originalCase mapping from this event's field names.
                // Columns in the SQL are keyed by UPPERCASE (Snowflake convention), but the
                // JSON payload preserves original PostgreSQL casing (e.g. "firstName").
                // Without this map, afterData.get("FIRSTNAME") returns null for camelCase columns.
                Map<String, String> upperToOriginal = new LinkedHashMap<>();
                if (afterData != null) {
                    afterData.fieldNames().forEachRemaining(f -> upperToOriginal.putIfAbsent(f.toUpperCase(), f));
                }
                if (beforeData != null) {
                    beforeData.fieldNames().forEachRemaining(f -> upperToOriginal.putIfAbsent(f.toUpperCase(), f));
                }

                // Set column values
                for (int i = 0; i < columns.size() - 2; i++) {
                    String column = columns.get(i); // UPPERCASE Snowflake column name
                    String originalFieldName = upperToOriginal.get(column); // original camelCase JSON field name

                    JsonNode value = null;
                    if (afterData != null && originalFieldName != null) {
                        value = afterData.get(originalFieldName);
                    }
                    if ((value == null) && beforeData != null && originalFieldName != null) {
                        value = beforeData.get(originalFieldName);
                    }
                    
                    if (value == null || value.isNull()) {
                        pstmt.setNull(paramIndex++, Types.VARCHAR);
                    } else {
                        String textValue = value.asText();
                        textValue = textValue != null ? textValue.stripTrailing() : null;
                        String sanitizedValue = sanitizeValue(textValue, column, tableDDL);
                        if (sanitizedValue != null) {
                            sanitizedValue = sanitizedValue.replace("&#39;", "'").replace("&quot;", "\"").replace("&amp;", "&");
                            pstmt.setString(paramIndex++, sanitizedValue);
                        } else {
                            pstmt.setNull(paramIndex++, Types.VARCHAR);
                        }
                    }
                }
                
                // Set CDC columns - R operations are not deletes
                pstmt.setBoolean(paramIndex++, "d".equals(op));
                pstmt.setTimestamp(paramIndex++, new Timestamp(System.currentTimeMillis()));
            }
            
            pstmt.execute();
        }
    }
    

    

    
    private void executeBatch(Connection conn, List<String> sqlStatements, String operationType) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            for (String sql : sqlStatements) {
                if (sql.contains("MERGE_CDC_BATCH_WITH_HISTORY")) {
                    log.info("[PROCEDURE] Executing: {}", sql);
                    // Stored procedures cannot run in explicit transactions - commit current transaction first
                    conn.commit();
                    conn.setAutoCommit(true);
                    try (ResultSet rs = stmt.executeQuery(sql)) {
                        if (rs.next()) {
                            String result = rs.getString(1);
                            log.info("[PROCEDURE] Return value: {}", result);
                            if (result != null && result.startsWith("ERROR")) {
                                log.error("[PROCEDURE] Failed: {}", result);
                                throw new SQLException("Stored procedure failed: " + result);
                            }
                        }
                    }
                    log.info("[PROCEDURE] Completed successfully");
                    // Resume transaction mode
                    conn.setAutoCommit(false);
                } else {
                    stmt.addBatch(sql);
                }
            }
            
            // Execute non-procedure statements in batch
            boolean hasNonProcedures = sqlStatements.stream().anyMatch(sql -> !sql.contains("MERGE_CDC_BATCH_WITH_HISTORY"));
            if (hasNonProcedures) {
                int[] results = stmt.executeBatch();
                log.debug("Executed {} batch with {} statements", operationType, results.length);
            }
        }
    }
    


    private String buildKeyHash(JsonNode keyNode) {
        if (keyNode == null) return "null";
        
        // Build hash from actual key field values in consistent order
        List<String> keyParts = new ArrayList<>();
        
        // Sort field names for consistent ordering
        List<String> fieldNames = new ArrayList<>();
        keyNode.fieldNames().forEachRemaining(fieldNames::add);
        Collections.sort(fieldNames);
        
        // Build key string from sorted field values
        for (String fieldName : fieldNames) {
            JsonNode value = keyNode.get(fieldName);
            String text = value.isNull() ? null : value.asText();
            String valueStr = text != null ? text.stripTrailing() : "NULL";
            keyParts.add(fieldName + "=" + valueStr);
        }
        
        return String.join("&", keyParts);
    }

    private void processTableWithTempAndMerge(String tableName, List<CdcRecord> records, Map<String, List<CdcRecord>> recordsByTable) throws Exception {
        String[] parts = tableName.split("\\.");
        String tempTableName = "TEMP_" + parts[1].toUpperCase() + "_" + System.currentTimeMillis() + "_" + ThreadLocalRandom.current().nextInt(100000, 999999);
        
        String[] targetParts = tableName.split("\\.");
        String targetSchema = config.getTargetSchema(targetParts[0]);
        String targetTableName = targetSchema + "." + targetParts[1];
        
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(true); // Multi-statement needs autocommit
            
            Map<String, Object> tableDDL = ddlSyncService.loadTableDDL(targetTableName);
            List<CdcRecord> finalRecords = getFinalInsertsUpdatesForTable(tableName, recordsByTable);
            
            if (finalRecords.isEmpty()) {
                log.warn("[TABLE] No final records for table {}", tableName);
                return;
            }
            
            // 1. Create temp table with data using PreparedStatement
            createTempTableWithData(conn, tempTableName, finalRecords, tableDDL);
            
            // 2. Call stored procedure
            JsonNode firstEvent = ChangeEventParser.parseEvent(finalRecords.get(0).value);
            JsonNode firstKey = finalRecords.get(0).key != null ? ChangeEventParser.parseKey(finalRecords.get(0).key) : null;
            
            if (firstKey != null) {
                List<String> keyColumnNames = new ArrayList<>();
                firstKey.fieldNames().forEachRemaining(keyColumnNames::add);
                String keyColumns = String.join(",", keyColumnNames);
                
                String procCall = String.format("CALL %s.PUBLIC.MERGE_CDC_BATCH_WITH_HISTORY('%s', '%s', '%s', '%s')",
                    config.getDatabase(), targetTableName, tempTableName, keyColumns, config.getDatabase());
                
                log.info("[TABLE] Executing temp table + merge for {}: {} records", targetTableName, finalRecords.size());
                
                // Execute stored procedure
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(procCall)) {
                    if (rs.next()) {
                        String result = rs.getString(1);
                        log.info("[TABLE] Merge result for {}: {}", targetTableName, result);
                        if (result != null && result.startsWith("ERROR")) {
                            throw new SQLException("Stored procedure failed for " + targetTableName + ": " + result);
                        }
                    }
                }
                
                log.info("[TABLE COMMIT] {}: {} records committed successfully", targetTableName, finalRecords.size());
                log.info("[TABLE] Successfully completed {}", targetTableName);
            }
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.warn("Error closing connection for table {}: {}", tableName, e.getMessage());
                }
            }
        }
    }

    @PreDestroy
    public void close() {
        log.info("Shutting down thread pool...");
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
        log.info("SnowflakeSink shutdown complete");
    }
    
    private boolean isTimestampColumn(String columnName, Map<String, Object> tableDDL) {
        if (tableDDL == null) {
            return false;
        }
        
        try {
            Object columns = tableDDL.get("columns");
            if (columns instanceof List) {
                List<Map<String, Object>> columnList = (List<Map<String, Object>>) columns;
                for (Map<String, Object> col : columnList) {
                    if (columnName.equals(col.get("name"))) {
                        String snowflakeType = (String) col.get("snowflake_type");
                        return snowflakeType != null && (snowflakeType.startsWith("TIMESTAMP") || snowflakeType.startsWith("DATE"));
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Could not check timestamp type for {}: {}", columnName, e.getMessage());
        }
        return false;
    }
    

    private Integer getColumnMaxLength(String columnName, Map<String, Object> tableDDL) {
        if (tableDDL == null) return null;
        
        try {
            Object columns = tableDDL.get("columns");
            if (columns instanceof List) {
                List<Map<String, Object>> columnList = (List<Map<String, Object>>) columns;
                for (Map<String, Object> col : columnList) {
                    if (columnName.equalsIgnoreCase(col.get("name").toString())) {
                        if (col.containsKey("max_length")) {
                            return (Integer) col.get("max_length");
                        }
                        break;
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Could not extract column length for {}: {}", columnName, e.getMessage());
        }
        return null;
    }
    
    private boolean isEpochTimestamp(String value) {
        if (value == null || value.trim().isEmpty()) {
            return false;
        }
        
        // Only match the specific problematic epoch format: "Wed Dec 31 18:00:00 CST 1969"
        return value.matches(".*(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\\s+\\d{1,2}\\s+\\d{2}:\\d{2}:\\d{2}\\s+\\w{3}\\s+(1969|1970).*");
    }
    
    private void logFailedRecords(List<CdcRecord> records, String tableName) {
        log.error("[PAYLOAD DEBUG] Failed records for table {}: {} records", tableName, records.size());
        for (int i = 0; i < records.size(); i++) {
            try {
                JsonNode event = ChangeEventParser.parseEvent(records.get(i).value);
                String op = event.get("op").asText();
                JsonNode data = "d".equals(op) ? event.get("before") : event.get("after");
                log.error("[ROW {}] op={}, data={}", i, op, data.toString());
            } catch (Exception e) {
                log.error("[ROW {}] Failed to parse: {}", i, records.get(i).value);
            }
        }
    }
}