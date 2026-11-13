package io.pgkafkasnowflakestreamer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pgkafkasnowflakestreamer.config.SnowflakeConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class DDLSyncService {
    
    private static final Logger log = LoggerFactory.getLogger(DDLSyncService.class);
    // Use Schema.TableName directly as Redis key
    private final ObjectMapper objectMapper = new ObjectMapper();

    
    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Autowired
    private org.springframework.cloud.fn.common.debezium.DebeziumProperties debeziumProperties;
    
    @Autowired
    private SnowflakeConfiguration snowflakeConfig;
    
    private String getTargetTableName(String tableName) {
        String[] parts = tableName.split("\\.");
        if (parts.length == 2) {
            String sourceSchema = parts[0];
            String targetSchema = snowflakeConfig.getTargetSchema(sourceSchema);
            return targetSchema + "." + parts[1];
        }
        return tableName;
    }
    

    
    public boolean checkSchemaDrift(Connection snowflakeConnection, String tableName, JsonNode record) throws Exception {
        Map<String, Object> savedDDL = loadTableDDL(tableName);
        return checkSchemaDrift(snowflakeConnection, tableName, record, savedDDL);
    }
    
    public Map<String, Map<String, Object>> fetchAllTableDDLFromSnowflake(Connection conn, List<String> tables) throws SQLException {
        Map<String, Map<String, Object>> ddls = new HashMap<>();
        
        for (String tableKey : tables) {
            String[] parts = tableKey.split("\\.");
            String schema = parts[0];
            String tableName = parts[1];
            
            try {
                Map<String, Object> ddl = fetchTableDDLFromSnowflake(conn, schema, tableName);
                ddls.put(tableKey, ddl);
            } catch (Exception e) {
                log.warn("Failed to fetch DDL for {}: {}", tableKey, e.getMessage());
            }
        }
        
        return ddls;
    }
    
    public void cacheTableDDLsToRedis(Map<String, Map<String, Object>> tableDDLs) {
        if (tableDDLs.isEmpty()) return;
        
        Map<String, String> redisData = new HashMap<>();
        for (Map.Entry<String, Map<String, Object>> entry : tableDDLs.entrySet()) {
            try {
                redisData.put(entry.getKey(), objectMapper.writeValueAsString(entry.getValue()));
            } catch (Exception e) {
                log.error("Failed to serialize DDL for {}: {}", entry.getKey(), e.getMessage());
            }
        }
        
        if (!redisData.isEmpty()) {
            redisTemplate.opsForValue().multiSet(redisData);
            log.info("Cached {} DDLs to Redis", redisData.size());
        }
    }
    
    public List<String> createTablesFromPostgreSQL(Connection snowflakeConnection, String targetSchema) throws SQLException {
        List<String> createdTables = new ArrayList<>();
        
        String[] schemaList = debeziumProperties.getProperties().get("schema.include.list").split(",");
        String pgUrl = String.format("jdbc:postgresql://%s:%s/%s", 
            debeziumProperties.getProperties().get("database.hostname"),
            debeziumProperties.getProperties().get("database.port"),
            debeziumProperties.getProperties().get("database.dbname"));
        
        try (Connection pgConnection = DriverManager.getConnection(pgUrl, 
                debeziumProperties.getProperties().get("database.user"),
                debeziumProperties.getProperties().get("database.password"))) {
            
            ensureSchemaExists(snowflakeConnection, targetSchema);
            
            String qry_placeholders = String.join(",", Collections.nCopies(schemaList.length, "?"));
            String qry_sql = "SELECT schemaname, tablename FROM pg_tables WHERE schemaname IN (" + qry_placeholders + ")";
            
            try (PreparedStatement stmt = pgConnection.prepareStatement(qry_sql)) {
                for (int i = 0; i < schemaList.length; i++) {
                    stmt.setString(i + 1, schemaList[i]);
                }
                
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        String sourceSchema = rs.getString("schemaname");
                        String table = rs.getString("tablename");
                        String mappedTargetSchema = snowflakeConfig.getTargetSchema(sourceSchema);
                        
                        if (mappedTargetSchema.equals(targetSchema)) {
                            try {
                                createTableFromPostgres(snowflakeConnection, pgConnection, sourceSchema, table, targetSchema);
                                createdTables.add(targetSchema.toLowerCase() + "." + table.toLowerCase());
                                log.info("Created table from PostgreSQL: {}.{}", targetSchema, table);
                            } catch (Exception e) {
                                log.error("Failed to create table {}.{}: {}", targetSchema, table, e.getMessage());
                            }
                        }
                    }
                }
            }
        }
        
        return createdTables;
    }
    
    public Map<String, Object> fetchTableDDLFromSnowflake(Connection conn, String schema, String tableName) throws SQLException {
        Map<String, Object> ddl = new HashMap<>();
        List<Map<String, Object>> columns = new ArrayList<>();
        
        String sql = "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE FROM information_schema.columns WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, schema.toUpperCase());
            stmt.setString(2, tableName.toUpperCase());
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> column = new HashMap<>();
                    column.put("name", rs.getString("COLUMN_NAME").toLowerCase());
                    column.put("snowflake_type", rs.getString("DATA_TYPE"));
                    column.put("nullable", "YES".equals(rs.getString("IS_NULLABLE")));
                    
                    Integer maxLength = rs.getObject("CHARACTER_MAXIMUM_LENGTH") != null ? rs.getInt("CHARACTER_MAXIMUM_LENGTH") : null;
                    if (maxLength != null) {
                        column.put("max_length", maxLength);
                    }
                    
                    columns.add(column);
                }
            }
        }
        
        ddl.put("schema", schema);
        ddl.put("table", tableName);
        ddl.put("columns", columns);
        ddl.put("last_updated", System.currentTimeMillis());
        return ddl;
    }
    
    public boolean checkSchemaDrift(Connection snowflakeConnection, String tableName, JsonNode record, Map<String, Object> savedDDL) throws Exception {
        Set<String> recordColumns = getRecordColumns(record);

        if (savedDDL.isEmpty()) {
            return true; // New table
        }
        
        Set<String> savedColumns = getSavedTableColumns(savedDDL);

        // Check if CDC record has new columns not in cache
        Set<String> newColumns = new HashSet<>(recordColumns);
        newColumns.removeAll(savedColumns);
        
        if (!newColumns.isEmpty()) {
            log.info("[DRIFT DETECTED] Table: {} - New columns in CDC record: {}", tableName, newColumns);
            return true;
        }
        
        // Skip type checking - too error prone with CDC data
        
        log.info("[NO DRIFT] Table: {} - CDC record matches cached schema", tableName);
        return false;
    }
    
    private boolean hasTypeMismatch(Map<String, Object> savedDDL, Map<String, Object> currentDDL) {
        List<Map<String, Object>> savedColumns = (List<Map<String, Object>>) savedDDL.get("columns");
        List<Map<String, Object>> currentColumns = (List<Map<String, Object>>) currentDDL.get("columns");
        
        if (savedColumns == null || currentColumns == null) return false;
        
        Map<String, String> savedTypes = savedColumns.stream()
            .collect(Collectors.toMap(
                col -> (String) col.get("name"),
                col -> (String) col.get("snowflake_type")
            ));
            
        Map<String, String> currentTypes = currentColumns.stream()
            .collect(Collectors.toMap(
                col -> (String) col.get("name"),
                col -> (String) col.get("snowflake_type")
            ));
        
        // Compare mapped types
        for (Map.Entry<String, String> entry : currentTypes.entrySet()) {
            String columnName = entry.getKey();
            String expectedType = entry.getValue();
            String savedType = savedTypes.get(columnName);
            
            if (savedType != null && !savedType.equals(expectedType)) {
                log.debug("Type mismatch for column {}: saved={}, expected={}", columnName, savedType, expectedType);
                return true;
            }
        }
        
        return false;
    }
    
    public void handleSchemaDrift(Connection snowflakeConnection, String tableName, JsonNode record) throws Exception {
        Map<String, Object> savedDDL = loadTableDDL(tableName);
        
        if (savedDDL.isEmpty()) {
            // Check if table exists in Snowflake first
            if (tableExistsInSnowflake(snowflakeConnection, tableName)) {
                // Table exists but not cached - sync DDL from Snowflake
                Map<String, Object> snowflakeDDL = fetchTableDDLFromSnowflake(snowflakeConnection, tableName);
                saveTableDDL(tableName, snowflakeDDL);
                savedDDL = snowflakeDDL;
                log.info("Synced existing table DDL from Snowflake: {}", tableName);
            } else {
                // Table doesn't exist - create new table
                handleNewTable(snowflakeConnection, tableName);
                return;
            }
        }
        
        // Fetch current DDL from PostgreSQL
        Map<String, Object> currentDDL = fetchTableDDLFromPostgres(tableName);
        
        Set<String> recordColumns = getRecordColumns(record);
        Set<String> savedColumns = getSavedTableColumns(savedDDL);
        Set<String> currentColumns = getSavedTableColumns(currentDDL);
        
        // Check for new columns
        Set<String> newColumns = new HashSet<>(recordColumns);
        newColumns.removeAll(savedColumns);
        
        // Check for deleted columns
        Set<String> deletedColumns = new HashSet<>(savedColumns);
        deletedColumns.removeAll(currentColumns);
        
        boolean schemaChanged = false;
        
        if (!newColumns.isEmpty()) {
            handleNewColumns(snowflakeConnection, tableName, newColumns, currentDDL);
            schemaChanged = true;
        }
        
        if (!deletedColumns.isEmpty()) {
            log.info("Detected deleted columns {} in table {}", deletedColumns, tableName);
            schemaChanged = true;
        }
        
        // Update Redis with latest DDL if schema changed using target schema name
        if (schemaChanged) {
            String[] parts = tableName.split("\\.");
            String sourceSchema = parts[0];
            String targetSchema = snowflakeConfig.getTargetSchema(sourceSchema);
            String targetTableName = targetSchema + "." + parts[1];
            saveTableDDL(targetTableName, currentDDL);
        }
    }
    
    public Map<String, Object> loadTableDDL(String tableName) {
        if (tableName == null || tableName.isEmpty()) {
            log.warn("loadTableDDL called with null/empty tableName");
            return new HashMap<>();
        }
        String targetTableName = getTargetTableName(tableName);
        String redisKey = targetTableName.toLowerCase();
        log.debug("Loading DDL for table: {} -> key: {}", tableName, redisKey);
        String ddlJson = redisTemplate.opsForValue().get(redisKey);
        if (ddlJson != null) {
            log.debug("Found DDL in Redis for key: {}", redisKey);
            try {
                return objectMapper.readValue(ddlJson, Map.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize DDL for " + redisKey, e);
            }
        }
        log.warn("No DDL found in Redis for key: {}", redisKey);
        return new HashMap<>();
    }

    private void saveTableDDL(String tableName, Map<String, Object> ddl) throws Exception {
        String targetTableName = getTargetTableName(tableName);
        String ddlJson = objectMapper.writeValueAsString(ddl);
        redisTemplate.opsForValue().set(targetTableName.toLowerCase(), ddlJson);
        log.info("Saved DDL for table: {}", targetTableName.toLowerCase());
    }
    

    

    
    private Set<String> getRecordColumns(JsonNode record) {
        Set<String> columns = new HashSet<>();
        // Normalize to lowercase — DDL cache stores column names lowercase,
        // but Debezium events preserve original PostgreSQL case (e.g. "firstName").
        // Without normalization, camelCase columns always appear as "new" columns
        // triggering false schema drift on every batch.
        record.fieldNames().forEachRemaining(f -> columns.add(f.toLowerCase()));
        return columns;
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
    

    
    private Map<String, Object> fetchTableDDLFromPostgres(Connection pgConnection, String schema, String table) throws SQLException {
        Map<String, Object> ddl = new HashMap<>();
        List<Map<String, Object>> columns = new ArrayList<>();
        
        String sql = "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, " +
                    "is_nullable, column_default FROM information_schema.columns " +
                    "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position";
        
        try (PreparedStatement stmt = pgConnection.prepareStatement(sql)) {
            stmt.setString(1, schema);
            stmt.setString(2, table);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> column = new HashMap<>();
                    column.put("name", rs.getString("column_name"));
                    column.put("type", rs.getString("data_type"));
                    column.put("snowflake_type", mapDataType(rs.getString("data_type"), 
                                                rs.getObject("character_maximum_length"), 
                                                rs.getObject("numeric_precision"), 
                                                rs.getObject("numeric_scale")));
                    column.put("nullable", "YES".equals(rs.getString("is_nullable")));
                    column.put("default_value", rs.getString("column_default"));
                    if (rs.getObject("character_maximum_length") != null) {
                        column.put("max_length", rs.getInt("character_maximum_length"));
                    }
                    if (rs.getObject("numeric_precision") != null) {
                        column.put("precision", rs.getInt("numeric_precision"));
                    }
                    if (rs.getObject("numeric_scale") != null) {
                        column.put("scale", rs.getInt("numeric_scale"));
                    }
                    columns.add(column);
                }
            }
        }
        
        ddl.put("schema", schema);
        ddl.put("table", table);
        ddl.put("columns", columns);
        ddl.put("last_updated", System.currentTimeMillis());
        
        return ddl;
    }
    
    private void createTableFromDDL(Connection snowflakeConnection, String tableName, Map<String, Object> ddl) throws SQLException {
        List<Map<String, Object>> columns = (List<Map<String, Object>>) ddl.get("columns");
        if (columns == null || columns.isEmpty()) return;
        
        StringBuilder createSql = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
            .append(tableName.toUpperCase()).append(" (");
        
        boolean first = true;
        for (Map<String, Object> column : columns) {
            if (!first) createSql.append(", ");
            createSql.append(((String) column.get("name")).toUpperCase()).append(" ")
                   .append(column.get("snowflake_type"));
            first = false;
        }
        
        createSql.append(")");
        
        try (Statement createStmt = snowflakeConnection.createStatement()) {
            createStmt.execute(createSql.toString());
            log.info("Created table: {}", tableName);
        }
    }
    
    private Map<String, String> getSavedColumnTypes(Map<String, Object> ddl) {
        Object columns = ddl.get("columns");
        if (columns instanceof List) {
            return ((List<Map<String, Object>>) columns).stream()
                .collect(Collectors.toMap(
                    col -> (String) col.get("name"),
                    col -> (String) col.get("snowflake_type")
                ));
        }
        return new HashMap<>();
    }
    
    public void handleNewTable(Connection snowflakeConnection, String tableName) throws Exception {
        log.info("[DRIFT NEW TABLE] Table not found in Snowflake, creating from PostgreSQL schema: {}", tableName);
        String[] parts = tableName.split("\\.");
        String pgSchema = parts[0];
        String table = parts[1];
        String targetSchema = snowflakeConfig.getTargetSchema(pgSchema);
        
        String pgUrl = String.format("jdbc:postgresql://%s:%s/%s", 
            debeziumProperties.getProperties().get("database.hostname"),
            debeziumProperties.getProperties().get("database.port"),
            debeziumProperties.getProperties().get("database.dbname"));
        
        try (Connection pgConnection = DriverManager.getConnection(pgUrl, 
                debeziumProperties.getProperties().get("database.user"),
                debeziumProperties.getProperties().get("database.password"))) {
            
            ensureSchemaExists(snowflakeConnection, targetSchema);
            createTableFromPostgres(snowflakeConnection, pgConnection, pgSchema, table, targetSchema);
            
            // Save DDL to Redis using target schema name
            Map<String, Object> ddl = fetchTableDDLFromPostgres(pgConnection, pgSchema, table);
            String targetTableName = targetSchema + "." + table;
            saveTableDDL(targetTableName, ddl);
            
            log.info("Case 4: Created new table: {}", tableName);
        }
    }
    
    private void handleNewColumns(Connection snowflakeConnection, String tableName, Set<String> newColumns, Map<String, Object> currentDDL) throws SQLException {
        List<Map<String, Object>> columns = (List<Map<String, Object>>) currentDDL.get("columns");
        
        // Convert to target schema table name
        String[] parts = tableName.split("\\.");
        String sourceSchema = parts[0];
        String targetSchema = snowflakeConfig.getTargetSchema(sourceSchema);
        String targetTableName = targetSchema + "." + parts[1];
        
        for (String columnName : newColumns) {
            Map<String, Object> columnInfo = columns.stream()
                .filter(col -> columnName.equals(col.get("name")))
                .findFirst()
                .orElse(null);
            
            if (columnInfo != null) {
                String dataType = (String) columnInfo.get("snowflake_type");
                String alterSql = String.format("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s", targetTableName.toUpperCase(), columnName.toUpperCase(), dataType);
                log.info("[DRIFT ALTER] Executing: {}", alterSql);
                
                try (Statement stmt = snowflakeConnection.createStatement()) {
                    stmt.execute(alterSql);
                    
                    log.info("Added new column {} to table {} with type {}", columnName, targetTableName, dataType);
                }
            } else {
                log.warn("[DRIFT ALTER] Column {} not found in PostgreSQL DDL for table {}", columnName, tableName);
            }
        }
    }
    
    public Map<String, Object> fetchTableDDLFromPostgres(String tableName) throws SQLException {
        String pgUrl = String.format("jdbc:postgresql://%s:%s/%s", 
            debeziumProperties.getProperties().get("database.hostname"),
            debeziumProperties.getProperties().get("database.port"),
            debeziumProperties.getProperties().get("database.dbname"));
        
        String[] parts = tableName.split("\\.");
        String schema = parts[0];
        String table = parts[1];
        
        Map<String, Object> ddl = new HashMap<>();
        List<Map<String, Object>> columns = new ArrayList<>();
        
        try (Connection pgConnection = DriverManager.getConnection(pgUrl, 
                debeziumProperties.getProperties().get("database.user"),
                debeziumProperties.getProperties().get("database.password"))) {
            
            String sql = "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, " +
                        "is_nullable, column_default FROM information_schema.columns " +
                        "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position";
            
            try (PreparedStatement stmt = pgConnection.prepareStatement(sql)) {
                stmt.setString(1, schema);
                stmt.setString(2, table);
                
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> column = new HashMap<>();
                        column.put("name", rs.getString("column_name"));
                        column.put("type", rs.getString("data_type"));
                        column.put("snowflake_type", mapDataType(rs.getString("data_type"), 
                                                    rs.getObject("character_maximum_length"), 
                                                    rs.getObject("numeric_precision"), 
                                                    rs.getObject("numeric_scale")));
                        column.put("nullable", "YES".equals(rs.getString("is_nullable")));
                        column.put("default_value", rs.getString("column_default"));
                        if (rs.getObject("character_maximum_length") != null) {
                            column.put("max_length", rs.getInt("character_maximum_length"));
                        }
                        if (rs.getObject("numeric_precision") != null) {
                            column.put("precision", rs.getInt("numeric_precision"));
                        }
                        if (rs.getObject("numeric_scale") != null) {
                            column.put("scale", rs.getInt("numeric_scale"));
                        }
                        columns.add(column);
                    }
                }
            }
        }
        
        ddl.put("schema", schema);
        ddl.put("table", table);
        ddl.put("columns", columns);
        ddl.put("last_updated", System.currentTimeMillis());
        
        return ddl;
    }
    
//    private void handleDataTypeChanges(Connection snowflakeConnection, String tableName, Map<String, String> savedColumnTypes) throws SQLException {
//        String pgUrl = String.format("jdbc:postgresql://%s:%s/%s",
//            postgresHost, postgresPort, postgresDatabase);
//
//        String[] parts = tableName.split("\\.");
//
//        try (Connection pgConnection = DriverManager.getConnection(pgUrl,
//                postgresUsername, postgresPassword)) {
//
//            String sql = "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale " +
//                        "FROM information_schema.columns WHERE table_schema = ? AND table_name = ?";
//
//            try (PreparedStatement stmt = pgConnection.prepareStatement(sql)) {
//                stmt.setString(1, parts[0]);
//                stmt.setString(2, parts[1]);
//
//                try (ResultSet rs = stmt.executeQuery()) {
//                    while (rs.next()) {
//                        String columnName = rs.getString("column_name");
//                        String currentType = mapDataType(rs.getString("data_type"),
//                                                       rs.getObject("character_maximum_length"),
//                                                       rs.getObject("numeric_precision"),
//                                                       rs.getObject("numeric_scale"));
//
//                        String savedType = savedColumnTypes.get(columnName);
//                        if (savedType != null && !savedType.equals(currentType)) {
//                            String alterSql = String.format("ALTER TABLE %s ALTER COLUMN %s SET DATA TYPE %s",
//                                                           tableName, columnName, currentType);
//                            try (Statement alterStmt = snowflakeConnection.createStatement()) {
//                                alterStmt.execute(alterSql);
//                                log.info("Case 3: Changed data type for column {} in table {} from {} to {}",
//                                        columnName, tableName, savedType, currentType);
//                            }
//                        }
//                    }
//                }
//            }
//        }
//    }
    

    
    private void createTableFromPostgres(Connection snowflakeConnection, Connection pgConnection, 
                                       String pgSchema, String table, String targetSchema) throws SQLException {
        String fullTableName = targetSchema + "." + table;
        
        String sql = "SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale " +
                    "FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position";
        
        try (PreparedStatement stmt = pgConnection.prepareStatement(sql)) {
            stmt.setString(1, pgSchema);
            stmt.setString(2, table);
            
            StringBuilder createSql = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(fullTableName.toUpperCase()).append(" (");
            
            try (ResultSet rs = stmt.executeQuery()) {
                boolean first = true;
                while (rs.next()) {
                    if (!first) createSql.append(", ");
                    createSql.append(rs.getString("column_name").toUpperCase()).append(" ")
                           .append(mapDataType(rs.getString("data_type"), 
                                             rs.getObject("character_maximum_length"), 
                                             rs.getObject("numeric_precision"), 
                                             rs.getObject("numeric_scale")));
                    first = false;
                }
            }
            
            // Add CDC columns
            createSql.append(", _IS_DELETED BOOLEAN DEFAULT FALSE");
            createSql.append(", _CDC_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP");
            createSql.append(")");
            
            log.info("[DRIFT CREATE] Executing: {}", createSql);
            try (Statement createStmt = snowflakeConnection.createStatement()) {
                createStmt.execute(createSql.toString());
                log.info("Created table with CDC columns: {}", fullTableName);
            }
        }
    }
    
    private String mapDataType(String pgType, Object charLength, Object precision, Object scale) {
        switch (pgType.toLowerCase()) {
            case "smallint": case "int2": return "NUMBER(5,0)";
            case "integer": case "int4": return "NUMBER(10,0)";
            case "bigint": case "int8": return "NUMBER(19,0)";
            case "numeric": case "decimal":
                if (precision != null && scale != null) {
                    return String.format("NUMBER(%s,%s)", precision, scale);
                }
                return "NUMBER(38,10)";
            case "real": case "float4": return "FLOAT4";
            case "double precision": case "float8": return "FLOAT8";
            case "boolean": return "BOOLEAN";
            case "date": return "DATE";
            case "time": case "time without time zone": return "TIME";
            case "timestamp": case "timestamp without time zone": return "TIMESTAMP_NTZ";
            case "timestamptz": case "timestamp with time zone": return "TIMESTAMP_TZ";
            case "char": case "character":
                return charLength != null ? String.format("CHAR(%s)", charLength) : "CHAR(1)";
            case "varchar": case "character varying":
                return charLength != null ? String.format("VARCHAR(%s)", charLength) : "VARCHAR(16777216)";
            case "text": return "VARCHAR(16777216)";
            case "bytea": return "VARCHAR(16777216)";
            case "uuid": return "VARCHAR(36)";
            case "json": case "jsonb": return "VARCHAR(16777216)";
            case "array": return "VARCHAR(16777216)";
            default: return "VARCHAR(16777216)";
        }
    }

    private void createHistoryTable(Connection connection, String tableName, String originalCreateSql) throws SQLException {
        // Skip history table creation
        log.info("Skipping history table creation for: {}", tableName);
    }


    

    

    
    private boolean tableExistsInSnowflake(Connection snowflakeConnection, String tableName) throws SQLException {
        String[] parts = tableName.split("\\.");
        String sourceSchema = parts[0];
        String targetSchema = snowflakeConfig.getTargetSchema(sourceSchema);
        String table = parts[1].toUpperCase();
        
        String sql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";
        try (PreparedStatement stmt = snowflakeConnection.prepareStatement(sql)) {
            stmt.setString(1, targetSchema.toUpperCase());
            stmt.setString(2, table);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }
    
    private Map<String, Object> fetchTableDDLFromSnowflake(Connection snowflakeConnection, String tableName) throws SQLException {
        String[] parts = tableName.split("\\.");
        String sourceSchema = parts[0];
        String targetSchema = snowflakeConfig.getTargetSchema(sourceSchema);
        String table = parts[1].toUpperCase();
        
        Map<String, Object> ddl = new HashMap<>();
        List<Map<String, Object>> columns = new ArrayList<>();
        
        String sql = "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE FROM information_schema.columns " +
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION";
        
        try (PreparedStatement stmt = snowflakeConnection.prepareStatement(sql)) {
            stmt.setString(1, targetSchema.toUpperCase());
            stmt.setString(2, table);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> column = new HashMap<>();
                    column.put("name", rs.getString("COLUMN_NAME").toLowerCase());
                    column.put("type", rs.getString("DATA_TYPE"));
                    column.put("snowflake_type", rs.getString("DATA_TYPE"));
                    column.put("nullable", "YES".equals(rs.getString("IS_NULLABLE")));
                    
                    Integer maxLength = rs.getObject("CHARACTER_MAXIMUM_LENGTH") != null ? rs.getInt("CHARACTER_MAXIMUM_LENGTH") : null;
                    if (maxLength != null) {
                        column.put("max_length", maxLength);
                    }
                    
                    columns.add(column);
                }
            }
        }
        
        ddl.put("schema", snowflakeConfig.getTargetSchema(sourceSchema));
        ddl.put("table", parts[1]);
        ddl.put("columns", columns);
        ddl.put("last_updated", System.currentTimeMillis());
        
        return ddl;
    }
    
    private void ensureSchemaExists(Connection connection, String schemaName) throws SQLException {
        String createSchema = "CREATE SCHEMA IF NOT EXISTS " + schemaName;
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createSchema);
        }
    }
}