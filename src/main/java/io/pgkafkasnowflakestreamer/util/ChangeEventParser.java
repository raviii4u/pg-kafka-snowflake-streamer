package io.pgkafkasnowflakestreamer.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ChangeEventParser {
    
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    public static JsonNode parseEvent(String changeEvent) throws Exception {
        return objectMapper.readTree(changeEvent);
    }
    
    public static JsonNode parseKey(String key) throws Exception {
        return key != null ? objectMapper.readTree(key) : null;
    }
    
    public static String getSchemaName(JsonNode event) {
        JsonNode source = event.get("source");
        if (source == null) return null;
        
        JsonNode schema = source.get("schema");
        return schema != null ? schema.asText() : null;
    }
    
    public static String getTableName(JsonNode event) {
        JsonNode source = event.get("source");
        if (source == null) return null;
        
        JsonNode schema = source.get("schema");
        JsonNode table = source.get("table");
        if (schema == null || table == null) return null;
        
        return schema.asText() + "." + table.asText();
    }
}