package io.pgkafkasnowflakestreamer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Configuration
@ConfigurationProperties(prefix = "snowflake")
public class SnowflakeConfiguration {
    
    private String url;
    private String username;
    private String privateKey;
    private String database;
    private String warehouse;
    private String role;
    private Map<String, String> schemaMapping = new HashMap<>();

    public String getUrl() { return url; }
    public void setUrl(String url) { this.url = url; }
    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }
    public String getPrivateKey() { return privateKey; }
    public void setPrivateKey(String privateKey) { this.privateKey = privateKey; }
    public String getDatabase() { return database; }
    public void setDatabase(String database) { this.database = database; }
    public String getWarehouse() { return warehouse; }
    public void setWarehouse(String warehouse) { this.warehouse = warehouse; }
    public String getRole() { return role; }
    public void setRole(String role) { this.role = role; }
    public Map<String, String> getSchemaMapping() { return schemaMapping; }
    public void setSchemaMapping(Map<String, String> schemaMapping) { this.schemaMapping = schemaMapping; }
    
    public String getTargetSchema(String sourceSchema) {
        return schemaMapping.getOrDefault(sourceSchema, sourceSchema);
    }

    public Properties buildConnectionProperties() throws Exception {
        String pem = privateKey.replaceAll("-----.*?-----", "").replaceAll("\\s", "");
        PrivateKey key = KeyFactory.getInstance("RSA")
            .generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(pem)));
        Properties props = new Properties();
        props.put("user", username);
        props.put("privateKey", key);
        return props;
    }
}