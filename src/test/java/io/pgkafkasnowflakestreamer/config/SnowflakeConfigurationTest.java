package io.pgkafkasnowflakestreamer.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.ConfigurationPropertiesBinding;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.test.context.TestPropertySource;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@SpringJUnitConfig
@EnableConfigurationProperties(SnowflakeConfiguration.class)
@TestPropertySource(properties = {
    "snowflake.url=jdbc:snowflake://test.snowflakecomputing.com",
    "snowflake.username=testuser",
    "snowflake.private-key=-----BEGIN PRIVATE KEY-----\nMIItest\n-----END PRIVATE KEY-----",
    "snowflake.database=testdb",
    "snowflake.warehouse=testwh",
    "snowflake.role=testrole",
    "snowflake.schema-mapping.public=target_public",
    "snowflake.schema-mapping.orders=target_orders",
    "snowflake.schema-mapping.management_schema=TARGET_SCHEMA"
})
class SnowflakeConfigurationTest {

    @Autowired
    private SnowflakeConfiguration config;

    @Test
    void shouldBindProperties() {
        assertEquals("jdbc:snowflake://test.snowflakecomputing.com", config.getUrl());
        assertEquals("testuser", config.getUsername());
        assertNotNull(config.getPrivateKey());
        assertEquals("testdb", config.getDatabase());
        assertEquals("testwh", config.getWarehouse());
        assertEquals("testrole", config.getRole());
    }

    @Test
    void shouldMapSchemasWithKubernetesFormat() {
        // Kubernetes env vars need to be converted to Spring property format
        // SNOWFLAKE_SCHEMA-MAPPING_management_schema should be set as:
        // snowflake.schema-mapping.management_schema=TARGET_SCHEMA

        assertEquals("target_public", config.getTargetSchema("public"));
        assertEquals("target_orders", config.getTargetSchema("orders"));
        assertEquals("TARGET_SCHEMA", config.getTargetSchema("management_schema"));
        assertEquals("unmapped", config.getTargetSchema("unmapped"));
    }

    @Test
    void shouldReturnSchemaMapping() {
        Map<String, String> mapping = config.getSchemaMapping();
        assertEquals("target_public", mapping.get("public"));
        assertEquals("target_orders", mapping.get("orders"));
        assertEquals("TARGET_SCHEMA", mapping.get("management_schema"));
    }
}