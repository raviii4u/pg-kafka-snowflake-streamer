package io.pgkafkasnowflakestreamer.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.junit.jupiter.api.Assertions.*;

@SpringJUnitConfig
@EnableConfigurationProperties(SnowflakeConfiguration.class)
class SnowflakeEnvVariableTest {

    @Autowired
    private SnowflakeConfiguration config;

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        // Test with lowercase version
        registry.add("snowflake.schema-mapping.management_schema", () -> "TARGET_SCHEMA");
        registry.add("snowflake.url", () -> "jdbc:snowflake://test.snowflakecomputing.com");
    }

    @Test
    void shouldBindFromEnvVariable() {
        assertEquals("TARGET_SCHEMA", config.getTargetSchema("management_schema"));
        assertEquals("jdbc:snowflake://test.snowflakecomputing.com", config.getUrl());
    }
}