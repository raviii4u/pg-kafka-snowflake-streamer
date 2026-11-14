package io.pgkafkasnowflakestreamer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "kafka")
public record KafkaConfiguration(
    boolean enabled,
    String bootstrapServers,
    String topic,
    String region,
    boolean iamEnabled
) {
}