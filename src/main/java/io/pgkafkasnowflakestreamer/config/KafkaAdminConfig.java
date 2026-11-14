package io.pgkafkasnowflakestreamer.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaAdminConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.security.protocol:SASL_SSL}")
    private String securityProtocol;

    @Value("${spring.kafka.properties.sasl.mechanism:AWS_MSK_IAM}")
    private String saslMechanism;

    @Value("${spring.kafka.properties.sasl.jaas.config:software.amazon.msk.auth.iam.IAMLoginModule required;}")
    private String saslJaasConfig;

    @Value("${spring.kafka.properties.sasl.client.callback.handler.class:software.amazon.msk.auth.iam.IAMClientCallbackHandler}")
    private String callbackHandler;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put("security.protocol", securityProtocol);
        configs.put("sasl.mechanism", saslMechanism);
        configs.put("sasl.jaas.config", saslJaasConfig);
        configs.put("sasl.client.callback.handler.class", callbackHandler);
        
        KafkaAdmin admin = new KafkaAdmin(configs);
        admin.setAutoCreate(true);
        return admin;
    }
}