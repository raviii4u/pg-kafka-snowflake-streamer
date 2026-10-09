package io.cdcstreamers.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
@ConditionalOnExpression("'${cdc.mode:REALTIME}' == 'BATCH' && '${cdc.consumer.enabled:true}' != 'false'")
public class KafkaConfig {
    
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    
    @Value("${kafka.consumer.group-id:stream-group}")
    private String consumerGroupId;
    
    @Value("${kafka.max-poll-records:5000}")
    private int maxPollRecords;
    
    @Value("${kafka.max-partition-fetch-bytes:5242880}")
    private int maxPartitionFetchBytes;
    
    @Value("${spring.kafka.consumer.max-poll-interval:86400000}")
    private int maxPollIntervalMs;
    
    @Value("${spring.kafka.properties.request.timeout.ms:60000}")
    private int requestTimeoutMs;
    
    @Value("${spring.kafka.consumer.session-timeout:45000}")
    private int sessionTimeoutMs;
    
    @Value("${spring.kafka.consumer.heartbeat-interval:15000}")
    private int heartbeatIntervalMs;
    
    @Value("${spring.kafka.properties.connections.max.idle.ms:1800000}")
    private int connectionsMaxIdleMs;
    
    @Value("${spring.kafka.security.protocol:SASL_SSL}")
    private String securityProtocol;
    
    @Value("${spring.kafka.properties.sasl.mechanism:AWS_MSK_IAM}")
    private String saslMechanism;
    
    @Value("${spring.kafka.properties.sasl.jaas.config:software.amazon.msk.auth.iam.IAMLoginModule required;}")
    private String saslJaasConfig;
    
    @Value("${spring.kafka.properties.sasl.client.callback.handler.class:software.amazon.msk.auth.iam.IAMClientCallbackHandler}")
    private String callbackHandler;
    

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        
        // MSK IAM Authentication using injected values
        props.put("security.protocol", securityProtocol);
        props.put("sasl.mechanism", saslMechanism);
        props.put("sasl.jaas.config", saslJaasConfig);
        props.put("sasl.client.callback.handler.class", callbackHandler);
        
        // Batch processing settings - use CDC batch size for consistent batching
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 5000);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        
        // Timeout settings - fix rebalance issues
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        props.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, connectionsMaxIdleMs);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        return new DefaultKafkaConsumerFactory<>(props);
    }

    

}