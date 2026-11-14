package io.pgkafkasnowflakestreamer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.support.SendResult;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.Collections;

@Service
public class KafkaBatchService {
    
    private static final Logger log = LoggerFactory.getLogger(KafkaBatchService.class);
    
    @Value("${kafka.topic-prefix}")
    private String topicPrefix;

    @Value("${kafka.topic-replication-factor:3}")
    private short topicReplicationFactor;
    
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaAdmin kafkaAdmin;
    private final Set<String> createdTopics = ConcurrentHashMap.newKeySet();
    
    @Autowired
    private DDLCacheChecker ddlCacheChecker;
    
    @Autowired
    private io.pgkafkasnowflakestreamer.config.SnowflakeConfiguration snowflakeConfig;
    
    public KafkaBatchService(KafkaTemplate<String, String> kafkaTemplate, KafkaAdmin kafkaAdmin) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaAdmin = kafkaAdmin;
    }
    
    public Future<SendResult<String, String>> send(String tableName, String changeEvent, String key) throws Exception {
        String topicName = topicPrefix + tableName.replace(".", "-");
        checkIfNotExistCreate(topicName);
        log.debug("Sending event to Kafka topic {}", topicName);
        return kafkaTemplate.send(topicName, key, changeEvent);
    }
    
    public void flush() {
        kafkaTemplate.flush();
    }
    
    private void checkIfNotExistCreate(String topicName) {
        if (!createdTopics.contains(topicName)) {
            try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
                adminClient.describeTopics(Collections.singleton(topicName)).all().get();
                createdTopics.add(topicName);
                log.debug("Topic already exists: {}", topicName);
            } catch (Exception ex) {
                log.debug("Topic does not exist, creating: {} - reason: {}", topicName, ex.getMessage(), ex);
                NewTopic newTopic = new NewTopic(topicName, 1, topicReplicationFactor);
                kafkaAdmin.createOrModifyTopics(newTopic);
                createdTopics.add(topicName);
                log.debug("Created topic: {}", topicName);
            }
        }
    }
}
