package io.cdcstreamers.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import io.cdcstreamers.config.RedisConfig;
import io.cdcstreamers.util.RedisSchemaUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import io.cdcstreamers.model.CdcRecord;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import io.cdcstreamers.util.SentryUtil;
import io.sentry.SentryLevel;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;


@Service
@ConditionalOnExpression("'${cdc.mode:BATCH}'.equals('BATCH') and '${cdc.consumer.enabled:true}'.equals('true')")
public class KafkaConsumerService {
    
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerService.class);
    
    @Value("${kafka.topic-prefix}")
    private String topicPrefix;
    
    @Value("${kafka.consumer.topic-pattern}")
    private String consumerTopicPattern;
    
    @Value("${cdc.batch-timeout-ms:900000}") // Default 15 minutes
    private int batchTimeoutMs;
    
    @Value("${kafka.consumer.group-id:stream-group}")
    private String consumerGroupId;
    
    @Value("${kafka.consumer.client-id:cdc-consumer}")
    private String consumerClientId;
    
    @Value("${kafka.consumer.excluded-tables:}")
    private String excludedTables;
    
    @Value("${kafka.consumer.poll-timeout-seconds:30}")
    private int pollTimeoutSeconds;
    
    @Value("${schema.clone.enabled:false}")
    private boolean cloneEnabled;
    
    @Value("${pg.snapshot.enabled:false}")
    private boolean snapshotEnabled;
    
    @Value("${schema.clone.source.schemas:}")
    private String sourceSchemas;

    @Value("${pg.snapshot.target.schemas:}")
    private String snapshotTargetSchemas;

    @Autowired
    private RedisSchemaUtil redisSchemaUtil;

    @Autowired
    private RedisConfig redisConfig;

    private final SnowflakeSink snowflakeSink;
    private final ConsumerFactory<String, String> consumerFactory;
    private Consumer<String, String> consumer;
    private final Map<String, Integer> retryCount = new ConcurrentHashMap<>();
    private volatile boolean shutdownRequested = false;
    private volatile boolean batchInProgress = false;

    @Autowired(required = false)
    private DebeziumCdcService debeziumCdcService;
    
    public KafkaConsumerService(SnowflakeSink snowflakeSink, ConsumerFactory<String, String> consumerFactory) {
        this.snowflakeSink = snowflakeSink;
        this.consumerFactory = consumerFactory;
    }
    
    @PostConstruct
    public void initializeConsumer() {
        if (!redisConfig.isRedisAvailable()) {
            log.error("Redis is unavailable - Kafka consumer will not start. Debezium producer continues running.");
            shutdownRequested = true;
            return;
        }
        if (redisSchemaUtil.areSchemasAlreadyCloned(getActiveSchemas())) {
            startConsumer();
        } else {
            log.info("Kafka consumer initialization delayed - waiting for schema cloning to complete");
        }
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        if (shutdownRequested && !redisConfig.isRedisAvailable()) {
            SentryUtil.captureMessage("Kafka consumer not started - Redis unavailable. Manual restart required after Redis recovery.", SentryLevel.FATAL);
        }
    }

    private String getActiveSchemas() {
        return snapshotEnabled ? snapshotTargetSchemas : sourceSchemas;
    }

    private void startConsumer() {
        if (consumer == null) {
            consumer = consumerFactory.createConsumer(consumerGroupId, consumerClientId);
            String regexPattern = consumerTopicPattern.replace("*", ".*");
            consumer.subscribe(Pattern.compile(regexPattern));
            log.info("Kafka consumer started with pattern: {} (regex: {}) - Consumer instance: {}", consumerTopicPattern, regexPattern, consumer.hashCode());
        }
    }
    
    public void startConsumerAfterClone() {
        if (consumer == null) {
            startConsumer();
            log.info("Kafka consumer started after schema cloning completion");
        }
    }
    
    @PreDestroy
    public void closeConsumer() {
        log.info("Graceful shutdown initiated - waiting for current batch to complete");
        shutdownRequested = true;
        
        // Wait for current batch to complete (up to 30 seconds)
        int waitTime = 0;
        while (batchInProgress && waitTime < 30000) {
            try {
                Thread.sleep(1000);
                waitTime += 1000;
                log.debug("Waiting for batch completion... {}s", waitTime / 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        
        if (batchInProgress) {
            log.warn("Batch still in progress after 30s, forcing shutdown");
        } else {
            log.info("Batch completed, proceeding with shutdown");
        }
        
        if (consumer != null) {
            consumer.close();
            log.info("Consumer closed gracefully");
        }
    }
    
    @Scheduled(fixedDelayString = "#{${cdc.batch-timeout-ms:900000}}") // 15 minutes
    public void processScheduledBatch() {
        if (shutdownRequested) {
            log.info("Shutdown requested, skipping scheduled batch");
            return;
        }

        if (debeziumCdcService != null && !debeziumCdcService.isEngineRunning()) {
            log.error("Debezium engine is down, stopping consumer");
            shutdownRequested = true;
            if (consumer != null) {
                consumer.close();
                consumer = null;
            }
            SentryUtil.captureMessage("Consumer stopped because Debezium engine is down. Manual restart required.", SentryLevel.FATAL);
            return;
        }
        
        if ((cloneEnabled || snapshotEnabled) && !redisSchemaUtil.areSchemasAlreadyCloned(getActiveSchemas())) {
            log.info("Schema cloning not completed yet, consumer not started");
            return;
        }
        
        // Start consumer if not already started
        if (consumer == null && !shutdownRequested) {
            startConsumer();
        }
        
        batchInProgress = true;
        long batchStartTime = System.currentTimeMillis();
        log.info("Starting scheduled batch - Consumer instance: {}", consumer != null ? consumer.hashCode() : "NULL");
        List<CdcRecord> allRecords = new ArrayList<>();
        
        try {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(pollTimeoutSeconds));
            log.debug("Polled {} records from Kafka", records.count());
            
            for (ConsumerRecord<String, String> record : records) {
                // Filter out excluded tables
                if (!isTableExcluded(record)) {
                    allRecords.add(new CdcRecord(record.value(), record.key()));
                }
            }
            
            if (allRecords.isEmpty()) {
                log.info("***********No records found in scheduled batch************** - Consumer: {}", consumer.hashCode());
                
                long nextRunTime = System.currentTimeMillis() + batchTimeoutMs;
                java.time.ZonedDateTime nextRun = java.time.Instant.ofEpochMilli(nextRunTime).atZone(java.time.ZoneId.of("America/Chicago"));
                log.info("Next scheduled batch will run at: {}", nextRun);
                return;
            }
            
            log.info("[KAFKA PULL] Pulled {} total records from Kafka - processing in single transaction", allRecords.size());
            
            // Log Kafka pull summary
            log.info("[KAFKA BATCH] Starting batch processing: {} records from Kafka topics", allRecords.size());
            
            // Process ALL records in single Snowflake transaction
            snowflakeSink.processBatch(allRecords);
            
        } catch (Exception e) {
            log.error("Batch processing failed - Kafka offsets NOT committed. Stopping consumer: {}", e.getMessage(), e);
            SentryUtil.captureException(e);
            
            // Stop only the consumer, keep Debezium running
            shutdownRequested = true;
            if (consumer != null) {
                consumer.close();
                consumer = null;
            }
            
            log.error("Consumer stopped due to failure. Manual restart required.");
            SentryUtil.captureMessage("CDC Consumer stopped due to failure. Manual restart required.", SentryLevel.FATAL);
            return;
        } finally {
            batchInProgress = false;
        }
        
        // Only commit Kafka offsets if Snowflake transaction succeeded
        log.info("[KAFKA COMMIT] About to commit Kafka offsets for {} records", allRecords.size());
        consumer.commitSync();
        
        long totalTime = System.currentTimeMillis() - batchStartTime;
        log.info("[BATCH COMPLETE] Successfully processed {} records in {}ms - offsets committed", allRecords.size(), totalTime);
        
        // Log next scheduled run time
        long nextRunTime = System.currentTimeMillis() + batchTimeoutMs;
        java.time.ZonedDateTime nextRun = java.time.Instant.ofEpochMilli(nextRunTime).atZone(java.time.ZoneId.of("America/Chicago"));
        log.info("Next scheduled batch will run at: {}", nextRun);
    }
    
    private boolean isTableExcluded(ConsumerRecord<String, String> record) {
        if (excludedTables == null || excludedTables.trim().isEmpty()) {
            return false;
        }
        
        try {
            // Extract table name from topic name (format: stream-db2-schema-table)
            String topic = record.topic();
            String[] topicParts = topic.split("-");
            if (topicParts.length >= 4) {
                String schema = topicParts[2];
                String table = topicParts[3];
                String tableName = schema + "." + table;
                
                // Check if table is in excluded list
                String[] excluded = excludedTables.split(",");
                for (String excludedPattern : excluded) {
                    String pattern = excludedPattern.trim();
                    
                    // Check for wildcard pattern (schema.*)
                    if (pattern.endsWith(".*")) {
                        String schemaPattern = pattern.substring(0, pattern.length() - 2);
                        if (schema.equalsIgnoreCase(schemaPattern)) {
                            log.debug("Excluding table {} (schema wildcard: {})", tableName, pattern);
                            return true;
                        }
                    }
                    // Check for exact match (schema.table)
                    else if (tableName.equalsIgnoreCase(pattern)) {
                        log.debug("Excluding table: {}", tableName);
                        return true;
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Failed to parse table name from topic {}: {}", record.topic(), e.getMessage(), e);
        }
        
        return false;
    }
    

}