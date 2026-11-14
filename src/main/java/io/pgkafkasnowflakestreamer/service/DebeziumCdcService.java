package io.pgkafkasnowflakestreamer.service;

import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.config.Configuration;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.json.JsonConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.kafka.support.SendResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.springframework.cloud.fn.common.debezium.DebeziumProperties;

@Service
@ConditionalOnProperty(name = "cdc.producer.enabled", havingValue = "true", matchIfMissing = true)
public class DebeziumCdcService {
    
    private static final Logger log = LoggerFactory.getLogger(DebeziumCdcService.class);
    private final SnowflakeSink snowflakeSink;
    private final DebeziumProperties debeziumProperties;
    private final KafkaBatchService kafkaBatchService;
    private DebeziumEngine<RecordChangeEvent<SourceRecord>> debeziumEngine;
    private ExecutorService executor;
    private JsonConverter jsonConverter;
    private volatile boolean engineRunning = false;

    public DebeziumCdcService(SnowflakeSink snowflakeSink, DebeziumProperties debeziumProperties,
                             @Autowired(required = false) KafkaBatchService kafkaBatchService) {
        this.snowflakeSink = snowflakeSink;
        this.debeziumProperties = debeziumProperties;
        this.kafkaBatchService = kafkaBatchService;
    }
    
    @PostConstruct
    public void start() {
        try {
            if (debeziumProperties.getProperties() == null || debeziumProperties.getProperties().isEmpty()) {
                log.warn("Debezium properties not configured, skipping CDC service initialization");
                return;
            }
            
            log.info("Initializing Debezium CDC Service...");
            
            jsonConverter = new JsonConverter();
            Map<String, Object> converterConfig = new HashMap<>();
            converterConfig.put("schemas.enable", false);
            converterConfig.put("converter.type", "value");
            converterConfig.put("decimal.format", "NUMERIC");
            jsonConverter.configure(converterConfig, false);
            
            Properties props = new Properties();
            props.putAll(debeziumProperties.getProperties());
            props.put("converters", "datetime");
            props.put("datetime.type", "io.pgkafkasnowflakestreamer.converter.CustomDateTimeConverter");
            
            Configuration debeziumConfig = Configuration.from(props);
            
            debeziumEngine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                    .using(debeziumConfig.asProperties())
                    .notifying((records, committer) -> {
                        List<Future<SendResult<String, String>>> futures = new ArrayList<>();

                        for (RecordChangeEvent<SourceRecord> record : records) {
                            try {
                                SourceRecord sourceRecord = record.record();
                                String key = sourceRecord.key() != null ?
                                    new String(jsonConverter.fromConnectData(sourceRecord.topic(), sourceRecord.keySchema(), sourceRecord.key())) : null;

                                if (sourceRecord.value() == null) {
                                    committer.markProcessed(record);
                                    continue;
                                }

                                String changeEvent = new String(jsonConverter.fromConnectData(sourceRecord.topic(), sourceRecord.valueSchema(), sourceRecord.value()));
                                JsonNode event = io.pgkafkasnowflakestreamer.util.ChangeEventParser.parseEvent(changeEvent);
                                String tableName = io.pgkafkasnowflakestreamer.util.ChangeEventParser.getTableName(event);

                                if (tableName == null || tableName.contains("debezium_heartbeat")) {
                                    committer.markProcessed(record);
                                    continue;
                                }

                                futures.add(kafkaBatchService.send(tableName, changeEvent, key));

                            } catch (Exception e) {
                                log.error("Error processing record: {}", e.getMessage(), e);
                                throw new RuntimeException("Failed to process change event, stopping Debezium engine", e);
                            }
                        }

                        // Wait for all Kafka sends — single wait for entire batch
                        try {
                            for (Future<SendResult<String, String>> future : futures) {
                                future.get();
                            }
                            committer.markBatchFinished();
                        } catch (Exception e) {
                            log.error("Kafka send failed: {}", e.getMessage(), e);
                            throw new RuntimeException("Kafka batch send failed, stopping Debezium engine", e);
                        }
                    })
                    .using((success, message, error) -> {
                        if (error != null) {
                            log.error("Debezium engine stopped with error: {}", message, error);
                            engineRunning = false;
                            cleanup();
                        }
                    })
                    .build();
            
            engineRunning = true;
            executor = Executors.newSingleThreadExecutor();
            executor.execute(debeziumEngine);
            
            log.info("Debezium CDC Service started successfully");
        } catch (Exception e) {
            log.error("Failed to start Debezium CDC Service: {}", e.getMessage(), e);
            log.warn("Application will continue without CDC functionality");
        }
    }

    public boolean isEngineRunning() {
        return engineRunning;
    }

    @PreDestroy
    public void stop() {
        try {
            log.info("Graceful shutdown initiated - stopping Debezium engine");
            if (debeziumEngine != null) {
                debeziumEngine.close();
                log.info("Debezium engine stopped");
            }
            if (executor != null) {
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                        log.warn("Executor did not terminate within 30s, forcing shutdown");
                        executor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    executor.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        } catch (Exception e) {
            log.error("Error stopping Debezium engine: {}", e.getMessage(), e);
        }
        cleanup();
    }

    private void cleanup() {
        if (kafkaBatchService != null) {
            try {
                kafkaBatchService.flush();
                log.info("Kafka producer flushed");
            } catch (Exception e) {
                log.warn("Error flushing Kafka producer: {}", e.getMessage(), e);
            }
        }
    }
}
