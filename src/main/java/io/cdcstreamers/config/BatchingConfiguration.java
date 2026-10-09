package io.cdcstreamers.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "cdc")
public class BatchingConfiguration {
    
    private String mode = "realtime"; // realtime or batch
    private BatchProvider batchProvider = BatchProvider.REDIS;
    private int batchSize = 500;
    private int batchTimeoutSeconds = 30;
    
    public String getMode() { return mode; }
    public void setMode(String mode) { this.mode = mode; }
    
    public BatchProvider getBatchProvider() { return batchProvider; }
    public void setBatchProvider(BatchProvider batchProvider) { this.batchProvider = batchProvider; }
    
    public int getBatchSize() {
        return batchSize;
    }
    
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
    
    public int getBatchTimeoutSeconds() {
        return batchTimeoutSeconds;
    }
    
    public void setBatchTimeoutSeconds(int batchTimeoutSeconds) {
        this.batchTimeoutSeconds = batchTimeoutSeconds;
    }

}