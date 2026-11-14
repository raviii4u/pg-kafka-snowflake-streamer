package io.pgkafkasnowflakestreamer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "pg.snapshot")
public class PgSnapshotProperties {

    private boolean enabled;
    private String targetSchemas;
    private Target target = new Target();
    private int batchSize = 1500000;
    private int threads = 4;
    private String tempDir = "/tmp/pg-snapshot";

    public int getThreads() { return threads; }
    public void setThreads(int threads) { this.threads = threads; }

    public static class Target {
        private String database;
        public String getDatabase() { return database; }
        public void setDatabase(String database) { this.database = database; }
    }

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public String getTargetSchemas() { return targetSchemas; }
    public void setTargetSchemas(String targetSchemas) { this.targetSchemas = targetSchemas; }
    public Target getTarget() { return target; }
    public void setTarget(Target target) { this.target = target; }
    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
    public String getTempDir() { return tempDir; }
    public void setTempDir(String tempDir) { this.tempDir = tempDir; }
}
