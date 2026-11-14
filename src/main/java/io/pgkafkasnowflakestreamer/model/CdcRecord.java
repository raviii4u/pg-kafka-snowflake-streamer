package io.pgkafkasnowflakestreamer.model;

public class CdcRecord {
    public final String value;
    public final String key;
    
    public CdcRecord(String value, String key) {
        this.value = value;
        this.key = key;
    }
}