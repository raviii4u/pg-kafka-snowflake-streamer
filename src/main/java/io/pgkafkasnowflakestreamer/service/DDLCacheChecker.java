package io.pgkafkasnowflakestreamer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Set;

@Service
public class DDLCacheChecker {
    
    private static final Logger log = LoggerFactory.getLogger(DDLCacheChecker.class);
    
    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    
    public boolean isDDLCacheReady(String targetSchema) {
        try {
            Set<String> keys = redisTemplate.keys(targetSchema.toLowerCase() + ".*");
            if (keys == null || keys.isEmpty()) {
                log.debug("DDL cache not ready - no keys found for schema: {}", targetSchema);
                return false;
            }
            log.debug("DDL cache ready - found {} keys for schema: {}", keys.size(), targetSchema);
            return true;
        } catch (Exception e) {
            log.warn("Redis unavailable - treating DDL cache as not ready for schema {}: {}", targetSchema, e.getMessage());
            return false;
        }
    }
}