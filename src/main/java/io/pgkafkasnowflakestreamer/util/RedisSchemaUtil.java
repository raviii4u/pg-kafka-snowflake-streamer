package io.pgkafkasnowflakestreamer.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import java.util.Set;

@Component
public class RedisSchemaUtil {

    private static final Logger log = LoggerFactory.getLogger(RedisSchemaUtil.class);
    
    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    
    public boolean areSchemasAlreadyCloned(String sourceSchemas) {
        if (sourceSchemas == null || sourceSchemas.isEmpty() || redisTemplate == null) {
            return false;
        }
        try {
            String[] schemaList = sourceSchemas.split(",");
            for (String schema : schemaList) {
                String targetSchema = schema.trim().toLowerCase();
                String pattern = targetSchema + ".*";
                Set<String> keys = redisTemplate.keys(pattern);
                if (keys == null || keys.isEmpty()) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            log.warn("Redis unavailable - treating schemas as not cloned: {}", e.getMessage());
            return false;
        }
    }
}