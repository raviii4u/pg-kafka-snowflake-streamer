package io.pgkafkasnowflakestreamer.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CompressionUtil {
    
    private static final Logger log = LoggerFactory.getLogger(CompressionUtil.class);
    private static final String COMPRESSED_PREFIX = "GZIP:";
    private static final int MIN_COMPRESSION_SIZE = 1024; // Only compress if >1KB
    
    public static String compress(String data) {
        if (data == null || data.length() < MIN_COMPRESSION_SIZE) {
            return data;
        }
        
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             GZIPOutputStream gzos = new GZIPOutputStream(baos)) {
            
            gzos.write(data.getBytes(StandardCharsets.UTF_8));
            gzos.finish();
            
            String compressed = COMPRESSED_PREFIX + Base64.getEncoder().encodeToString(baos.toByteArray());
            
            // Only return compressed if it's actually smaller
            if (compressed.length() < data.length()) {
                log.debug("Compressed {} bytes to {} bytes ({}% reduction)", 
                         data.length(), compressed.length(), 
                         100 - (compressed.length() * 100 / data.length()));
                return compressed;
            }
            
        } catch (Exception e) {
            log.warn("Compression failed, using original data: {}", e.getMessage());
        }
        
        return data;
    }
    
    public static String decompress(String data) {
        if (data == null || !data.startsWith(COMPRESSED_PREFIX)) {
            return data;
        }
        
        try {
            String base64Data = data.substring(COMPRESSED_PREFIX.length());
            byte[] bytes = Base64.getDecoder().decode(base64Data);
            
            try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                 GZIPInputStream gzis = new GZIPInputStream(bais)) {
                
                return new String(gzis.readAllBytes(), StandardCharsets.UTF_8);
            }
            
        } catch (Exception e) {
            log.error("Decompression failed for data starting with: {}, using as-is: {}", 
                     data.substring(0, Math.min(50, data.length())), e.getMessage());
            return data;
        }
    }
}