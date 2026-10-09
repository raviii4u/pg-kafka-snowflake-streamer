package io.cdcstreamers.controller;

import io.cdcstreamers.service.DebeziumCdcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import jakarta.annotation.PreDestroy;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@RestController
public class HealthController {

    private static final Logger log = LoggerFactory.getLogger(HealthController.class);
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    @Autowired(required = false)
    private DebeziumCdcService debeziumCdcService;

    @Value("${debezium.properties.database.hostname:}")
    private String postgresHost;

    @Value("${debezium.properties.database.port:5432}")
    private String postgresPort;

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> status = new HashMap<>();

        if (!shuttingDown.get() && debeziumCdcService != null && !debeziumCdcService.isEngineRunning()) {
            status.put("status", "DOWN");
            status.put("reason", "Debezium engine is not running");
            return ResponseEntity.status(503).body(status);
        }

        status.put("status", "UP");
        status.put("timestamp", System.currentTimeMillis());
        return ResponseEntity.ok(status);
    }

    @GetMapping("/ready")
    public ResponseEntity<Map<String, Object>> ready() {
        Map<String, Object> status = new HashMap<>();
        status.put("status", "READY");
        return ResponseEntity.ok(status);
    }

    @GetMapping("/health/cdc")
    public ResponseEntity<Map<String, Object>> cdcHealth() {
        Map<String, Object> health = new HashMap<>();

        if (shuttingDown.get()) {
            health.put("status", "SHUTTING_DOWN");
            return ResponseEntity.ok(health);
        }

        if (debeziumCdcService != null && !debeziumCdcService.isEngineRunning()) {
            health.put("status", "DOWN");
            health.put("debezium", "engine stopped");
            return ResponseEntity.status(503).body(health);
        }
        if (debeziumCdcService == null) {
            health.put("debezium", "producer disabled");
        }

        health.put("status", "UP");

        if (postgresHost != null && !postgresHost.isEmpty()) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(postgresHost, Integer.parseInt(postgresPort)), 5000);
                health.put("postgres", "reachable");
            } catch (Exception e) {
                health.put("status", "DOWN");
                health.put("postgres", e.getMessage());
                log.info("Health check: {}", health);
                return ResponseEntity.status(503).body(health);
            }
        } else {
            health.put("postgres", "not configured");
        }

        return ResponseEntity.ok(health);
    }

    @PreDestroy
    public void onShutdown() {
        shuttingDown.set(true);
        log.info("Application shutting down - health checks will return OK");
    }
}