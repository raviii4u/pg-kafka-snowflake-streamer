package io.cdcstreamers.generator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.zip.*;

@SpringBootApplication
@Controller
public class YamlGeneratorController {

    @GetMapping("/")
    public String index() {
        return "agent-generator";
    }

    @PostMapping("/generate-yaml")
    @ResponseBody
    public ResponseEntity<ByteArrayResource> generateYaml(@RequestBody Map<String, String> config) throws Exception {
        
        String agentName = config.get("agentName");
        String environment = config.get("environment");
        
        // Set ECR registry based on environment
        String ecrRegistry = "prod".equals(environment) 
            ? System.getenv().getOrDefault("ECR_REGISTRY_PROD", "YOUR_ACCOUNT_ID.dkr.ecr.YOUR_REGION.amazonaws.com")
            : System.getenv().getOrDefault("ECR_REGISTRY_NONPROD", "YOUR_ACCOUNT_ID.dkr.ecr.YOUR_REGION.amazonaws.com");
        
        // Build environment variables map
        Map<String, String> env = new HashMap<>();
        env.put("AGENT_NAME", agentName);
        env.put("CONFIG_MAP_NAME", "pg-kafka-snowflake-streamer-" + agentName);
        env.put("SECRET_NAME", "pg-kafka-snowflake-streamer-" + agentName);
        env.put("PVC_CLAIM", "redis-data-pvc");
        env.put("NAMESPACE", System.getenv().getOrDefault("NAMESPACE_PREFIX", "cdc") + "-" + environment);
        env.put("ENVIRONMENT", environment);
        env.put("DEBEZIUM_COMMON_PROPS", "debezium-common-props");
        env.put("DOCKER_IMAGE", ecrRegistry + "/" + System.getenv().getOrDefault("NAMESPACE_PREFIX", "cdc") + "-" + environment + "/pg-kafka-snowflake-streamer:latest");
        env.put("POSTGRES_PORT", "5432");
        
        // Add all form fields
        config.forEach((key, value) -> {
            String envKey = camelToSnakeCase(key).toUpperCase();
            env.put(envKey, value);
        });
        
        // Generate YAML files
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(baos)) {
            
            String[] templates = {"configmap-template.yaml", "secret-template.yaml", "deployment-template.yaml"};
            
            for (String template : templates) {
                String templateContent = readTemplate(template);
                String processedContent = substituteVariables(templateContent, env);
                String fileName = template.replace("-template", "-" + agentName);
                
                ZipEntry entry = new ZipEntry(fileName);
                zos.putNextEntry(entry);
                zos.write(processedContent.getBytes());
                zos.closeEntry();
            }
        }
        
        ByteArrayResource resource = new ByteArrayResource(baos.toByteArray());
        
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + agentName + "-k8s-manifests.zip")
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .body(resource);
    }
    
    private String readTemplate(String templateName) throws IOException {
        Path templatePath = Paths.get("k8s", templateName);
        return Files.readString(templatePath);
    }
    
    private String substituteVariables(String content, Map<String, String> variables) {
        String result = content;
        for (Map.Entry<String, String> entry : variables.entrySet()) {
            result = result.replace("${" + entry.getKey() + "}", entry.getValue());
            result = result.replace("${" + entry.getKey() + ":-}", entry.getValue());
        }
        return result;
    }
    
    private String camelToSnakeCase(String camelCase) {
        return camelCase.replaceAll("([a-z])([A-Z])", "$1_$2").toLowerCase();
    }

    public static void main(String[] args) {
        SpringApplication.run(YamlGeneratorController.class, args);
    }
}