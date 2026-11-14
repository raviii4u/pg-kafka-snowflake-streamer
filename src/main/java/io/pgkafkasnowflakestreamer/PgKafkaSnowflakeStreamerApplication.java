package io.pgkafkasnowflakestreamer;


import io.pgkafkasnowflakestreamer.config.KafkaConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties({org.springframework.cloud.fn.common.debezium.DebeziumProperties.class, KafkaConfiguration.class})
public class PgKafkaSnowflakeStreamerApplication {

    public static void main(String[] args) {
        SpringApplication.run(PgKafkaSnowflakeStreamerApplication.class, args);
    }

}
