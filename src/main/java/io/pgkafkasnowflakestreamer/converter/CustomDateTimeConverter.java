package io.pgkafkasnowflakestreamer.converter;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class CustomDateTimeConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    @Override
    public void configure(Properties props) {
        // No configuration needed
    }

    @Override
    public void converterFor(RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
        String typeName = column.typeName().toLowerCase();
        
        if ("date".equals(typeName)) {
            registration.register(SchemaBuilder.string().optional(), value -> {
                if (value == null) return null;
                if (value instanceof Number) {
                    long epochDays = ((Number) value).longValue();
                    // Handle epoch (0) as null for DATE columns
                    if (epochDays == 0) {
                        return null;
                    }
                    return LocalDate.ofEpochDay(epochDays).format(DATE_FORMATTER);
                }
                return value.toString();
            });
        } else if ("time".equals(typeName) || "timetz".equals(typeName)) {
            registration.register(SchemaBuilder.string().optional(), value -> {
                if (value == null) return null;
                if (value instanceof Number) {
                    long nanos = ((Number) value).longValue();
                    return LocalTime.ofNanoOfDay(nanos * 1000).format(TIME_FORMATTER);
                }
                return value.toString();
            });
        } else if (typeName.startsWith("timestamp")) {
            registration.register(SchemaBuilder.string().optional(), value -> {
                if (value == null) return null;
                if (value instanceof Number) {
                    long epochMicros = ((Number) value).longValue();
                    // Handle Unix epoch (0) as null to avoid Snowflake timezone issues
                    if (epochMicros == 0) {
                        return null;
                    }
                    return Instant.ofEpochSecond(epochMicros / 1_000_000, (epochMicros % 1_000_000) * 1000)
                            .atOffset(ZoneOffset.UTC)
                            .format(DATETIME_FORMATTER);
                }
                return value.toString();
            });
        }
    }
}