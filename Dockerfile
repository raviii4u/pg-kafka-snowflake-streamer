FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

# Copy the fat JAR built by bootJar
COPY build/libs/pg-kafka-snowflake-streamer-0.0.1-SNAPSHOT.jar app.jar

# Debezium's NIO internals require this flag
ENTRYPOINT ["java", "--add-opens=java.base/java.nio=ALL-UNNAMED", "-jar", "/app/app.jar"]

EXPOSE 8080
