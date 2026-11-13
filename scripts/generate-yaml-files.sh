#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
OUTPUT_DIR="$PROJECT_ROOT/k8s/generated"

# Read JSON from stdin
read -r JSON_DATA

# Parse JSON using jq or basic parsing
AGENT_NAME=$(echo "$JSON_DATA" | grep -o '"agentName":"[^"]*"' | cut -d'"' -f4)
ENVIRONMENT=$(echo "$JSON_DATA" | grep -o '"environment":"[^"]*"' | cut -d'"' -f4)

# Clear and create output directory
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

# Set ECR registry
if [ "$ENVIRONMENT" = "prod" ]; then
    ECR_REGISTRY="${AWS_ACCOUNT_ID_PROD}.dkr.ecr.us-east-1.amazonaws.com"
else
    ECR_REGISTRY="${AWS_ACCOUNT_ID_NONPROD}.dkr.ecr.us-east-1.amazonaws.com"
fi

# Extract all values from JSON
POSTGRES_HOST=$(echo "$JSON_DATA" | grep -o '"postgresHost":"[^"]*"' | cut -d'"' -f4)
POSTGRES_DATABASE=$(echo "$JSON_DATA" | grep -o '"postgresDatabase":"[^"]*"' | cut -d'"' -f4)
POSTGRES_USER=$(echo "$JSON_DATA" | grep -o '"postgresUser":"[^"]*"' | cut -d'"' -f4)
POSTGRES_PASSWORD=$(echo "$JSON_DATA" | grep -o '"postgresPassword":"[^"]*"' | cut -d'"' -f4)
SNOWFLAKE_URL=$(echo "$JSON_DATA" | grep -o '"snowflakeUrl":"[^"]*"' | cut -d'"' -f4)
SNOWFLAKE_DATABASE=$(echo "$JSON_DATA" | grep -o '"snowflakeDatabase":"[^"]*"' | cut -d'"' -f4)
SNOWFLAKE_SCHEMA=$(echo "$JSON_DATA" | grep -o '"snowflakeSchema":"[^"]*"' | cut -d'"' -f4)
SNOWFLAKE_USERNAME=$(echo "$JSON_DATA" | grep -o '"snowflakeUsername":"[^"]*"' | cut -d'"' -f4)
SNOWFLAKE_PASSWORD=$(echo "$JSON_DATA" | grep -o '"snowflakePassword":"[^"]*"' | cut -d'"' -f4)
SNOWFLAKE_ROLE=$(echo "$JSON_DATA" | grep -o '"snowflakeRole":"[^"]*"' | cut -d'"' -f4)
SNOWFLAKE_WAREHOUSE=$(echo "$JSON_DATA" | grep -o '"snowflakeWarehouse":"[^"]*"' | cut -d'"' -f4)
REDIS_PASSWORD=$(echo "$JSON_DATA" | grep -o '"redisPassword":"[^"]*"' | cut -d'"' -f4)
SCHEMA_INCLUDE_LIST=$(echo "$JSON_DATA" | grep -o '"schemaIncludeList":"[^"]*"' | cut -d'"' -f4)
REPLICATION_SLOT_NAME=$(echo "$JSON_DATA" | grep -o '"replicationSlotName":"[^"]*"' | cut -d'"' -f4)
PUBLICATION_NAME=$(echo "$JSON_DATA" | grep -o '"publicationName":"[^"]*"' | cut -d'"' -f4)
TABLE_EXCLUDE_LIST=$(echo "$JSON_DATA" | grep -o '"tableExcludeList":"[^"]*"' | cut -d'"' -f4 || echo "")
KAFKA_BOOTSTRAP_SERVERS=$(echo "$JSON_DATA" | grep -o '"kafkaBootstrapServers":"[^"]*"' | cut -d'"' -f4)
KAFKA_CONSUMER_CLIENT_ID=$(echo "$JSON_DATA" | grep -o '"kafkaConsumerClientId":"[^"]*"' | cut -d'"' -f4)
KAFKA_CONSUMER_GROUP_ID=$(echo "$JSON_DATA" | grep -o '"kafkaConsumerGroupId":"[^"]*"' | cut -d'"' -f4)
KAFKA_CONSUMER_TOPIC_PATTERN=$(echo "$JSON_DATA" | grep -o '"kafkaConsumerTopicPattern":"[^"]*"' | cut -d'"' -f4)
KAFKA_TOPIC_PREFIX=$(echo "$JSON_DATA" | grep -o '"kafkaTopicPrefix":"[^"]*"' | cut -d'"' -f4)
AWS_ACCESS_KEY_ID=$(echo "$JSON_DATA" | grep -o '"awsAccessKeyId":"[^"]*"' | cut -d'"' -f4)
AWS_SECRET_ACCESS_KEY=$(echo "$JSON_DATA" | grep -o '"awsSecretAccessKey":"[^"]*"' | cut -d'"' -f4)
CDC_BATCH_SIZE=$(echo "$JSON_DATA" | grep -o '"cdcBatchSize":[^,}]*' | cut -d':' -f2 | tr -d '"')
CDC_MODE=$(echo "$JSON_DATA" | grep -o '"cdcMode":"[^"]*"' | cut -d'"' -f4)
CDC_BATCH_TIMEOUT=$(echo "$JSON_DATA" | grep -o '"cdcBatchTimeout":[^,}]*' | cut -d':' -f2 | tr -d '"')

# Set environment variables for envsubst
export AGENT_NAME CONFIG_MAP_NAME="pg-kafka-snowflake-streamer-${AGENT_NAME}" SECRET_NAME="pg-kafka-snowflake-streamer-${AGENT_NAME}"
export NAMESPACE="cdc-${ENVIRONMENT}" ENVIRONMENT DEBEZIUM_COMMON_PROPS="debezium-common-props"
export PVC_CLAIM="redis-data-pvc" DOCKER_IMAGE="${ECR_REGISTRY}/cdc-${ENVIRONMENT}/pg-kafka-snowflake-streamer:latest"
export POSTGRES_HOST POSTGRES_DATABASE POSTGRES_USER POSTGRES_PASSWORD POSTGRES_PORT="5432"
export SNOWFLAKE_URL SNOWFLAKE_DATABASE SNOWFLAKE_SCHEMA SNOWFLAKE_USERNAME SNOWFLAKE_PASSWORD SNOWFLAKE_ROLE SNOWFLAKE_WAREHOUSE
export REDIS_PASSWORD SCHEMA_INCLUDE_LIST
export REPLICATION_SLOT_NAME PUBLICATION_NAME TABLE_EXCLUDE_LIST
export KAFKA_BOOTSTRAP_SERVERS KAFKA_CONSUMER_CLIENT_ID KAFKA_CONSUMER_GROUP_ID
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
# Set defaults for empty values
CDC_BATCH_SIZE=${CDC_BATCH_SIZE:-500}
CDC_MODE=${CDC_MODE:-REALTIME}
CDC_BATCH_TIMEOUT=${CDC_BATCH_TIMEOUT:-300000}

export CDC_BATCH_SIZE CDC_MODE CDC_BATCH_TIMEOUT

# Generate YAML files
for template in configmap-template.yaml secret-template.yaml deployment-template.yaml; do
    output_file=$(echo "$template" | sed "s/-template/-${AGENT_NAME}/")
    envsubst < "$PROJECT_ROOT/k8s/$template" > "$OUTPUT_DIR/$output_file"
    echo "Generated: $output_file"
done

echo ""
echo "Files generated in: $OUTPUT_DIR"
echo "To deploy: kubectl apply -f $OUTPUT_DIR/"