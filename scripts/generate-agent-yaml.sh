#!/bin/bash
set -e

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

ENVIRONMENT=${1:-dev}
AGENT_NAME=${2}
OUTPUT_DIR="$PROJECT_ROOT/k8s/generated"

if [ -z "$AGENT_NAME" ]; then
    echo "Usage: ./generate-agent-yaml.sh <environment> <agent-name>"
    echo "Available agents:"
    cat "$SCRIPT_DIR/agents.txt" | sed 's/^/  /'
    exit 1
fi

# Check if agent exists
if ! grep -q "^${AGENT_NAME}$" "$SCRIPT_DIR/agents.txt"; then
    echo "Error: Agent '${AGENT_NAME}' not found in agents.txt"
    exit 1
fi

mkdir -p "$OUTPUT_DIR"

echo "Generating YAML files for agent: ${AGENT_NAME} (${ENVIRONMENT})"

# Set common variables
export AGENT_NAME="$AGENT_NAME"
export CONFIG_MAP_NAME="pg-kafka-snowflake-streamer-${AGENT_NAME}"
export SECRET_NAME="pg-kafka-snowflake-streamer-${AGENT_NAME}"
export PVC_CLAIM="redis-data-pvc"
export NAMESPACE="cdc-${ENVIRONMENT}"
export ENVIRONMENT="$ENVIRONMENT"
export DEBEZIUM_COMMON_PROPS="debezium-common-props"

# Set environment-specific ECR registry
if [ "$ENVIRONMENT" = "prod" ]; then
    ECR_REGISTRY="${AWS_ACCOUNT_ID_PROD}.dkr.ecr.us-east-1.amazonaws.com"
else
    ECR_REGISTRY="${AWS_ACCOUNT_ID_NONPROD}.dkr.ecr.us-east-1.amazonaws.com"
fi
export DOCKER_IMAGE="${ECR_REGISTRY}/cdc-${ENVIRONMENT}/pg-kafka-snowflake-streamer:latest"

# Prompt for agent-specific config
echo "Enter configuration for ${AGENT_NAME}:"
read -p "POSTGRES_HOST: " POSTGRES_HOST
export POSTGRES_HOST
read -p "POSTGRES_DATABASE: " POSTGRES_DATABASE
export POSTGRES_DATABASE
read -p "POSTGRES_USER: " POSTGRES_USER
export POSTGRES_USER
read -p "POSTGRES_PASSWORD: " POSTGRES_PASSWORD
export POSTGRES_PASSWORD
read -p "SNOWFLAKE_SCHEMA: " SNOWFLAKE_SCHEMA
export SNOWFLAKE_SCHEMA
read -p "SNOWFLAKE_PASSWORD: " SNOWFLAKE_PASSWORD
export SNOWFLAKE_PASSWORD
read -p "REDIS_PASSWORD: " REDIS_PASSWORD
export REDIS_PASSWORD
read -p "SCHEMA_INCLUDE_LIST: " SCHEMA_INCLUDE_LIST
export SCHEMA_INCLUDE_LIST
read -p "REPLICATION_SLOT_NAME: " REPLICATION_SLOT_NAME
export REPLICATION_SLOT_NAME
read -p "PUBLICATION_NAME: " PUBLICATION_NAME
export PUBLICATION_NAME
read -p "TABLE_EXCLUDE_LIST (optional): " TABLE_EXCLUDE_LIST
export TABLE_EXCLUDE_LIST
read -p "KAFKA_BOOTSTRAP_SERVERS: " KAFKA_BOOTSTRAP_SERVERS
export KAFKA_BOOTSTRAP_SERVERS
read -p "KAFKA_CONSUMER_CLIENT_ID: " KAFKA_CONSUMER_CLIENT_ID
export KAFKA_CONSUMER_CLIENT_ID
read -p "KAFKA_CONSUMER_GROUP_ID: " KAFKA_CONSUMER_GROUP_ID
export KAFKA_CONSUMER_GROUP_ID
read -p "KAFKA_CONSUMER_TOPIC_PATTERN: " KAFKA_CONSUMER_TOPIC_PATTERN
export KAFKA_CONSUMER_TOPIC_PATTERN
read -p "KAFKA_TOPIC_PREFIX: " KAFKA_TOPIC_PREFIX
export KAFKA_TOPIC_PREFIX
read -p "AWS_ACCESS_KEY_ID: " AWS_ACCESS_KEY_ID
export AWS_ACCESS_KEY_ID
read -p "AWS_SECRET_ACCESS_KEY: " AWS_SECRET_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY

# Set defaults
export POSTGRES_PORT="5432"

# Generate YAML files
envsubst < "$PROJECT_ROOT/k8s/configmap-template.yaml" > "${OUTPUT_DIR}/${AGENT_NAME}-configmap.yaml"
envsubst < "$PROJECT_ROOT/k8s/secret-template.yaml" > "${OUTPUT_DIR}/${AGENT_NAME}-secret.yaml"
envsubst < "$PROJECT_ROOT/k8s/deployment-template.yaml" > "${OUTPUT_DIR}/${AGENT_NAME}-deployment.yaml"

echo "Generated YAML files for ${AGENT_NAME} in ${OUTPUT_DIR}/"
echo "To deploy: kubectl apply -f ${OUTPUT_DIR}/${AGENT_NAME}-*.yaml"