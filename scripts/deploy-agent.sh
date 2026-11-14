#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
OUTPUT_DIR="$PROJECT_ROOT/k8s/generated"

if [ $# -ne 2 ]; then
    echo "Usage: ./deploy-agent.sh <environment> <agent-name>"
    echo "This will prompt for config, generate YAML files, and deploy to Kubernetes"
    exit 1
fi

ENVIRONMENT=$1
AGENT_NAME=$2

# Set ECR registry
if [ "$ENVIRONMENT" = "prod" ]; then
    ECR_REGISTRY="${AWS_ACCOUNT_ID_PROD}.dkr.ecr.us-east-1.amazonaws.com"
else
    ECR_REGISTRY="${AWS_ACCOUNT_ID_NONPROD}.dkr.ecr.us-east-1.amazonaws.com"
fi

echo "Deploying CDC Agent: ${AGENT_NAME} to ${ENVIRONMENT}"
echo "=================================================="

# Prompt for configuration
echo "Enter configuration for ${AGENT_NAME}:"
read -p "POSTGRES_HOST: " POSTGRES_HOST
read -p "POSTGRES_DATABASE: " POSTGRES_DATABASE
read -p "POSTGRES_USER: " POSTGRES_USER
read -s -p "POSTGRES_PASSWORD: " POSTGRES_PASSWORD
echo
read -p "SNOWFLAKE_SCHEMA: " SNOWFLAKE_SCHEMA
read -s -p "SNOWFLAKE_PASSWORD: " SNOWFLAKE_PASSWORD
echo
read -s -p "REDIS_PASSWORD: " REDIS_PASSWORD
echo
read -p "SCHEMA_INCLUDE_LIST: " SCHEMA_INCLUDE_LIST
read -p "REPLICATION_SLOT_NAME: " REPLICATION_SLOT_NAME
read -p "PUBLICATION_NAME: " PUBLICATION_NAME
read -p "TABLE_EXCLUDE_LIST (optional): " TABLE_EXCLUDE_LIST
read -p "KAFKA_BOOTSTRAP_SERVERS: " KAFKA_BOOTSTRAP_SERVERS
read -p "KAFKA_CONSUMER_CLIENT_ID: " KAFKA_CONSUMER_CLIENT_ID
read -p "KAFKA_CONSUMER_GROUP_ID: " KAFKA_CONSUMER_GROUP_ID
read -p "KAFKA_CONSUMER_TOPIC_PATTERN: " KAFKA_CONSUMER_TOPIC_PATTERN
read -p "KAFKA_TOPIC_PREFIX: " KAFKA_TOPIC_PREFIX
read -p "AWS_ACCESS_KEY_ID: " AWS_ACCESS_KEY_ID
read -s -p "AWS_SECRET_ACCESS_KEY: " AWS_SECRET_ACCESS_KEY
echo

# Clear and create output directory
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

# Set environment variables
export AGENT_NAME CONFIG_MAP_NAME="pg-kafka-snowflake-streamer-${AGENT_NAME}" SECRET_NAME="pg-kafka-snowflake-streamer-${AGENT_NAME}"
export NAMESPACE="cdc-${ENVIRONMENT}" ENVIRONMENT DEBEZIUM_COMMON_PROPS="debezium-common-props"
export PVC_CLAIM="redis-data-pvc" DOCKER_IMAGE="${ECR_REGISTRY}/cdc-${ENVIRONMENT}/pg-kafka-snowflake-streamer:latest"
export POSTGRES_HOST POSTGRES_DATABASE POSTGRES_USER POSTGRES_PASSWORD POSTGRES_PORT="5432"
export SNOWFLAKE_SCHEMA SNOWFLAKE_PASSWORD REDIS_PASSWORD SCHEMA_INCLUDE_LIST
export REPLICATION_SLOT_NAME PUBLICATION_NAME TABLE_EXCLUDE_LIST
export KAFKA_BOOTSTRAP_SERVERS KAFKA_CONSUMER_CLIENT_ID KAFKA_CONSUMER_GROUP_ID
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY

echo ""
echo "Generating YAML files..."

# Generate YAML files
for template in configmap-template.yaml secret-template.yaml deployment-template.yaml; do
    output_file=$(echo "$template" | sed "s/-template/-${AGENT_NAME}/")
    envsubst < "$PROJECT_ROOT/k8s/$template" > "$OUTPUT_DIR/$output_file"
    echo "Generated: $output_file"
done

echo ""
echo "Deploying to Kubernetes..."

# Deploy to Kubernetes
kubectl apply -f "$OUTPUT_DIR/${AGENT_NAME}-configmap.yaml"
kubectl apply -f "$OUTPUT_DIR/${AGENT_NAME}-secret.yaml"
kubectl apply -f "$OUTPUT_DIR/${AGENT_NAME}-deployment.yaml"

echo ""
echo "Deployment completed!"
echo "Check status: kubectl get pods -n cdc-${ENVIRONMENT} -l agent=${AGENT_NAME}"
echo "View logs: kubectl logs -n cdc-${ENVIRONMENT} -l agent=${AGENT_NAME} -f"