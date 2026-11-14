#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

ENVIRONMENT=${1:-dev}
AGENT_NAME=${2}
CONFIG_FILE="$SCRIPT_DIR/${AGENT_NAME}.env"

if [ -z "$AGENT_NAME" ]; then
    echo "Usage: ./generate-config.sh <environment> <agent-name>"
    exit 1
fi

echo "Creating config file for ${AGENT_NAME} (${ENVIRONMENT})"

# Set ECR registry
if [ "$ENVIRONMENT" = "prod" ]; then
    ECR_REGISTRY="${AWS_ACCOUNT_ID_PROD}.dkr.ecr.us-east-1.amazonaws.com"
else
    ECR_REGISTRY="${AWS_ACCOUNT_ID_NONPROD}.dkr.ecr.us-east-1.amazonaws.com"
fi

cat > "$CONFIG_FILE" << EOF
# Agent Configuration for ${AGENT_NAME}
AGENT_NAME=${AGENT_NAME}
CONFIG_MAP_NAME=pg-kafka-snowflake-streamer-${AGENT_NAME}
SECRET_NAME=pg-kafka-snowflake-streamer-${AGENT_NAME}
PVC_CLAIM=redis-data-pvc
NAMESPACE=cdc-${ENVIRONMENT}
ENVIRONMENT=${ENVIRONMENT}
DEBEZIUM_COMMON_PROPS=debezium-common-props
DOCKER_IMAGE=${ECR_REGISTRY}/cdc-${ENVIRONMENT}/pg-kafka-snowflake-streamer:latest
POSTGRES_PORT=5432

# Fill in these values:
POSTGRES_HOST=
POSTGRES_DATABASE=
POSTGRES_USER=
POSTGRES_PASSWORD=
SNOWFLAKE_SCHEMA=
SNOWFLAKE_PASSWORD=
REDIS_PASSWORD=
SCHEMA_INCLUDE_LIST=
REPLICATION_SLOT_NAME=
PUBLICATION_NAME=
TABLE_EXCLUDE_LIST=
KAFKA_BOOTSTRAP_SERVERS=
KAFKA_CONSUMER_CLIENT_ID=
KAFKA_CONSUMER_GROUP_ID=
KAFKA_CONSUMER_TOPIC_PATTERN=
KAFKA_TOPIC_PREFIX=
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
EOF

echo "Config template created: $CONFIG_FILE"
echo "Edit the file with your values, then run:"
echo "./apply-config.sh $ENVIRONMENT $AGENT_NAME"