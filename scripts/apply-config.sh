#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

ENVIRONMENT=${1}
AGENT_NAME=${2}
CONFIG_FILE="$SCRIPT_DIR/${AGENT_NAME}.env"
OUTPUT_DIR="$PROJECT_ROOT/k8s/generated"

if [ -z "$AGENT_NAME" ] || [ -z "$ENVIRONMENT" ]; then
    echo "Usage: ./apply-config.sh <environment> <agent-name>"
    exit 1
fi

if [ ! -f "$CONFIG_FILE" ]; then
    echo "Config file not found: $CONFIG_FILE"
    echo "Run: ./generate-config.sh $ENVIRONMENT $AGENT_NAME"
    exit 1
fi

mkdir -p "$OUTPUT_DIR"

echo "Generating YAML files for ${AGENT_NAME} using ${CONFIG_FILE}"

# Source the config file
set -a
source "$CONFIG_FILE"
set +a

# Generate YAML files
envsubst < "$PROJECT_ROOT/k8s/configmap-template.yaml" > "${OUTPUT_DIR}/${AGENT_NAME}-configmap.yaml"
envsubst < "$PROJECT_ROOT/k8s/secret-template.yaml" > "${OUTPUT_DIR}/${AGENT_NAME}-secret.yaml"
envsubst < "$PROJECT_ROOT/k8s/deployment-template.yaml" > "${OUTPUT_DIR}/${AGENT_NAME}-deployment.yaml"

echo "Generated YAML files in ${OUTPUT_DIR}/"
echo "To deploy: kubectl apply -f ${OUTPUT_DIR}/${AGENT_NAME}-*.yaml"