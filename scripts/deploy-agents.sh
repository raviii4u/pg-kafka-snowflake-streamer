#!/bin/bash
set -e

ENVIRONMENT=${1:-dev}
IMAGE_TAG=${2:-latest}

# Map environment to project names
case $ENVIRONMENT in
  "dev")
    PROJECT_NAME="cdc-dev"
    ACCOUNT="${AWS_ACCOUNT_ID_NONPROD}"
    ;;
  "stage")
    PROJECT_NAME="cdc-stg" 
    ACCOUNT="${AWS_ACCOUNT_ID_NONPROD}"
    ;;
  "prod")
    PROJECT_NAME="cdc-prd"
    ACCOUNT="${AWS_ACCOUNT_ID_PROD}"
    ;;
esac

NAMESPACE="${PROJECT_NAME}"
IMAGE="${ACCOUNT}.dkr.ecr.us-east-1.amazonaws.com/${PROJECT_NAME}/pg-kafka-snowflake-streamer:${IMAGE_TAG}"

echo "Restarting CDC agents in ${NAMESPACE} with image ${IMAGE}"

# Get all CDC deployments
DEPLOYMENTS=$(kubectl get deployments -n ${NAMESPACE} -l app=pg-kafka-snowflake-streamer -o name 2>/dev/null || echo "")

if [ -z "$DEPLOYMENTS" ]; then
  # Try alternative label selector
  DEPLOYMENTS=$(kubectl get deployments -n ${NAMESPACE} -o name 2>/dev/null | grep cdc || echo "")
fi

if [ -z "$DEPLOYMENTS" ]; then
    echo "No CDC deployments found in ${NAMESPACE}"
    echo "Use ./scripts/create-local-agent.sh to create new agents"
    exit 1
fi

echo "Patching deployments with new image..."
for deployment in $DEPLOYMENTS; do
    kubectl patch $deployment -n ${NAMESPACE} -p '{"spec":{"template":{"spec":{"containers":[{"name":"pg-kafka-snowflake-streamer","image":"'${IMAGE}'"}]}}}}'
    echo "Patched: $deployment"
done

echo "Waiting for rollouts to complete..."
for deployment in $DEPLOYMENTS; do
    kubectl rollout status $deployment -n ${NAMESPACE} --timeout=300s
done

echo "All CDC agents restarted successfully!"