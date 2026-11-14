#!/bin/bash

ENVIRONMENT=${1:-dev}
NAMESPACE="cdc-${ENVIRONMENT}"

echo "Checking CDC agent deployments in ${NAMESPACE}..."

kubectl get deployments -n ${NAMESPACE} -l app=pg-kafka-snowflake-streamer
kubectl get pods -n ${NAMESPACE} -l app=pg-kafka-snowflake-streamer