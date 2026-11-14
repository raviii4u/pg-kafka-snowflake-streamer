#!/bin/bash
set -e

ENVIRONMENT=${1:-dev}
GENERATED_DIR="../k8s/generated"
NAMESPACE="cdc-${ENVIRONMENT}"

if [ ! -d "$GENERATED_DIR" ]; then
    echo "Error: Generated directory $GENERATED_DIR not found"
    exit 1
fi

if [ -z "$(ls -A $GENERATED_DIR/*.yaml 2>/dev/null)" ]; then
    echo "Error: No YAML files found in $GENERATED_DIR"
    exit 1
fi

# Get cluster info from current kubectl config
CURRENT_CLUSTER=$(kubectl config view --minify -o jsonpath='{.clusters[0].name}')
SERVER_URL=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')

echo "Using current kubectl cluster: ${CURRENT_CLUSTER}"
echo "Server: ${SERVER_URL}"

echo "Deploying all generated YAML files..."

for yaml_file in $GENERATED_DIR/*.yaml; do
    echo "Applying: $yaml_file"
    kubectl apply -f "$yaml_file"
done

echo "All YAML files deployed!"
echo "Check status: kubectl get pods -n ${NAMESPACE}"