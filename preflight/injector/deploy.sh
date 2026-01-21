#!/bin/bash
#
# Deploy preflight-injector webhook to the cluster
#
# Usage: ./deploy.sh [namespace]
#
# Prerequisites:
# - kubectl configured with cluster access
# - Docker image built and pushed (or use ko for local dev)
#

set -euo pipefail

NAMESPACE="${1:-nvsentinel}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Deploying preflight-injector to namespace: ${NAMESPACE}"

# Step 1: Create namespace
echo "==> Creating namespace ${NAMESPACE}"
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

# Step 2: Apply RBAC
echo "==> Applying RBAC"
kubectl apply -f "${SCRIPT_DIR}/rbac.yaml"

# Step 3: Apply ConfigMap
echo "==> Applying ConfigMap"
kubectl apply -f "${SCRIPT_DIR}/configmap.yaml"

# Step 4: Apply NetworkPolicy
echo "==> Applying NetworkPolicy"
kubectl apply -f "${SCRIPT_DIR}/networkpolicy.yaml"

# Step 5: Apply Deployment and Service
echo "==> Applying Deployment and Service"
kubectl apply -f "${SCRIPT_DIR}/deployment.yaml"

# Step 6: Apply MutatingWebhookConfiguration (without caBundle yet)
echo "==> Applying MutatingWebhookConfiguration"
kubectl apply -f "${SCRIPT_DIR}/mutatingWebhook.yaml"

# Step 7: Generate certificates and patch webhook
echo "==> Generating certificates and patching webhook"
"${SCRIPT_DIR}/generate-certs.sh" "${NAMESPACE}"

# Step 8: Wait for deployment to be ready
echo "==> Waiting for deployment to be ready"
kubectl rollout status deployment/preflight-injector -n "${NAMESPACE}" --timeout=60s

echo ""
echo "==> Deployment complete!"
echo ""
echo "Test by creating a pod in a namespace other than kube-system/nvsentinel:"
echo ""
echo "  kubectl run test-pod --image=nginx -n default"
echo "  kubectl get pod test-pod -n default -o yaml | grep -A 20 initContainers"
echo ""
echo "To see webhook logs:"
echo "  kubectl logs -n ${NAMESPACE} -l app.kubernetes.io/name=preflight-injector -f"

