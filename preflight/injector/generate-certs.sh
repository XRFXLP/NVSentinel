#!/bin/bash
#
# Generate TLS certificates for the preflight-injector webhook
# This script creates a self-signed CA and server certificate.
#
# Usage: ./generate-certs.sh [namespace]
#
# The script will:
# 1. Generate a CA certificate
# 2. Generate a server certificate signed by the CA
# 3. Create a Kubernetes secret with the certificates
# 4. Patch the MutatingWebhookConfiguration with the CA bundle
#

set -euo pipefail

NAMESPACE="${1:-nvsentinel}"
SERVICE_NAME="preflight-injector"
SECRET_NAME="preflight-webhook-cert"
WEBHOOK_NAME="preflight-injector"

CERT_DIR=$(mktemp -d)
trap "rm -rf ${CERT_DIR}" EXIT

echo "==> Generating certificates in ${CERT_DIR}"

# Generate CA private key
openssl genrsa -out "${CERT_DIR}/ca.key" 2048

# Generate CA certificate
openssl req -x509 -new -nodes \
    -key "${CERT_DIR}/ca.key" \
    -sha256 -days 365 \
    -out "${CERT_DIR}/ca.crt" \
    -subj "/CN=preflight-injector-ca"

# Generate server private key
openssl genrsa -out "${CERT_DIR}/tls.key" 2048

# Create CSR config
cat > "${CERT_DIR}/csr.conf" <<EOF
[req]
default_bits = 2048
prompt = no
default_md = sha256
req_extensions = req_ext
distinguished_name = dn

[dn]
CN = ${SERVICE_NAME}.${NAMESPACE}.svc

[req_ext]
subjectAltName = @alt_names

[alt_names]
DNS.1 = ${SERVICE_NAME}
DNS.2 = ${SERVICE_NAME}.${NAMESPACE}
DNS.3 = ${SERVICE_NAME}.${NAMESPACE}.svc
DNS.4 = ${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local
EOF

# Generate CSR
openssl req -new \
    -key "${CERT_DIR}/tls.key" \
    -out "${CERT_DIR}/tls.csr" \
    -config "${CERT_DIR}/csr.conf"

# Create certificate extension config
cat > "${CERT_DIR}/cert.conf" <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = ${SERVICE_NAME}
DNS.2 = ${SERVICE_NAME}.${NAMESPACE}
DNS.3 = ${SERVICE_NAME}.${NAMESPACE}.svc
DNS.4 = ${SERVICE_NAME}.${NAMESPACE}.svc.cluster.local
EOF

# Sign the certificate with CA
openssl x509 -req \
    -in "${CERT_DIR}/tls.csr" \
    -CA "${CERT_DIR}/ca.crt" \
    -CAkey "${CERT_DIR}/ca.key" \
    -CAcreateserial \
    -out "${CERT_DIR}/tls.crt" \
    -days 365 \
    -sha256 \
    -extfile "${CERT_DIR}/cert.conf"

echo "==> Creating namespace ${NAMESPACE} (if not exists)"
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

echo "==> Creating/updating secret ${SECRET_NAME}"
kubectl create secret tls "${SECRET_NAME}" \
    --cert="${CERT_DIR}/tls.crt" \
    --key="${CERT_DIR}/tls.key" \
    --namespace="${NAMESPACE}" \
    --dry-run=client -o yaml | kubectl apply -f -

echo "==> Patching MutatingWebhookConfiguration with CA bundle"
CA_BUNDLE=$(base64 -w0 < "${CERT_DIR}/ca.crt")
kubectl patch mutatingwebhookconfiguration "${WEBHOOK_NAME}" \
    --type='json' \
    -p="[{\"op\": \"replace\", \"path\": \"/webhooks/0/clientConfig/caBundle\", \"value\": \"${CA_BUNDLE}\"}]"

echo "==> Done! Certificates created and webhook patched."
echo ""
echo "Verify with:"
echo "  kubectl get secret ${SECRET_NAME} -n ${NAMESPACE}"
echo "  kubectl get mutatingwebhookconfiguration ${WEBHOOK_NAME} -o yaml | grep caBundle"

