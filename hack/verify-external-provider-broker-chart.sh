#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
CHART="${ROOT}/charts/substrate"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

if ! command -v helm >/dev/null 2>&1; then
  echo "helm not found in PATH" >&2
  exit 1
fi

helm lint --strict "${CHART}"
helm lint --strict "${CHART}" --set externalProviderBroker.enabled=true
helm lint --strict "${CHART}" \
  --set externalProviderBroker.enabled=true \
  --set externalProviderBroker.gateway.enabled=true \
  --set externalProviderBroker.gateway.hostname=api.ate-system.svc

helm template substrate "${CHART}" \
  --show-only templates/ate-api-server.yaml \
  > "${TMP_DIR}/default.yaml"
helm template substrate "${CHART}" \
  --show-only templates/ate-api-server.yaml \
  --set externalProviderBroker.enabled=false \
  > "${TMP_DIR}/explicit-disabled.yaml"
helm template substrate "${CHART}" \
  --show-only templates/ate-api-server.yaml \
  --set externalProviderBroker.enabled=true \
  > "${TMP_DIR}/enabled.yaml"
helm template substrate "${CHART}" --namespace ate-system \
  > "${TMP_DIR}/gateway-default.yaml"
helm template substrate "${CHART}" --namespace ate-system \
  --set externalProviderBroker.enabled=true \
  --set externalProviderBroker.gateway.enabled=true \
  --set externalProviderBroker.gateway.hostname=api.ate-system.svc \
  > "${TMP_DIR}/gateway-enabled.yaml"
helm template substrate "${CHART}" --namespace ate-system \
  --set externalProviderBroker.enabled=true \
  --set externalProviderBroker.gateway.enabled=true \
  --set externalProviderBroker.gateway.hostname=api.ate-system.svc \
  --set-string 'externalProviderBroker.gateway.addresses[0].type=IPAddress' \
  --set-string 'externalProviderBroker.gateway.addresses[0].value=203.0.113.11' \
  --set-string externalProviderBroker.gateway.infrastructure.parametersRef.group=agentgateway.dev \
  --set-string externalProviderBroker.gateway.infrastructure.parametersRef.kind=AgentgatewayParameters \
  --set-string externalProviderBroker.gateway.infrastructure.parametersRef.name=broker-gateway-params \
  > "${TMP_DIR}/gateway-configured.yaml"

if ! cmp -s "${TMP_DIR}/default.yaml" "${TMP_DIR}/explicit-disabled.yaml"; then
  echo "default chart output differs from explicit externalProviderBroker.enabled=false" >&2
  diff -u "${TMP_DIR}/default.yaml" "${TMP_DIR}/explicit-disabled.yaml" >&2 || true
  exit 1
fi

expect_failure() {
  local expected="$1"
  shift
  local output
  if output="$("$@" 2>&1)"; then
    echo "command unexpectedly succeeded: $*" >&2
    exit 1
  fi
  if [[ "${output}" != *"${expected}"* ]]; then
    echo "command failed without expected message ${expected}: ${output}" >&2
    exit 1
  fi
}

expect_failure \
  "externalProviderBroker.gateway.enabled requires externalProviderBroker.enabled=true" \
  helm template substrate "${CHART}" \
    --set externalProviderBroker.gateway.enabled=true \
    --set externalProviderBroker.gateway.hostname=api.default.svc
expect_failure \
  "externalProviderBroker.gateway.hostname must be an exact valid DNS name" \
  helm template substrate "${CHART}" \
    --set externalProviderBroker.enabled=true \
    --set externalProviderBroker.gateway.enabled=true \
    --set externalProviderBroker.gateway.hostname='*.example.com'
expect_failure \
  "additional properties 'typo' not allowed" \
  helm template substrate "${CHART}" \
    --set externalProviderBroker.gateway.typo=true
expect_failure \
  "'not-an-ip' is not valid ipv4" \
  helm template substrate "${CHART}" \
    --set externalProviderBroker.enabled=true \
    --set externalProviderBroker.gateway.enabled=true \
    --set externalProviderBroker.gateway.hostname=api.default.svc \
    --set-string 'externalProviderBroker.gateway.addresses[0].type=IPAddress' \
    --set-string 'externalProviderBroker.gateway.addresses[0].value=not-an-ip'
expect_failure \
  "externalProviderBroker.gateway.infrastructure.parametersRef.kind must be a valid Kubernetes kind" \
  helm template substrate "${CHART}" \
    --set externalProviderBroker.enabled=true \
    --set externalProviderBroker.gateway.enabled=true \
    --set externalProviderBroker.gateway.hostname=api.default.svc \
    --set-string externalProviderBroker.gateway.infrastructure.parametersRef.group=agentgateway.dev

python3 - \
  "${TMP_DIR}/default.yaml" \
  "${TMP_DIR}/enabled.yaml" \
  "${TMP_DIR}/gateway-default.yaml" \
  "${TMP_DIR}/gateway-enabled.yaml" \
  "${TMP_DIR}/gateway-configured.yaml" <<'PY'
import re
import sys


def documents(path):
    with open(path, encoding="utf-8") as rendered:
        return rendered.read().split("\n---\n")


def resource(docs, kind, name):
    matches = [
        doc
        for doc in docs
        if re.search(rf"(?m)^kind: {re.escape(kind)}$", doc)
        and re.search(rf"(?m)^  name: {re.escape(name)}$", doc)
    ]
    if len(matches) != 1:
        raise AssertionError(f"expected one {kind}/{name}, found {len(matches)}")
    return matches[0]


default_docs = documents(sys.argv[1])
enabled_docs = documents(sys.argv[2])
gateway_default_docs = documents(sys.argv[3])
gateway_enabled_docs = documents(sys.argv[4])
gateway_configured_docs = documents(sys.argv[5])
default_deployment = resource(default_docs, "Deployment", "ate-api-server")
enabled_deployment = resource(enabled_docs, "Deployment", "ate-api-server")
enabled_service = resource(enabled_docs, "Service", "api")

default_strategy = """  replicas: 2
  strategy:
    rollingUpdate:
      maxUnavailable: 0
      maxSurge: 1
"""
if default_strategy not in default_deployment:
    raise AssertionError("default ate-api-server topology is not replicas=2 RollingUpdate")
for unexpected in ("type: Recreate", "external-provider-broker", "provider-grpc"):
    if unexpected in default_deployment:
        raise AssertionError(f"default Deployment unexpectedly contains {unexpected!r}")

enabled_strategy = """  replicas: 1
  strategy:
    type: Recreate
"""
if enabled_strategy not in enabled_deployment:
    raise AssertionError("enabled Broker topology is not replicas=1 Recreate")
if "rollingUpdate:" in enabled_deployment:
    raise AssertionError("enabled Broker Deployment still permits a rolling overlap")
for required in (
    "--external-provider-broker-listen-addr=0.0.0.0:8443",
    "--external-provider-broker-server-cred-bundle=",
    "--external-provider-session-token-ttl=5m",
    "name: provider-grpc",
    "containerPort: 8443",
):
    if required not in enabled_deployment:
        raise AssertionError(f"enabled Broker Deployment lacks {required!r}")

for required in ("clusterIP: None", "name: provider-grpc", "port: 8443", "targetPort: provider-grpc"):
    if required not in enabled_service:
        raise AssertionError(f"enabled internal API Service lacks {required!r}")
for forbidden in ("type: LoadBalancer", "type: NodePort", "externalIPs:"):
    if forbidden in enabled_service:
        raise AssertionError(f"enabled API Service exposes forbidden field {forbidden!r}")

for kind in ("Gateway", "TLSRoute", "ReferenceGrant"):
    if any(re.search(rf"(?m)^kind: {kind}$", doc) for doc in gateway_default_docs):
        raise AssertionError(f"default chart unexpectedly rendered {kind}")

gateway = resource(gateway_enabled_docs, "Gateway", "external-provider-broker")
for required in (
    "gatewayClassName: agentgateway",
    'hostname: "api.ate-system.svc"',
    "port: 443",
    "protocol: TLS",
    "mode: Passthrough",
    "from: Same",
    "kind: TLSRoute",
):
    if required not in gateway:
        raise AssertionError(f"Broker Gateway lacks {required!r}")
for forbidden in (
    "certificateRefs:",
    "protocol: HTTPS",
    "kind: HTTPRoute",
    "addresses:",
    "infrastructure:",
):
    if forbidden in gateway:
        raise AssertionError(f"Broker Gateway contains forbidden field {forbidden!r}")

configured_gateway = resource(
    gateway_configured_docs, "Gateway", "external-provider-broker"
)
for required in (
    "addresses:\n  - type: IPAddress\n    value: 203.0.113.11",
    "infrastructure:\n    parametersRef:",
    'group: "agentgateway.dev"',
    'kind: "AgentgatewayParameters"',
    'name: "broker-gateway-params"',
):
    if required not in configured_gateway:
        raise AssertionError(f"configured Broker Gateway lacks {required!r}")

tls_route = resource(gateway_enabled_docs, "TLSRoute", "external-provider-broker")
for required in (
    '  - "api.ate-system.svc"',
    "sectionName: broker",
    'group: ""',
    "kind: Service",
    "name: api",
    "port: 8443",
):
    if required not in tls_route:
        raise AssertionError(f"Broker TLSRoute lacks {required!r}")
for forbidden in ("port: 443", "name: ate-api-server", "kind: HTTPRoute"):
    if forbidden in tls_route:
        raise AssertionError(f"Broker TLSRoute exposes forbidden backend {forbidden!r}")

if any(re.search(r"(?m)^kind: ReferenceGrant$", doc) for doc in gateway_enabled_docs):
    raise AssertionError("same-namespace Broker route rendered an unnecessary ReferenceGrant")

for kind in ("Deployment", "ConfigMap", "Service"):
    if any(
        re.search(rf"(?m)^kind: {kind}$", doc)
        and re.search(r"(?m)^  name: external-provider-broker$", doc)
        for doc in gateway_enabled_docs
    ):
        raise AssertionError(f"Substrate unexpectedly owns agentgateway {kind}")
PY

echo "External provider Broker and Gateway API adapter are fail-closed and default-disabled."
