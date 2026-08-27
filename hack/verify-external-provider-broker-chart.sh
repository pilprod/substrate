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

if ! cmp -s "${TMP_DIR}/default.yaml" "${TMP_DIR}/explicit-disabled.yaml"; then
  echo "default chart output differs from explicit externalProviderBroker.enabled=false" >&2
  diff -u "${TMP_DIR}/default.yaml" "${TMP_DIR}/explicit-disabled.yaml" >&2 || true
  exit 1
fi

python3 - "${TMP_DIR}/default.yaml" "${TMP_DIR}/enabled.yaml" <<'PY'
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
PY

echo "External provider Broker chart topology is fail-closed and default-disabled."
