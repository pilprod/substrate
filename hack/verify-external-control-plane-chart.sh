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
VALUES="${CHART}/examples/external-control-plane-only-cloud-sql.values.yaml"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

if ! command -v helm >/dev/null 2>&1; then
  echo "helm not found in PATH" >&2
  exit 1
fi

helm lint --strict "${CHART}"
helm lint --strict "${CHART}" --values "${VALUES}"
helm template substrate "${CHART}" > "${TMP_DIR}/default.yaml"
helm template substrate "${CHART}" --set profile=standard > "${TMP_DIR}/explicit-standard.yaml"
helm template substrate "${CHART}" --values "${VALUES}" > "${TMP_DIR}/external.yaml"

if ! cmp -s "${TMP_DIR}/default.yaml" "${TMP_DIR}/explicit-standard.yaml"; then
  echo "default chart output differs from explicit profile=standard" >&2
  diff -u "${TMP_DIR}/default.yaml" "${TMP_DIR}/explicit-standard.yaml" >&2 || true
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
  "externalControlPlane.postgres.existingSecret.name is required" \
  helm template substrate "${CHART}" --set profile=external-control-plane-only
expect_failure \
  "postgres.connectionString is forbidden" \
  helm template substrate "${CHART}" --set profile=external-control-plane-only \
    --set externalControlPlane.postgres.existingSecret.name=substrate-db \
    --set postgres.connectionString=postgresql://forbidden
expect_failure \
  "profile must be one of" \
  helm template substrate "${CHART}" --set profile=unknown

python3 - "${TMP_DIR}/external.yaml" <<'PY'
import re
import sys


def documents(path):
    with open(path, encoding="utf-8") as rendered:
        return [doc for doc in rendered.read().split("\n---\n") if doc.strip()]


def resources(docs, kind):
    return [doc for doc in docs if re.search(rf"(?m)^kind: {re.escape(kind)}$", doc)]


def resource(docs, kind, name):
    matches = [
        doc
        for doc in resources(docs, kind)
        if re.search(rf"(?m)^  name: {re.escape(name)}$", doc)
    ]
    if len(matches) != 1:
        raise AssertionError(f"expected one {kind}/{name}, found {len(matches)}")
    return matches[0]


docs = documents(sys.argv[1])
rendered = "\n---\n".join(docs)

for kind in (
    "DaemonSet",
    "StatefulSet",
    "PersistentVolumeClaim",
    "Job",
    "WorkerPool",
    "SandboxConfig",
    "ValidatingAdmissionPolicy",
    "ValidatingAdmissionPolicyBinding",
    "Secret",
):
    if resources(docs, kind):
        raise AssertionError(f"external control-plane profile rendered forbidden kind {kind}")

for forbidden in (
    "privileged: true",
    "hostPath:",
    "ATE_API_POSTGRES_CONNECTION_STRING",
    "--postgres-connection-string=@env",
    "kind: ConfigMap\nmetadata:\n  name: ate-api-server-envvars",
    "name: atelet",
    "name: atenet-router",
    "name: atenet-egress",
    "app: rustfs",
    "image: rustfs/",
    "app: postgres",
    "image: postgres:",
):
    if forbidden in rendered:
        raise AssertionError(f"external control-plane profile contains forbidden text {forbidden!r}")

deployments = resources(docs, "Deployment")
deployment_names = sorted(
    re.search(r"(?m)^  name: ([^\n]+)$", doc).group(1) for doc in deployments
)
if deployment_names != ["ate-api-server", "ate-controller", "podcertificate-controller"]:
    raise AssertionError(f"unexpected Deployments: {deployment_names}")

api = resource(docs, "Deployment", "ate-api-server")
for required in (
    "  replicas: 1\n  strategy:\n    type: Recreate",
    "--external-provider-broker-listen-addr=0.0.0.0:8443",
    "--external-provider-broker-server-cred-bundle=",
    "--postgres-connection-string-file=/run/secrets/substrate/postgres/connection-string",
    "secretName: substrate-cloud-sql",
    "key: connection-string",
    "path: connection-string",
    "defaultMode: 0400",
    "readOnlyRootFilesystem: true",
    "allowPrivilegeEscalation: false",
):
    if required not in api:
        raise AssertionError(f"ate-api-server Deployment lacks {required!r}")

controller = resource(docs, "Deployment", "ate-controller")
for required in (
    "replicas: 1",
    "--controller-mode=external-templates-only",
    "--ateapi-conn-spec=dns:///api.",
    "readOnlyRootFilesystem: true",
    "allowPrivilegeEscalation: false",
):
    if required not in controller:
        raise AssertionError(f"external template controller Deployment lacks {required!r}")
for forbidden in (
    "--otel-metric-export-interval",
    "--otel-traces-sampler",
):
    if forbidden in controller:
        raise AssertionError(f"external template controller enables local-worker option {forbidden!r}")

controller_role = resource(docs, "ClusterRole", "ate-controller")
for required in (
    'resources: ["actortemplates"]',
    'verbs: ["get", "list", "watch"]',
    'resources: ["actortemplates/status"]',
    'verbs: ["get", "patch", "update"]',
):
    if required not in controller_role:
        raise AssertionError(f"external template controller RBAC lacks {required!r}")
for forbidden in ("workerpools", "deployments", "networkpolicies", "secrets", "pods"):
    if forbidden in controller_role:
        raise AssertionError(f"external template controller RBAC contains forbidden resource {forbidden!r}")

service = resource(docs, "Service", "api")
for required in ("clusterIP: None", "name: provider-grpc", "port: 8443", "targetPort: provider-grpc"):
    if required not in service:
        raise AssertionError(f"internal API Service lacks {required!r}")
for forbidden in ("type: LoadBalancer", "type: NodePort", "externalIPs:"):
    if forbidden in service:
        raise AssertionError(f"internal API Service exposes forbidden field {forbidden!r}")

default_deny = resource(docs, "NetworkPolicy", "ate-api-server-default-deny")
if "- Ingress\n  - Egress" not in default_deny or "app: ate-api-server" not in default_deny:
    raise AssertionError("default-deny NetworkPolicy does not isolate ate-api-server")

controller_default_deny = resource(docs, "NetworkPolicy", "ate-controller-default-deny")
if "- Ingress\n  - Egress" not in controller_default_deny or "app: ate-controller" not in controller_default_deny:
    raise AssertionError("default-deny NetworkPolicy does not isolate ate-controller")

allow_ingress = resource(docs, "NetworkPolicy", "ate-api-server-allow-ingress")
for required in ("port: 443", "port: 8443", "kagent-system", "client-gateway-system"):
    if required not in allow_ingress:
        raise AssertionError(f"ingress NetworkPolicy lacks {required!r}")

allow_egress = resource(docs, "NetworkPolicy", "ate-api-server-allow-egress")
for required in ("port: 53", "port: 443", "port: 5432", "192.0.2.10/32", "192.0.2.20/32"):
    if required not in allow_egress:
        raise AssertionError(f"egress NetworkPolicy lacks {required!r}")

controller_egress = resource(docs, "NetworkPolicy", "ate-controller-allow-egress")
for required in ("port: 53", "port: 443", "192.0.2.10/32", "app: ate-api-server"):
    if required not in controller_egress:
        raise AssertionError(f"controller egress NetworkPolicy lacks {required!r}")
for forbidden in ("port: 5432", "192.0.2.20/32"):
    if forbidden in controller_egress:
        raise AssertionError(f"controller egress NetworkPolicy permits database destination {forbidden!r}")
PY

echo "External control-plane-only profile is isolated, secret-backed, and data-plane free."
