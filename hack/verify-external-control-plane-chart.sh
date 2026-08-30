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
helm template substrate "${CHART}" --namespace ate-system --values "${VALUES}" > "${TMP_DIR}/external.yaml"
helm template substrate "${CHART}" --set rbac.create=false > "${TMP_DIR}/standard-no-rbac.yaml"
helm template substrate "${CHART}" --namespace ate-system --values "${VALUES}" \
  --set rbac.create=false > "${TMP_DIR}/external-no-rbac.yaml"
agentgateway_image="ghcr.io/kagent-dev/substrate/agentgateway@sha256:068028a256bd63c91fd6e85a471269c014747297b0ffa785feaef6967eb0c429"
ateapi_digest="sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
atecontroller_digest="sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
atenet_digest="sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
helm template substrate "${CHART}" --namespace ate-system --values "${VALUES}" \
  --set-string image.registry=ghcr.io/example/substrate \
  --set-string image.digests.ateapi="${ateapi_digest}" \
  --set-string image.digests.atecontroller="${atecontroller_digest}" \
  --set-string image.digests.atenet="${atenet_digest}" \
  > "${TMP_DIR}/external-digests.yaml"

if ! cmp -s "${TMP_DIR}/default.yaml" "${TMP_DIR}/explicit-standard.yaml"; then
  echo "default chart output differs from explicit profile=standard" >&2
  diff -u "${TMP_DIR}/default.yaml" "${TMP_DIR}/explicit-standard.yaml" >&2 || true
  exit 1
fi

for rendered_profile in default external; do
  if ! grep -Fq "image: ${agentgateway_image}" "${TMP_DIR}/${rendered_profile}.yaml"; then
    echo "${rendered_profile} profile does not use the immutable agentgateway image ${agentgateway_image}" >&2
    exit 1
  fi
done

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
  "externalControlPlane.tls.apiServer.existingSecret.name is required" \
  helm template substrate "${CHART}" --set profile=external-control-plane-only \
    --set externalControlPlane.postgres.existingSecret.name=substrate-db
expect_failure \
  "externalControlPlane.tls.apiServer.existingSecret.name must be a valid Kubernetes Secret name" \
  helm template substrate "${CHART}" --set profile=external-control-plane-only \
    --set externalControlPlane.postgres.existingSecret.name=substrate-db \
    --set externalControlPlane.tls.apiServer.existingSecret.name=NOT_VALID
expect_failure \
  "externalControlPlane.tls.apiServer.existingSecret.credentialBundleKey must be a valid Kubernetes Secret data key" \
  helm template substrate "${CHART}" --set profile=external-control-plane-only \
    --set externalControlPlane.postgres.existingSecret.name=substrate-db \
    --set externalControlPlane.tls.apiServer.existingSecret.name=substrate-api-tls \
    --set externalControlPlane.tls.apiServer.existingSecret.credentialBundleKey=bad/key
expect_failure \
  "credentialBundleKey and clientCAKey must differ" \
  helm template substrate "${CHART}" --set profile=external-control-plane-only \
    --set externalControlPlane.postgres.existingSecret.name=substrate-db \
    --set externalControlPlane.tls.apiServer.existingSecret.name=substrate-api-tls \
    --set externalControlPlane.tls.apiServer.existingSecret.clientCAKey=server-credential-bundle.pem
expect_failure \
  "externalControlPlane.tls.controller.existingSecret.name is required" \
  helm template substrate "${CHART}" --set profile=external-control-plane-only \
    --set externalControlPlane.postgres.existingSecret.name=substrate-db \
    --set externalControlPlane.tls.apiServer.existingSecret.name=substrate-api-tls
expect_failure \
  "externalControlPlane.tls.controller.existingSecret.serverCAKey must be a valid Kubernetes Secret data key" \
  helm template substrate "${CHART}" --set profile=external-control-plane-only \
    --set externalControlPlane.postgres.existingSecret.name=substrate-db \
    --set externalControlPlane.tls.apiServer.existingSecret.name=substrate-api-tls \
    --set externalControlPlane.tls.controller.existingSecret.name=substrate-controller-tls \
    --set externalControlPlane.tls.controller.existingSecret.serverCAKey=bad/key
expect_failure \
  "credential keys must not project a private-key bundle into both Pods" \
  helm template substrate "${CHART}" --set profile=external-control-plane-only \
    --set externalControlPlane.postgres.existingSecret.name=substrate-db \
    --set externalControlPlane.tls.apiServer.existingSecret.name=shared-tls \
    --set externalControlPlane.tls.controller.existingSecret.name=shared-tls \
    --set externalControlPlane.tls.controller.existingSecret.credentialBundleKey=server-credential-bundle.pem
expect_failure \
  "externalControlPlane.tls.egressGateway.serverName must be a valid DNS name" \
  helm template substrate "${CHART}" --namespace ate-system --values "${VALUES}" \
    --set externalControlPlane.tls.egressGateway.serverName=NOT_VALID
expect_failure \
  "externalControlPlane.tls.egressGateway existing Secret credentialBundleKey and serverCAKey must differ" \
  helm template substrate "${CHART}" --namespace ate-system --values "${VALUES}" \
    --set externalControlPlane.tls.egressGateway.existingSecret.serverCAKey=server-credential-bundle.pem
expect_failure \
  "externalControlPlane.tls.egressAuthorizer.principal must be an exact SPIFFE URI" \
  helm template substrate "${CHART}" --namespace ate-system --values "${VALUES}" \
    --set externalControlPlane.tls.egressAuthorizer.principal=https://not-spiffe.example
expect_failure \
  "externalControlPlane.tls.egressAuthorizer existing Secret credentialBundleKey and serverCAKey must differ" \
  helm template substrate "${CHART}" --namespace ate-system --values "${VALUES}" \
    --set externalControlPlane.tls.egressAuthorizer.existingSecret.serverCAKey=client-credential-bundle.pem
expect_failure \
  "externalProviderBroker.gateway.gatewayClassName must be a valid GatewayClass name" \
  helm template substrate "${CHART}" --namespace ate-system --values "${VALUES}" \
    --set externalProviderBroker.gateway.gatewayClassName=NOT_VALID
expect_failure \
  "externalProviderBroker.gateway.hostname must be an exact valid DNS name" \
  helm template substrate "${CHART}" --namespace ate-system --values "${VALUES}" \
    --set externalProviderBroker.gateway.hostname='*.example.com'
expect_failure \
  "additional properties 'typo' not allowed" \
  helm template substrate "${CHART}" --namespace ate-system --values "${VALUES}" \
    --set externalProviderBroker.gateway.typo=true
expect_failure \
  "profile must be one of" \
  helm template substrate "${CHART}" --set profile=unknown
expect_failure \
  "image.digests.ateapi must be a sha256 OCI digest" \
  helm template substrate "${CHART}" --namespace ate-system --values "${VALUES}" \
    --set-string image.digests.ateapi=latest
expect_failure \
  "image.digests.atenet must be a sha256 OCI digest" \
  helm template substrate "${CHART}" --namespace ate-system --values "${VALUES}" \
    --set-string image.digests.atenet=latest

grep -Fq \
  "image: ghcr.io/example/substrate/ateapi@${ateapi_digest}" \
  "${TMP_DIR}/external-digests.yaml"
grep -Fq \
  "image: ghcr.io/example/substrate/atecontroller@${atecontroller_digest}" \
  "${TMP_DIR}/external-digests.yaml"
grep -Fq \
  "image: ghcr.io/example/substrate/atenet@${atenet_digest}" \
  "${TMP_DIR}/external-digests.yaml"

for rendered in standard-no-rbac external-no-rbac; do
  if grep -Eq '^kind: (ClusterRole|Role|ClusterRoleBinding|RoleBinding)$' \
    "${TMP_DIR}/${rendered}.yaml"; then
    echo "${rendered} rendered chart-managed RBAC while rbac.create=false" >&2
    exit 1
  fi
  if ! grep -Fq 'kind: ServiceAccount' "${TMP_DIR}/${rendered}.yaml"; then
    echo "${rendered} omitted workload ServiceAccounts while rbac.create=false" >&2
    exit 1
  fi
  if ! grep -Fq 'kind: Deployment' "${TMP_DIR}/${rendered}.yaml"; then
    echo "${rendered} omitted workloads while rbac.create=false" >&2
    exit 1
  fi
done

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


def container(deployment, name):
    matches = re.findall(
        rf"(?ms)^      - name: {re.escape(name)}\n(.*?)(?=^      - name: |^      volumes:)",
        deployment,
    )
    if len(matches) != 1:
        raise AssertionError(f"expected one container {name}, found {len(matches)}")
    return matches[0]


def assert_restricted_pod(deployment, name, container_count):
    if not re.search(
        r"(?m)^      securityContext:\n(?:^        .*\n)*?^        runAsNonRoot: true$",
        deployment,
    ):
        raise AssertionError(f"{name} Deployment lacks pod-level runAsNonRoot: true")
    if not re.search(
        r"(?m)^        seccompProfile:\n^          type: RuntimeDefault$", deployment
    ):
        raise AssertionError(f"{name} Deployment lacks pod-level RuntimeDefault seccomp")
    if not re.search(r"(?m)^        fsGroup: 65532$", deployment):
        raise AssertionError(f"{name} Deployment lacks pod-level fsGroup 65532")
    for required in (
        "allowPrivilegeEscalation: false",
        'drop: ["ALL"]',
    ):
        if deployment.count(required) != container_count:
            raise AssertionError(
                f"{name} Deployment expected {container_count} container occurrences "
                f"of {required!r}, found {deployment.count(required)}"
            )


def assert_ko_identity(deployment, deployment_name, container_name):
    block = container(deployment, container_name)
    for required in ("runAsUser: 65532", "runAsGroup: 65532"):
        if required not in block:
            raise AssertionError(
                f"{deployment_name}/{container_name} lacks numeric identity {required!r}"
            )


def assert_group_readable_secrets(deployment, name, expected_count):
    if "defaultMode: 0400" in deployment:
        raise AssertionError(f"{name} Deployment retains root-only Secret mode 0400")
    count = deployment.count("defaultMode: 0440")
    if count != expected_count:
        raise AssertionError(
            f"{name} Deployment expected {expected_count} group-readable Secret modes, found {count}"
        )


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
    "app: rustfs",
    "image: rustfs/",
    "app: postgres",
    "image: postgres:",
    "apiVersion: certificates.k8s.io/v1beta1",
    "podCertificate:",
    "clusterTrustBundle:",
    "podcertificaterequests",
    "clustertrustbundles",
    "podcertificate-controller",
    "/run/servicedns.podcert.ate.dev",
    "/run/podidentity.podcert.ate.dev",
):
    if forbidden in rendered:
        raise AssertionError(f"external control-plane profile contains forbidden text {forbidden!r}")

deployments = resources(docs, "Deployment")
deployment_names = sorted(
    re.search(r"(?m)^  name: ([^\n]+)$", doc).group(1) for doc in deployments
)
if deployment_names != ["ate-api-server", "ate-controller", "atenet-egress"]:
    raise AssertionError(f"unexpected Deployments: {deployment_names}")

gateway = resource(docs, "Gateway", "external-provider-broker")
for required in (
    "apiVersion: gateway.networking.k8s.io/v1",
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
        raise AssertionError(f"Broker Gateway exposes forbidden field {forbidden!r}")

tls_route = resource(docs, "TLSRoute", "external-provider-broker")
for required in (
    "apiVersion: gateway.networking.k8s.io/v1",
    '  - "api.ate-system.svc"',
    "kind: Gateway",
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

if resources(docs, "ReferenceGrant"):
    raise AssertionError("same-namespace Broker route unexpectedly rendered a ReferenceGrant")

api = resource(docs, "Deployment", "ate-api-server")
assert_restricted_pod(api, "ate-api-server", 1)
assert_ko_identity(api, "ate-api-server", "ate-api-server")
assert_group_readable_secrets(api, "ate-api-server", 5)
for required in (
    "  replicas: 1\n  strategy:\n    type: Recreate",
    "--external-provider-broker-listen-addr=0.0.0.0:8443",
    "--grpc-server-cred-bundle=/run/secrets/substrate/tls/ate-api/server-credential-bundle.pem",
    "--external-provider-broker-server-cred-bundle=/run/secrets/substrate/tls/ate-api/server-credential-bundle.pem",
    "--pod-identity-ca-certs=/run/secrets/substrate/tls/ate-api/client-ca.pem",
    "--postgres-connection-string-file=/run/secrets/substrate/postgres/connection-string",
    "--egress-gateway-address=atenet-egress.ate-system.svc:443",
    "--egress-gateway-server-name=atenet-egress.ate-system.svc",
    "--egress-gateway-trust-bundle=/run/secrets/substrate/tls/egress-gateway/server-ca.pem",
    "--egress-gateway-ateapi-principal=spiffe://cluster.local/ns/ate-system/sa/atenet-egress",
    "secretName: substrate-cloud-sql",
    "secretName: substrate-ate-api-tls",
    "key: connection-string",
    "path: connection-string",
    "key: server-credential-bundle.pem",
    "path: server-credential-bundle.pem",
    "key: client-ca.pem",
    "path: client-ca.pem",
    "secretName: substrate-atenet-egress-server-tls",
    "name: egress-gateway-trust",
    "path: server-ca.pem",
    "defaultMode: 0440",
    "readOnlyRootFilesystem: true",
    "allowPrivilegeEscalation: false",
):
    if required not in api:
        raise AssertionError(f"ate-api-server Deployment lacks {required!r}")

controller = resource(docs, "Deployment", "ate-controller")
assert_restricted_pod(controller, "ate-controller", 1)
assert_ko_identity(controller, "ate-controller", "ate-controller")
assert_group_readable_secrets(controller, "ate-controller", 1)
for required in (
    "replicas: 1",
    "--controller-mode=external-templates-only",
    "--ateapi-conn-spec=dns:///api.",
    "--ateapi-ca-file=/run/secrets/substrate/tls/ate-controller/server-ca.pem",
    "--ateapi-server-name=api.ate-system.svc",
    "--ateapi-client-cert=/run/secrets/substrate/tls/ate-controller/client-credential-bundle.pem",
    "secretName: substrate-ate-controller-tls",
    "key: client-credential-bundle.pem",
    "path: client-credential-bundle.pem",
    "key: server-ca.pem",
    "path: server-ca.pem",
    "defaultMode: 0440",
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

egress = resource(docs, "Deployment", "atenet-egress")
assert_restricted_pod(egress, "atenet-egress", 2)
assert_ko_identity(egress, "atenet-egress", "ext-proc")
assert_group_readable_secrets(egress, "atenet-egress", 3)
agentgateway = container(egress, "agentgateway")
if "runAsUser:" in agentgateway or "runAsGroup:" in agentgateway:
    raise AssertionError("external atenet-egress overrides the pinned agentgateway image UID/GID")
for required in (
    "image: ghcr.io/kagent-dev/substrate/agentgateway@sha256:068028a256bd63c91fd6e85a471269c014747297b0ffa785feaef6967eb0c429",
    "--ateapi-ca-file=/run/secrets/substrate/tls/egress-authorizer/server-ca.pem",
    "--ateapi-server-name=api.ate-system.svc",
    "--ateapi-client-cert=/run/secrets/substrate/tls/egress-authorizer/client-credential-bundle.pem",
    "secretName: substrate-atenet-egress-server-tls",
    "secretName: substrate-atenet-egress-client-tls",
    "key: server-credential-bundle.pem",
    "key: client-credential-bundle.pem",
    "key: server-ca.pem",
    "defaultMode: 0440",
    "allowPrivilegeEscalation: false",
    "mountPath: /var/run/atenet",
    "emptyDir: {}",
):
    if required not in egress:
        raise AssertionError(f"external atenet-egress Deployment lacks {required!r}")
for forbidden in (
    "/run/servicedns.podcert.ate.dev",
    "/run/podidentity.podcert.ate.dev",
    "podCertificate:",
    "clusterTrustBundle:",
):
    if forbidden in egress:
        raise AssertionError(f"external atenet-egress Deployment contains forbidden text {forbidden!r}")

egress_config = resource(docs, "ConfigMap", "atenet-egress-agentgateway-config")
for required in (
    "cert: /run/secrets/substrate/tls/egress-gateway/server-credential-bundle.pem",
    "key: /run/secrets/substrate/tls/egress-gateway/server-credential-bundle.pem",
    "root: /run/actor-id-ca-certs/ca.crt",
    "failureMode: failClosed",
):
    if required not in egress_config:
        raise AssertionError(f"external atenet-egress ConfigMap lacks {required!r}")

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

egress_default_deny = resource(docs, "NetworkPolicy", "atenet-egress-default-deny")
if "- Ingress\n  - Egress" not in egress_default_deny or "app: atenet-egress" not in egress_default_deny:
    raise AssertionError("default-deny NetworkPolicy does not isolate atenet-egress")

allow_ingress = resource(docs, "NetworkPolicy", "ate-api-server-allow-ingress")
for required in (
    "port: 443",
    "port: 8443",
    "kagent-system",
    "app.kubernetes.io/name: kagent",
    "app.kubernetes.io/instance: kagent",
    "app.kubernetes.io/component: controller",
    "app: atenet-egress",
    "gateway.networking.k8s.io/gateway-name: external-provider-broker",
):
    if required not in allow_ingress:
        raise AssertionError(f"ingress NetworkPolicy lacks {required!r}")
for forbidden in (
    "client-gateway-system",
    "app.kubernetes.io/name: client-gateway",
    "app.kubernetes.io/name: kagent-controller",
):
    if forbidden in allow_ingress:
        raise AssertionError(f"ingress NetworkPolicy contains obsolete peer {forbidden!r}")

allow_egress = resource(docs, "NetworkPolicy", "ate-api-server-allow-egress")
for required in ("port: 53", "port: 443", "port: 8443", "port: 5432", "192.0.2.10/32", "192.0.2.20/32", "app: atenet-egress"):
    if required not in allow_egress:
        raise AssertionError(f"egress NetworkPolicy lacks {required!r}")

controller_egress = resource(docs, "NetworkPolicy", "ate-controller-allow-egress")
for required in ("port: 53", "port: 443", "192.0.2.10/32", "app: ate-api-server"):
    if required not in controller_egress:
        raise AssertionError(f"controller egress NetworkPolicy lacks {required!r}")
for forbidden in ("port: 5432", "192.0.2.20/32"):
    if forbidden in controller_egress:
        raise AssertionError(f"controller egress NetworkPolicy permits database destination {forbidden!r}")

egress_allow_ingress = resource(docs, "NetworkPolicy", "atenet-egress-allow-ingress")
for required in ("app: atenet-egress", "app: ate-api-server", "port: 8443"):
    if required not in egress_allow_ingress:
        raise AssertionError(f"atenet ingress NetworkPolicy lacks {required!r}")

egress_allow_egress = resource(docs, "NetworkPolicy", "atenet-egress-allow-egress")
for required in ("app: atenet-egress", "app: ate-api-server", "port: 53", "port: 443", "192.0.2.30/32"):
    if required not in egress_allow_egress:
        raise AssertionError(f"atenet egress NetworkPolicy lacks {required!r}")
PY

echo "External control-plane-only profile is beta-certificate-free, secret-backed, isolated, and external-worker-only."
