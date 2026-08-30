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
WORKFLOW="${ROOT}/.github/workflows/gke-preview-release.yaml"
GENERATOR="${ROOT}/hack/generate-gke-preview-manifest.sh"
OVERWRITE_GUARD="${ROOT}/hack/refuse-gke-preview-overwrite.sh"
CHART_VALUES="${ROOT}/charts/substrate/values.yaml"

require_literal() {
  local file="$1"
  local literal="$2"
  if ! grep -Fq -- "${literal}" "${file}"; then
    printf '%s is missing required preview-release contract: %s\n' "${file}" "${literal}" >&2
    exit 1
  fi
}

[[ -f "${WORKFLOW}" ]]
[[ -x "${GENERATOR}" ]]
[[ -x "${OVERWRITE_GUARD}" ]]

require_literal "${WORKFLOW}" 'workflow_dispatch:'
require_literal "${WORKFLOW}" 'if: inputs.acknowledge_non_production'
require_literal "${WORKFLOW}" 'permissions:'
require_literal "${WORKFLOW}" 'contents: read'
require_literal "${WORKFLOW}" 'packages: write'
require_literal "${WORKFLOW}" 'id-token: write'
require_literal "${WORKFLOW}" 'attestations: write'
require_literal "${WORKFLOW}" 'image_tag="sha-${source_sha}"'
require_literal "${WORKFLOW}" 'for component in ateapi atecontroller ateom-gvisor atenet substrate-release-verify; do'
require_literal "${WORKFLOW}" './cmd/substrate-release-verify/...'
require_literal "${WORKFLOW}" '--platform linux/amd64,linux/arm64'
require_literal "${WORKFLOW}" 'CGO_ENABLED=0'
require_literal "${WORKFLOW}" '--sbom=spdx'
require_literal "${WORKFLOW}" 'actions/attest-build-provenance@4d101475d8b20a2381f78447822ac1eab6504dd8'
require_literal "${WORKFLOW}" 'subject-name: ghcr.io/${{ steps.coordinates.outputs.registry_repository }}/atenet'
require_literal "${WORKFLOW}" 'subject-digest: ${{ steps.images.outputs.atenet_digest }}'
require_literal "${WORKFLOW}" 'subject-name: ghcr.io/${{ steps.coordinates.outputs.registry_repository }}/substrate-release-verify'
require_literal "${WORKFLOW}" 'subject-digest: ${{ steps.images.outputs.substrate_release_verify_digest }}'
require_literal "${WORKFLOW}" 'published_ref="$(CGO_ENABLED=0'
require_literal "${WORKFLOW}" 'KO_DOCKER_REPO="${IMAGE_REGISTRY}/${component}"'
require_literal "${WORKFLOW}" 'ref_prefix="${tagged_ref}@"'
require_literal "${WORKFLOW}" 'digest="${published_ref#"${ref_prefix}"}"'
require_literal "${WORKFLOW}" 'version: v3.21.4'
require_literal "${WORKFLOW}" 'publish_chart substrate-crds CRDS_CHART_REF'
require_literal "${WORKFLOW}" 'publish_chart substrate APPLICATION_CHART_REF'
require_literal "${WORKFLOW}" './hack/refuse-gke-preview-overwrite.sh'
require_literal "${WORKFLOW}" './hack/generate-gke-preview-manifest.sh'
require_literal "${WORKFLOW}" 'PREVIEW_ATENET_REF="${ATENET_IMAGE_REF}"'
require_literal "${WORKFLOW}" 'docker buildx imagetools inspect "${agentgateway_ref}"'
require_literal "${WORKFLOW}" 'PREVIEW_AGENTGATEWAY_REF="${AGENTGATEWAY_IMAGE_REF}"'
require_literal "${WORKFLOW}" 'PREVIEW_RELEASE_VERIFIER_REF="${SUBSTRATE_RELEASE_VERIFY_IMAGE_REF}"'

if grep -Eq '(^|[^[:alnum:]_-])latest([^[:alnum:]_-]|$)' "${WORKFLOW}"; then
  printf '%s must never publish a latest tag\n' "${WORKFLOW}" >&2
  exit 1
fi
if grep -Eq '^[[:space:]]+(push|pull_request|schedule):' "${WORKFLOW}"; then
  printf '%s must remain dispatch-only\n' "${WORKFLOW}" >&2
  exit 1
fi
if grep -Eq 'contents:[[:space:]]*write|create-release|action-gh-release|git[[:space:]]+tag' "${WORKFLOW}"; then
  printf '%s must not create a source tag or GitHub release\n' "${WORKFLOW}" >&2
  exit 1
fi
if grep -Eq 'uses:[[:space:]]+[^#[:space:]]+@v[0-9]' "${WORKFLOW}"; then
  printf '%s must pin third-party actions by commit SHA\n' "${WORKFLOW}" >&2
  exit 1
fi

temporary_dir="$(mktemp -d)"
trap 'rm -rf "${temporary_dir}"' EXIT
fake_bin="${temporary_dir}/bin"
mkdir -p "${fake_bin}"

cat > "${fake_bin}/docker" <<'EOF'
#!/usr/bin/env bash
if [[ -n "${FAKE_EXISTING_IMAGE_COMPONENT:-}" && "$*" == *"/${FAKE_EXISTING_IMAGE_COMPONENT}:"* ]]; then
  printf 'existing image\n'
  exit 0
fi
if [[ "${FAKE_IMAGE_LOOKUP:-missing}" == "existing" ]]; then
  printf 'existing image\n'
  exit 0
fi
if [[ "${FAKE_IMAGE_LOOKUP:-missing}" == "error" ]]; then
  printf 'response status code 503: registry unavailable\n' >&2
  exit 1
fi
if [[ "${FAKE_IMAGE_LOOKUP:-missing}" == "missing-tool" ]]; then
  printf 'docker: command not found\n' >&2
  exit 127
fi
if [[ "${FAKE_IMAGE_LOOKUP:-missing}" == "generic-not-found" ]]; then
  printf 'credential helper not found\n' >&2
  exit 1
fi
printf 'manifest unknown\n' >&2
exit 1
EOF
cat > "${fake_bin}/helm" <<'EOF'
#!/usr/bin/env bash
if [[ "${FAKE_CHART_LOOKUP:-missing}" == "existing" ]]; then
  printf 'existing chart\n'
  exit 0
fi
if [[ "${FAKE_CHART_LOOKUP:-missing}" == "error" ]]; then
  printf 'response status code 503: registry unavailable\n' >&2
  exit 1
fi
if [[ "${FAKE_CHART_LOOKUP:-missing}" == "missing-tool" ]]; then
  printf 'helm: command not found\n' >&2
  exit 127
fi
if [[ "${FAKE_CHART_LOOKUP:-missing}" == "generic-not-found" ]]; then
  printf 'credential helper not found\n' >&2
  exit 1
fi
printf 'response status code 404: not found\n' >&2
exit 1
EOF
chmod +x "${fake_bin}/docker" "${fake_bin}/helm"

guard_env=(
  "PATH=${fake_bin}:${PATH}"
  "IMAGE_REGISTRY=ghcr.io/example/substrate"
  "IMAGE_TAG=sha-0123456789abcdef0123456789abcdef01234567"
  "CHART_VERSION=0.42.1"
)
env "${guard_env[@]}" "${OVERWRITE_GUARD}"
if env "${guard_env[@]}" FAKE_IMAGE_LOOKUP=existing "${OVERWRITE_GUARD}" >/dev/null 2>&1; then
  printf 'overwrite guard accepted an existing image tag\n' >&2
  exit 1
fi
if env "${guard_env[@]}" FAKE_EXISTING_IMAGE_COMPONENT=atenet "${OVERWRITE_GUARD}" >/dev/null 2>&1; then
  printf 'overwrite guard did not check the atenet image tag\n' >&2
  exit 1
fi
if env "${guard_env[@]}" FAKE_EXISTING_IMAGE_COMPONENT=substrate-release-verify "${OVERWRITE_GUARD}" >/dev/null 2>&1; then
  printf 'overwrite guard did not check the substrate-release-verify image tag\n' >&2
  exit 1
fi
if env "${guard_env[@]}" FAKE_CHART_LOOKUP=existing "${OVERWRITE_GUARD}" >/dev/null 2>&1; then
  printf 'overwrite guard accepted an existing chart version\n' >&2
  exit 1
fi
if env "${guard_env[@]}" FAKE_IMAGE_LOOKUP=error "${OVERWRITE_GUARD}" >/dev/null 2>&1; then
  printf 'overwrite guard treated an unknown registry failure as absence\n' >&2
  exit 1
fi
if env "${guard_env[@]}" FAKE_IMAGE_LOOKUP=missing-tool "${OVERWRITE_GUARD}" >/dev/null 2>&1; then
  printf 'overwrite guard treated a missing image probe as absence\n' >&2
  exit 1
fi
if env "${guard_env[@]}" FAKE_IMAGE_LOOKUP=generic-not-found "${OVERWRITE_GUARD}" >/dev/null 2>&1; then
  printf 'overwrite guard treated a missing image credential helper as coordinate absence\n' >&2
  exit 1
fi
if env "${guard_env[@]}" FAKE_CHART_LOOKUP=error "${OVERWRITE_GUARD}" >/dev/null 2>&1; then
  printf 'overwrite guard treated an unknown chart registry failure as absence\n' >&2
  exit 1
fi
if env "${guard_env[@]}" FAKE_CHART_LOOKUP=missing-tool "${OVERWRITE_GUARD}" >/dev/null 2>&1; then
  printf 'overwrite guard treated a missing chart probe as absence\n' >&2
  exit 1
fi
if env "${guard_env[@]}" FAKE_CHART_LOOKUP=generic-not-found "${OVERWRITE_GUARD}" >/dev/null 2>&1; then
  printf 'overwrite guard treated a missing chart credential helper as coordinate absence\n' >&2
  exit 1
fi

sha="0123456789abcdef0123456789abcdef01234567"
digest_a="sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
digest_b="sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
digest_c="sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
digest_d="sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
digest_e="sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
digest_f="sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
digest_verifier="sha256:1111111111111111111111111111111111111111111111111111111111111111"
agentgateway_refs="$(sed -nE 's|^  agentgateway: ([^[:space:]@]+@sha256:[0-9a-f]{64})$|\1|p' "${CHART_VALUES}")"
agentgateway_count="$(printf '%s\n' "${agentgateway_refs}" | awk 'NF { count++ } END { print count + 0 }')"
if [[ "${agentgateway_count}" -ne 1 ]]; then
  printf 'chart values must contain exactly one digest-qualified images.agentgateway reference\n' >&2
  exit 1
fi
agentgateway_ref="$(printf '%s\n' "${agentgateway_refs}" | awk 'NF { print; exit }')"

PREVIEW_OUTPUT="${temporary_dir}/manifest.json" \
PREVIEW_SOURCE_REPOSITORY="Example/Substrate" \
PREVIEW_REGISTRY_REPOSITORY="example/substrate" \
PREVIEW_SOURCE_SHA="${sha}" \
PREVIEW_IMAGE_TAG="sha-${sha}" \
PREVIEW_CHART_VERSION="0.42.1" \
PREVIEW_ATEAPI_REF="ghcr.io/example/substrate/ateapi@${digest_a}" \
PREVIEW_ATECONTROLLER_REF="ghcr.io/example/substrate/atecontroller@${digest_b}" \
PREVIEW_ATEOM_GVISOR_REF="ghcr.io/example/substrate/ateom-gvisor@${digest_c}" \
PREVIEW_ATENET_REF="ghcr.io/example/substrate/atenet@${digest_d}" \
PREVIEW_AGENTGATEWAY_REF="${agentgateway_ref}" \
PREVIEW_RELEASE_VERIFIER_REF="ghcr.io/example/substrate/substrate-release-verify@${digest_verifier}" \
PREVIEW_CRDS_CHART_REF="oci://ghcr.io/example/substrate/helm/substrate-crds@${digest_e}" \
PREVIEW_APPLICATION_CHART_REF="oci://ghcr.io/example/substrate/helm/substrate@${digest_f}" \
  "${GENERATOR}"

jq -e \
  --arg sha "${sha}" \
  --arg digest_a "${digest_a}" \
  --arg digest_c "${digest_c}" \
  --arg digest_d "${digest_d}" \
  --arg digest_e "${digest_e}" \
  --arg digest_f "${digest_f}" \
  --arg digest_verifier "${digest_verifier}" \
  --arg agentgateway_ref "${agentgateway_ref}" \
  '
    .schema_version == "yourown.chat/substrate-gke-preview/v1" and
    .deployment_class == "testbed" and
    .production_eligible == false and
    .source.commit == $sha and
    .candidate.image_tag == ("sha-" + $sha) and
    .image_digests.ateapi == $digest_a and
    .image_digests["ateom-gvisor"] == $digest_c and
    .image_digests.atenet == $digest_d and
    .image_digests.releaseVerifier == $digest_verifier and
    (.image_digests | keys) == ["ateapi", "atecontroller", "atenet", "ateom-gvisor", "releaseVerifier"] and
    .helm_values.image.digests.ateapi == $digest_a and
    .helm_values.image.digests.atenet == $digest_d and
    (.helm_values.image.digests | keys) == ["ateapi", "atecontroller", "atenet"] and
    .helm_values.images.agentgateway == $agentgateway_ref and
    (.helm_values.images | keys) == ["agentgateway"] and
    .images.atenet.ref == ("ghcr.io/example/substrate/atenet@" + $digest_d) and
    .images.agentgateway.ref == $agentgateway_ref and
    .images.releaseVerifier.ref == ("ghcr.io/example/substrate/substrate-release-verify@" + $digest_verifier) and
    (.images | keys) == ["agentgateway", "ateapi", "atecontroller", "atenet", "ateom-gvisor", "releaseVerifier"] and
    .charts.crds.digest == $digest_e and
    .charts.application.digest == $digest_f and
    (.images.ateapi.ref | contains(":latest") | not) and
    (.images.atenet.ref | contains(":latest") | not) and
    (.images.releaseVerifier.ref | contains(":latest") | not)
  ' "${temporary_dir}/manifest.json" >/dev/null

if PREVIEW_OUTPUT="${temporary_dir}/invalid.json" \
  PREVIEW_SOURCE_REPOSITORY="Example/Substrate" \
  PREVIEW_REGISTRY_REPOSITORY="example/substrate" \
  PREVIEW_SOURCE_SHA="${sha}" \
  PREVIEW_IMAGE_TAG="latest" \
  PREVIEW_CHART_VERSION="0.42.1" \
  PREVIEW_ATEAPI_REF="ghcr.io/example/substrate/ateapi@${digest_a}" \
  PREVIEW_ATECONTROLLER_REF="ghcr.io/example/substrate/atecontroller@${digest_b}" \
  PREVIEW_ATEOM_GVISOR_REF="ghcr.io/example/substrate/ateom-gvisor@${digest_c}" \
  PREVIEW_ATENET_REF="ghcr.io/example/substrate/atenet@${digest_d}" \
  PREVIEW_AGENTGATEWAY_REF="${agentgateway_ref}" \
  PREVIEW_RELEASE_VERIFIER_REF="ghcr.io/example/substrate/substrate-release-verify@${digest_verifier}" \
  PREVIEW_CRDS_CHART_REF="oci://ghcr.io/example/substrate/helm/substrate-crds@${digest_e}" \
  PREVIEW_APPLICATION_CHART_REF="oci://ghcr.io/example/substrate/helm/substrate@${digest_f}" \
    "${GENERATOR}" >/dev/null 2>&1; then
  printf 'manifest generator accepted a mutable image tag\n' >&2
  exit 1
fi

if PREVIEW_OUTPUT="${temporary_dir}/invalid-atenet.json" \
  PREVIEW_SOURCE_REPOSITORY="Example/Substrate" \
  PREVIEW_REGISTRY_REPOSITORY="example/substrate" \
  PREVIEW_SOURCE_SHA="${sha}" \
  PREVIEW_IMAGE_TAG="sha-${sha}" \
  PREVIEW_CHART_VERSION="0.42.1" \
  PREVIEW_ATEAPI_REF="ghcr.io/example/substrate/ateapi@${digest_a}" \
  PREVIEW_ATECONTROLLER_REF="ghcr.io/example/substrate/atecontroller@${digest_b}" \
  PREVIEW_ATEOM_GVISOR_REF="ghcr.io/example/substrate/ateom-gvisor@${digest_c}" \
  PREVIEW_ATENET_REF="ghcr.io/example/substrate/not-atenet@${digest_d}" \
  PREVIEW_AGENTGATEWAY_REF="${agentgateway_ref}" \
  PREVIEW_RELEASE_VERIFIER_REF="ghcr.io/example/substrate/substrate-release-verify@${digest_verifier}" \
  PREVIEW_CRDS_CHART_REF="oci://ghcr.io/example/substrate/helm/substrate-crds@${digest_e}" \
  PREVIEW_APPLICATION_CHART_REF="oci://ghcr.io/example/substrate/helm/substrate@${digest_f}" \
    "${GENERATOR}" >/dev/null 2>&1; then
  printf 'manifest generator accepted an atenet reference from the wrong repository\n' >&2
  exit 1
fi

if PREVIEW_OUTPUT="${temporary_dir}/missing-agentgateway.json" \
  PREVIEW_SOURCE_REPOSITORY="Example/Substrate" \
  PREVIEW_REGISTRY_REPOSITORY="example/substrate" \
  PREVIEW_SOURCE_SHA="${sha}" \
  PREVIEW_IMAGE_TAG="sha-${sha}" \
  PREVIEW_CHART_VERSION="0.42.1" \
  PREVIEW_ATEAPI_REF="ghcr.io/example/substrate/ateapi@${digest_a}" \
  PREVIEW_ATECONTROLLER_REF="ghcr.io/example/substrate/atecontroller@${digest_b}" \
  PREVIEW_ATEOM_GVISOR_REF="ghcr.io/example/substrate/ateom-gvisor@${digest_c}" \
  PREVIEW_ATENET_REF="ghcr.io/example/substrate/atenet@${digest_d}" \
  PREVIEW_RELEASE_VERIFIER_REF="ghcr.io/example/substrate/substrate-release-verify@${digest_verifier}" \
  PREVIEW_CRDS_CHART_REF="oci://ghcr.io/example/substrate/helm/substrate-crds@${digest_e}" \
  PREVIEW_APPLICATION_CHART_REF="oci://ghcr.io/example/substrate/helm/substrate@${digest_f}" \
    "${GENERATOR}" >/dev/null 2>&1; then
  printf 'manifest generator accepted a missing agentgateway dependency\n' >&2
  exit 1
fi

if PREVIEW_OUTPUT="${temporary_dir}/mismatched-agentgateway.json" \
  PREVIEW_SOURCE_REPOSITORY="Example/Substrate" \
  PREVIEW_REGISTRY_REPOSITORY="example/substrate" \
  PREVIEW_SOURCE_SHA="${sha}" \
  PREVIEW_IMAGE_TAG="sha-${sha}" \
  PREVIEW_CHART_VERSION="0.42.1" \
  PREVIEW_ATEAPI_REF="ghcr.io/example/substrate/ateapi@${digest_a}" \
  PREVIEW_ATECONTROLLER_REF="ghcr.io/example/substrate/atecontroller@${digest_b}" \
  PREVIEW_ATEOM_GVISOR_REF="ghcr.io/example/substrate/ateom-gvisor@${digest_c}" \
  PREVIEW_ATENET_REF="ghcr.io/example/substrate/atenet@${digest_d}" \
  PREVIEW_AGENTGATEWAY_REF="${agentgateway_ref%@*}@${digest_a}" \
  PREVIEW_RELEASE_VERIFIER_REF="ghcr.io/example/substrate/substrate-release-verify@${digest_verifier}" \
  PREVIEW_CRDS_CHART_REF="oci://ghcr.io/example/substrate/helm/substrate-crds@${digest_e}" \
  PREVIEW_APPLICATION_CHART_REF="oci://ghcr.io/example/substrate/helm/substrate@${digest_f}" \
    "${GENERATOR}" >/dev/null 2>&1; then
  printf 'manifest generator accepted an agentgateway dependency that differs from chart values\n' >&2
  exit 1
fi

if PREVIEW_OUTPUT="${temporary_dir}/missing-release-verifier.json" \
  PREVIEW_SOURCE_REPOSITORY="Example/Substrate" \
  PREVIEW_REGISTRY_REPOSITORY="example/substrate" \
  PREVIEW_SOURCE_SHA="${sha}" \
  PREVIEW_IMAGE_TAG="sha-${sha}" \
  PREVIEW_CHART_VERSION="0.42.1" \
  PREVIEW_ATEAPI_REF="ghcr.io/example/substrate/ateapi@${digest_a}" \
  PREVIEW_ATECONTROLLER_REF="ghcr.io/example/substrate/atecontroller@${digest_b}" \
  PREVIEW_ATEOM_GVISOR_REF="ghcr.io/example/substrate/ateom-gvisor@${digest_c}" \
  PREVIEW_ATENET_REF="ghcr.io/example/substrate/atenet@${digest_d}" \
  PREVIEW_AGENTGATEWAY_REF="${agentgateway_ref}" \
  PREVIEW_CRDS_CHART_REF="oci://ghcr.io/example/substrate/helm/substrate-crds@${digest_e}" \
  PREVIEW_APPLICATION_CHART_REF="oci://ghcr.io/example/substrate/helm/substrate@${digest_f}" \
    "${GENERATOR}" >/dev/null 2>&1; then
  printf 'manifest generator accepted a missing release verifier image\n' >&2
  exit 1
fi

if PREVIEW_OUTPUT="${temporary_dir}/mismatched-release-verifier.json" \
  PREVIEW_SOURCE_REPOSITORY="Example/Substrate" \
  PREVIEW_REGISTRY_REPOSITORY="example/substrate" \
  PREVIEW_SOURCE_SHA="${sha}" \
  PREVIEW_IMAGE_TAG="sha-${sha}" \
  PREVIEW_CHART_VERSION="0.42.1" \
  PREVIEW_ATEAPI_REF="ghcr.io/example/substrate/ateapi@${digest_a}" \
  PREVIEW_ATECONTROLLER_REF="ghcr.io/example/substrate/atecontroller@${digest_b}" \
  PREVIEW_ATEOM_GVISOR_REF="ghcr.io/example/substrate/ateom-gvisor@${digest_c}" \
  PREVIEW_ATENET_REF="ghcr.io/example/substrate/atenet@${digest_d}" \
  PREVIEW_AGENTGATEWAY_REF="${agentgateway_ref}" \
  PREVIEW_RELEASE_VERIFIER_REF="ghcr.io/example/substrate/not-substrate-release-verify@${digest_verifier}" \
  PREVIEW_CRDS_CHART_REF="oci://ghcr.io/example/substrate/helm/substrate-crds@${digest_e}" \
  PREVIEW_APPLICATION_CHART_REF="oci://ghcr.io/example/substrate/helm/substrate@${digest_f}" \
    "${GENERATOR}" >/dev/null 2>&1; then
  printf 'manifest generator accepted a release verifier image from the wrong repository\n' >&2
  exit 1
fi
