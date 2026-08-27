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
require_literal "${WORKFLOW}" 'image_tag="sha-${source_sha}"'
require_literal "${WORKFLOW}" 'for component in ateapi atecontroller ateom-gvisor; do'
require_literal "${WORKFLOW}" '--platform linux/amd64,linux/arm64'
require_literal "${WORKFLOW}" '--sbom=spdx'
require_literal "${WORKFLOW}" 'version: v3.21.4'
require_literal "${WORKFLOW}" 'publish_chart substrate-crds CRDS_CHART_REF'
require_literal "${WORKFLOW}" 'publish_chart substrate APPLICATION_CHART_REF'
require_literal "${WORKFLOW}" './hack/refuse-gke-preview-overwrite.sh'
require_literal "${WORKFLOW}" './hack/generate-gke-preview-manifest.sh'

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
if [[ "${FAKE_IMAGE_LOOKUP:-missing}" == "existing" ]]; then
  printf 'existing image\n'
  exit 0
fi
if [[ "${FAKE_IMAGE_LOOKUP:-missing}" == "error" ]]; then
  printf 'registry unavailable\n' >&2
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
  printf 'registry unavailable\n' >&2
  exit 1
fi
printf 'not found\n' >&2
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
if env "${guard_env[@]}" FAKE_CHART_LOOKUP=existing "${OVERWRITE_GUARD}" >/dev/null 2>&1; then
  printf 'overwrite guard accepted an existing chart version\n' >&2
  exit 1
fi
if env "${guard_env[@]}" FAKE_IMAGE_LOOKUP=error "${OVERWRITE_GUARD}" >/dev/null 2>&1; then
  printf 'overwrite guard treated an unknown registry failure as absence\n' >&2
  exit 1
fi

sha="0123456789abcdef0123456789abcdef01234567"
digest_a="sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
digest_b="sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
digest_c="sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
digest_d="sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
digest_e="sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

PREVIEW_OUTPUT="${temporary_dir}/manifest.json" \
PREVIEW_SOURCE_REPOSITORY="Example/Substrate" \
PREVIEW_REGISTRY_REPOSITORY="example/substrate" \
PREVIEW_SOURCE_SHA="${sha}" \
PREVIEW_IMAGE_TAG="sha-${sha}" \
PREVIEW_CHART_VERSION="0.42.1" \
PREVIEW_ATEAPI_REF="ghcr.io/example/substrate/ateapi@${digest_a}" \
PREVIEW_ATECONTROLLER_REF="ghcr.io/example/substrate/atecontroller@${digest_b}" \
PREVIEW_ATEOM_GVISOR_REF="ghcr.io/example/substrate/ateom-gvisor@${digest_c}" \
PREVIEW_CRDS_CHART_REF="oci://ghcr.io/example/substrate/helm/substrate-crds@${digest_d}" \
PREVIEW_APPLICATION_CHART_REF="oci://ghcr.io/example/substrate/helm/substrate@${digest_e}" \
  "${GENERATOR}"

jq -e \
  --arg sha "${sha}" \
  --arg digest_a "${digest_a}" \
  --arg digest_c "${digest_c}" \
  --arg digest_d "${digest_d}" \
  --arg digest_e "${digest_e}" \
  '
    .schema_version == "yourown.chat/substrate-gke-preview/v1" and
    .deployment_class == "testbed" and
    .production_eligible == false and
    .source.commit == $sha and
    .candidate.image_tag == ("sha-" + $sha) and
    .image_digests.ateapi == $digest_a and
    .image_digests["ateom-gvisor"] == $digest_c and
    .helm_values.image.digests.ateapi == $digest_a and
    .charts.crds.digest == $digest_d and
    .charts.application.digest == $digest_e and
    (.images.ateapi.ref | contains(":latest") | not)
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
  PREVIEW_CRDS_CHART_REF="oci://ghcr.io/example/substrate/helm/substrate-crds@${digest_d}" \
  PREVIEW_APPLICATION_CHART_REF="oci://ghcr.io/example/substrate/helm/substrate@${digest_e}" \
    "${GENERATOR}" >/dev/null 2>&1; then
  printf 'manifest generator accepted a mutable image tag\n' >&2
  exit 1
fi
