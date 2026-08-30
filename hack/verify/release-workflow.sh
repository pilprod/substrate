#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
WORKFLOW="${ROOT}/.github/workflows/release.yaml"
GUARD="${ROOT}/hack/refuse-release-overwrite.sh"

require_literal() {
  local literal="$1"
  if ! grep -Fq -- "${literal}" "${WORKFLOW}"; then
    printf '%s is missing release contract: %s\n' "${WORKFLOW}" "${literal}" >&2
    exit 1
  fi
}

require_literal 'group: release-${{ inputs.tag || github.sha }}'
require_literal 'uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262 # v4'
require_literal 'uses: actions/setup-go@40f1582b2485089dde7abd97c1529aa768e1baff # v5'
require_literal 'uses: ko-build/setup-ko@3aebd0597dc1e9d1a26bcfdb7cbeb19c131d3037 # v0.7'
require_literal 'uses: azure/setup-helm@bf6a7d304bc2fdb57e0331155b7ebf2c504acf0a # v4'
require_literal 'version: v3.21.4'
require_literal 'uses: docker/login-action@c94ce9fb468520275223c153574b00df6fe4bcc9 # v3'
require_literal 'uses: docker/setup-qemu-action@c7c53464625b32c7a7e944ae62b3e17d2b600130 # v3'
require_literal 'uses: softprops/action-gh-release@3bb12739c298aeb8a4eeaf626c5b8d85266b0e65 # v2'
require_literal 'IMAGE_REPOSITORY: ghcr.io/${{ steps.tag.outputs.registry_repository }}'
require_literal 'CHART_REPOSITORY: oci://ghcr.io/${{ steps.tag.outputs.registry_repository }}/helm'
require_literal 'run: ./hack/refuse-release-overwrite.sh'
require_literal 'for component in ateapi atecontroller atelet ateom-gvisor ateom-microvm podcertcontroller atenet substrate-release-verify; do'
require_literal '--tags "${IMAGE_TAG}"'
require_literal 'SHA="$(git rev-parse HEAD)"'
require_literal 'if [[ "${TAG}" =~ ^[Ll][Aa][Tt][Ee][Ss][Tt]$ ]]; then'
require_literal '^v[0-9]+\.[0-9]+\.[0-9]+$'

if grep -Eq 'uses:[[:space:]]+[^#[:space:]]+@(v[0-9]+|main|master)([[:space:]]|$)' "${WORKFLOW}"; then
  printf '%s contains a floating action reference\n' "${WORKFLOW}" >&2
  exit 1
fi
if grep -Eq -- '--tags.*(^|[ ,])latest([ ,]|$)|IMAGE_TAG:[[:space:]]*latest([[:space:]]|$)' "${WORKFLOW}"; then
  printf '%s contains a publication path for the moving latest tag\n' "${WORKFLOW}" >&2
  exit 1
fi
if grep -Fq 'ghcr.io/kagent-dev/substrate' "${WORKFLOW}"; then
  printf '%s hard-codes the upstream release repository\n' "${WORKFLOW}" >&2
  exit 1
fi

temporary_dir="$(mktemp -d)"
trap 'rm -rf "${temporary_dir}"' EXIT
fake_bin="${temporary_dir}/bin"
mkdir -p "${fake_bin}"

cat > "${fake_bin}/docker" <<'EOF'
#!/usr/bin/env bash
if [[ "${FAKE_EXISTING_IMAGE:-}" != "" && "$*" == *"/${FAKE_EXISTING_IMAGE}:"* ]]; then
  exit 0
fi
if [[ "${FAKE_AMBIGUOUS_IMAGE_ERROR:-}" == "true" ]]; then
  printf 'registry timeout\n' >&2
  exit 1
fi
if [[ "${FAKE_MISSING_IMAGE_TOOL:-}" == "true" ]]; then
  printf 'docker: command not found\n' >&2
  exit 127
fi
if [[ "${FAKE_GENERIC_NOT_FOUND:-}" == "true" ]]; then
  printf 'credential helper not found\n' >&2
  exit 1
fi
printf 'manifest unknown: not found\n' >&2
exit 1
EOF
cat > "${fake_bin}/git" <<'EOF'
#!/usr/bin/env bash
if [[ "${FAKE_EXISTING_TAG:-}" == "true" ]]; then
  exit 0
fi
if [[ "${FAKE_TAG_PROBE_ERROR:-}" == "true" ]]; then
  exit 128
fi
exit 2
EOF
cat > "${fake_bin}/gh" <<'EOF'
#!/usr/bin/env bash
if [[ "${FAKE_EXISTING_RELEASE:-}" == "true" ]]; then
  printf '{}\n'
  exit 0
fi
if [[ "${FAKE_RELEASE_PROBE_ERROR:-}" == "true" ]]; then
  printf 'HTTP 503\n' >&2
  exit 1
fi
printf 'HTTP 404\n' >&2
exit 1
EOF
cat > "${fake_bin}/helm" <<'EOF'
#!/usr/bin/env bash
if [[ "${FAKE_EXISTING_CHART:-}" != "" && "$*" == *"/${FAKE_EXISTING_CHART}"* ]]; then
  exit 0
fi
printf 'response status code 404: not found\n' >&2
exit 1
EOF
chmod +x "${fake_bin}/docker" "${fake_bin}/git" "${fake_bin}/gh" "${fake_bin}/helm"

guard_env=(
  "PATH=${fake_bin}:${PATH}"
  "CREATE_RELEASE=true"
  "RELEASE_TAG=v0.0.22"
  "GITHUB_REPOSITORY=pilprod/substrate"
  "IMAGE_REGISTRY=ghcr.io/pilprod/substrate"
  "CHART_REPOSITORY=oci://ghcr.io/pilprod/substrate/helm"
)
env "${guard_env[@]}" "${GUARD}" >/dev/null

expect_guard_failure() {
  if env "${guard_env[@]}" "$@" "${GUARD}" >/dev/null 2>&1; then
    printf 'release overwrite guard accepted forbidden state: %s\n' "$*" >&2
    exit 1
  fi
}

expect_guard_failure FAKE_EXISTING_IMAGE=ateapi
expect_guard_failure FAKE_AMBIGUOUS_IMAGE_ERROR=true
expect_guard_failure FAKE_MISSING_IMAGE_TOOL=true
expect_guard_failure FAKE_GENERIC_NOT_FOUND=true
expect_guard_failure FAKE_EXISTING_TAG=true
expect_guard_failure FAKE_TAG_PROBE_ERROR=true
expect_guard_failure FAKE_EXISTING_RELEASE=true
expect_guard_failure FAKE_RELEASE_PROBE_ERROR=true
expect_guard_failure FAKE_EXISTING_CHART=substrate
expect_guard_failure RELEASE_TAG=v0.0.22-rc1
expect_guard_failure CREATE_RELEASE=false RELEASE_TAG=LaTeSt
expect_guard_failure IMAGE_REGISTRY=ghcr.io/kagent-dev/substrate

printf 'Release workflow is fork-scoped, immutable, pinned, and overwrite-protected.\n'
