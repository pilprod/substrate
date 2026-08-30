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

for name in IMAGE_REGISTRY IMAGE_TAG CHART_VERSION; do
  if [[ -z "${!name:-}" ]]; then
    printf 'required environment variable %s is empty\n' "${name}" >&2
    exit 1
  fi
done

if [[ ! "${IMAGE_REGISTRY}" =~ ^ghcr\.io/[a-z0-9_.-]+/[a-z0-9_.-]+$ ]]; then
  printf 'IMAGE_REGISTRY must be a repository-scoped GHCR path\n' >&2
  exit 1
fi
if [[ ! "${IMAGE_TAG}" =~ ^sha-[0-9a-f]{40}$ ]]; then
  printf 'IMAGE_TAG must contain the full source commit SHA\n' >&2
  exit 1
fi
if [[ ! "${CHART_VERSION}" =~ ^0\.[1-9][0-9]*\.[1-9][0-9]*$ ]]; then
  printf 'CHART_VERSION must use the non-production 0.RUN.ATTEMPT form\n' >&2
  exit 1
fi

refuse_existing() {
  local kind="$1"
  local coordinate="$2"
  shift 2

  local output status
  set +o errexit
  output="$("$@" 2>&1)"
  status=$?
  set -o errexit

  if [[ "${status}" -eq 0 ]]; then
    printf '%s coordinate already exists and will not be overwritten: %s\n' "${kind}" "${coordinate}" >&2
    exit 1
  fi
  if [[ "${status}" -eq 126 || "${status}" -eq 127 ]]; then
    printf 'could not execute the %s coordinate probe: %s\n' "${kind}" "${coordinate}" >&2
    exit 1
  fi
  if grep -Eiq '(manifest unknown|name unknown|failed to inspect:.*: not found|response status( code)?[^[:cntrl:]]*404|unexpected status[^[:cntrl:]]*404|HTTP[^[:cntrl:]]*404([^0-9]|$))' <<< "${output}"; then
    return 0
  fi

  printf 'could not prove %s coordinate is absent: %s\n%s\n' "${kind}" "${coordinate}" "${output}" >&2
  exit 1
}

for component in ateapi atecontroller ateom-gvisor atenet substrate-release-verify; do
  ref="${IMAGE_REGISTRY}/${component}:${IMAGE_TAG}"
  refuse_existing image "${ref}" docker buildx imagetools inspect "${ref}"
done

chart_repository="oci://${IMAGE_REGISTRY}/helm"
for chart in substrate-crds substrate; do
  ref="${chart_repository}/${chart}"
  refuse_existing chart "${ref}:${CHART_VERSION}" helm show chart "${ref}" --version "${CHART_VERSION}"
done
