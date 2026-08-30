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

# Refuse a release before the first push unless every tag, image, chart, and
# GitHub release coordinate is proven absent. A probe failure is not absence:
# only an explicit not-found result is accepted.

set -o errexit -o nounset -o pipefail

: "${CREATE_RELEASE:?CREATE_RELEASE is required}"
: "${RELEASE_TAG:?RELEASE_TAG is required}"
: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}"
: "${IMAGE_REGISTRY:?IMAGE_REGISTRY is required}"
: "${CHART_REPOSITORY:?CHART_REPOSITORY is required}"

fail() {
  printf 'release overwrite guard: %s\n' "$*" >&2
  exit 1
}

if [[ "${CREATE_RELEASE}" != "true" && "${CREATE_RELEASE}" != "false" ]]; then
  fail "CREATE_RELEASE must be true or false"
fi
if [[ ! "${RELEASE_TAG}" =~ ^[A-Za-z0-9_][A-Za-z0-9._-]{0,127}$ ]]; then
  fail "RELEASE_TAG is not a valid OCI tag"
fi
if [[ "${RELEASE_TAG}" =~ ^[Ll][Aa][Tt][Ee][Ss][Tt]$ ]]; then
  fail "the moving latest tag is forbidden"
fi
if [[ ! "${GITHUB_REPOSITORY}" =~ ^[a-z0-9_.-]+/[a-z0-9_.-]+$ ]]; then
  fail "GITHUB_REPOSITORY must be one lowercase owner/repository path"
fi
if [[ "${IMAGE_REGISTRY}" != "ghcr.io/${GITHUB_REPOSITORY}" ]]; then
  fail "IMAGE_REGISTRY must be the current fork repository"
fi
if [[ "${CHART_REPOSITORY}" != "oci://ghcr.io/${GITHUB_REPOSITORY}/helm" ]]; then
  fail "CHART_REPOSITORY must be the current fork Helm repository"
fi
if [[ "${CREATE_RELEASE}" == "true" && ! "${RELEASE_TAG}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  fail "a GitHub release tag must match vMAJOR.MINOR.PATCH"
fi
if [[ "${CREATE_RELEASE}" == "false" && "${RELEASE_TAG}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  fail "SemVer release coordinates are reserved for CREATE_RELEASE=true"
fi

probe_registry_absence() {
  local kind="$1"
  local coordinate="$2"
  shift 2
  local output status
  set +o errexit
  output="$("$@" 2>&1)"
  status=$?
  set -o errexit
  if [[ "${status}" -eq 0 ]]; then
    fail "${kind} coordinate already exists: ${coordinate}"
  fi
  if [[ "${status}" -eq 126 || "${status}" -eq 127 ]]; then
    fail "could not execute the ${kind} coordinate probe: ${coordinate}"
  fi
  if ! grep -Eiq '(manifest unknown|name unknown|failed to inspect:.*: not found|response status( code)?[^[:cntrl:]]*404|unexpected status[^[:cntrl:]]*404|HTTP[^[:cntrl:]]*404([^0-9]|$))' <<<"${output}"; then
    fail "could not prove ${kind} coordinate is absent: ${coordinate}"
  fi
}

components=(
  ateapi
  atecontroller
  atelet
  ateom-gvisor
  ateom-microvm
  podcertcontroller
  atenet
  substrate-release-verify
)
for component in "${components[@]}"; do
  image="${IMAGE_REGISTRY}/${component}:${RELEASE_TAG}"
  probe_registry_absence image "${image}" docker buildx imagetools inspect "${image}"
done

if [[ "${CREATE_RELEASE}" == "true" ]]; then
  set +o errexit
  git ls-remote --exit-code --tags origin "refs/tags/${RELEASE_TAG}" >/dev/null 2>&1
  tag_status=$?
  set -o errexit
  case "${tag_status}" in
    0) fail "source tag already exists: ${RELEASE_TAG}" ;;
    2) ;;
    *) fail "could not prove source tag is absent: ${RELEASE_TAG}" ;;
  esac

  set +o errexit
  release_output="$(gh api "repos/${GITHUB_REPOSITORY}/releases/tags/${RELEASE_TAG}" 2>&1)"
  release_status=$?
  set -o errexit
  if [[ "${release_status}" -eq 0 ]]; then
    fail "GitHub release already exists: ${RELEASE_TAG}"
  fi
  if ! grep -Eq '(HTTP 404|"status"[[:space:]]*:[[:space:]]*"?404"?)' <<<"${release_output}"; then
    fail "could not prove GitHub release is absent: ${RELEASE_TAG}"
  fi

  chart_version="${RELEASE_TAG#v}"
  for chart in substrate-crds substrate; do
    coordinate="${CHART_REPOSITORY}/${chart}:${chart_version}"
    probe_registry_absence chart "${coordinate}" \
      helm show chart "${CHART_REPOSITORY}/${chart}" --version "${chart_version}"
  done
fi

printf 'All requested release coordinates are absent.\n'
