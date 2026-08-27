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

required=(
  PREVIEW_OUTPUT
  PREVIEW_SOURCE_REPOSITORY
  PREVIEW_REGISTRY_REPOSITORY
  PREVIEW_SOURCE_SHA
  PREVIEW_IMAGE_TAG
  PREVIEW_CHART_VERSION
  PREVIEW_ATEAPI_REF
  PREVIEW_ATECONTROLLER_REF
  PREVIEW_ATEOM_GVISOR_REF
  PREVIEW_CRDS_CHART_REF
  PREVIEW_APPLICATION_CHART_REF
)

for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    printf 'required environment variable %s is empty\n' "${name}" >&2
    exit 1
  fi
done

if [[ ! "${PREVIEW_SOURCE_REPOSITORY}" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]]; then
  printf 'PREVIEW_SOURCE_REPOSITORY must be an owner/repository pair\n' >&2
  exit 1
fi
if [[ ! "${PREVIEW_REGISTRY_REPOSITORY}" =~ ^[a-z0-9_.-]+/[a-z0-9_.-]+$ ]]; then
  printf 'PREVIEW_REGISTRY_REPOSITORY must be a lowercase owner/repository pair\n' >&2
  exit 1
fi
if [[ ! "${PREVIEW_SOURCE_SHA}" =~ ^[0-9a-f]{40}$ ]]; then
  printf 'PREVIEW_SOURCE_SHA must be a full lowercase commit SHA\n' >&2
  exit 1
fi
if [[ "${PREVIEW_IMAGE_TAG}" != "sha-${PREVIEW_SOURCE_SHA}" ]]; then
  printf 'PREVIEW_IMAGE_TAG must be sha- followed by PREVIEW_SOURCE_SHA\n' >&2
  exit 1
fi
if [[ ! "${PREVIEW_CHART_VERSION}" =~ ^0\.[1-9][0-9]*\.[1-9][0-9]*$ ]]; then
  printf 'PREVIEW_CHART_VERSION must use the non-production 0.RUN.ATTEMPT form\n' >&2
  exit 1
fi

digest_from_ref() {
  local name="$1"
  local ref="$2"
  local prefix="$3"

  if [[ "${ref}" != "${prefix}"@sha256:* ]] || [[ ! "${ref}" =~ @sha256:[0-9a-f]{64}$ ]]; then
    printf '%s must be an immutable reference below %s\n' "${name}" "${prefix}" >&2
    exit 1
  fi
  printf 'sha256:%s' "${ref##*@sha256:}"
}

image_root="ghcr.io/${PREVIEW_REGISTRY_REPOSITORY}"
chart_root="oci://${image_root}/helm"

ateapi_digest="$(digest_from_ref PREVIEW_ATEAPI_REF "${PREVIEW_ATEAPI_REF}" "${image_root}/ateapi")"
atecontroller_digest="$(digest_from_ref PREVIEW_ATECONTROLLER_REF "${PREVIEW_ATECONTROLLER_REF}" "${image_root}/atecontroller")"
ateom_gvisor_digest="$(digest_from_ref PREVIEW_ATEOM_GVISOR_REF "${PREVIEW_ATEOM_GVISOR_REF}" "${image_root}/ateom-gvisor")"
crds_chart_digest="$(digest_from_ref PREVIEW_CRDS_CHART_REF "${PREVIEW_CRDS_CHART_REF}" "${chart_root}/substrate-crds")"
application_chart_digest="$(digest_from_ref PREVIEW_APPLICATION_CHART_REF "${PREVIEW_APPLICATION_CHART_REF}" "${chart_root}/substrate")"

mkdir -p "$(dirname "${PREVIEW_OUTPUT}")"
temporary_output="$(mktemp "${PREVIEW_OUTPUT}.XXXXXX")"
trap 'rm -f "${temporary_output}"' EXIT

jq --null-input --sort-keys \
  --arg source_repository "${PREVIEW_SOURCE_REPOSITORY}" \
  --arg source_commit "${PREVIEW_SOURCE_SHA}" \
  --arg image_tag "${PREVIEW_IMAGE_TAG}" \
  --arg chart_version "${PREVIEW_CHART_VERSION}" \
  --arg image_registry "${image_root}" \
  --arg ateapi_ref "${PREVIEW_ATEAPI_REF}" \
  --arg ateapi_digest "${ateapi_digest}" \
  --arg atecontroller_ref "${PREVIEW_ATECONTROLLER_REF}" \
  --arg atecontroller_digest "${atecontroller_digest}" \
  --arg ateom_gvisor_ref "${PREVIEW_ATEOM_GVISOR_REF}" \
  --arg ateom_gvisor_digest "${ateom_gvisor_digest}" \
  --arg crds_chart_ref "${PREVIEW_CRDS_CHART_REF}" \
  --arg crds_chart_digest "${crds_chart_digest}" \
  --arg application_chart_ref "${PREVIEW_APPLICATION_CHART_REF}" \
  --arg application_chart_digest "${application_chart_digest}" \
  '{
    schema_version: "yourown.chat/substrate-gke-preview/v1",
    deployment_class: "testbed",
    production_eligible: false,
    source: {
      repository: $source_repository,
      commit: $source_commit
    },
    candidate: {
      image_tag: $image_tag,
      chart_version: $chart_version,
      image_registry: $image_registry
    },
    image_digests: {
      ateapi: $ateapi_digest,
      atecontroller: $atecontroller_digest,
      "ateom-gvisor": $ateom_gvisor_digest
    },
    helm_values: {
      image: {
        registry: $image_registry,
        digests: {
          ateapi: $ateapi_digest,
          atecontroller: $atecontroller_digest
        }
      }
    },
    images: {
      ateapi: {ref: $ateapi_ref},
      atecontroller: {ref: $atecontroller_ref},
      "ateom-gvisor": {ref: $ateom_gvisor_ref}
    },
    charts: {
      crds: {
        release_name: "substrate-crds",
        ref: $crds_chart_ref,
        version: $chart_version,
        digest: $crds_chart_digest
      },
      application: {
        release_name: "substrate",
        ref: $application_chart_ref,
        version: $chart_version,
        digest: $application_chart_digest
      }
    }
  }' > "${temporary_output}"

mv "${temporary_output}" "${PREVIEW_OUTPUT}"
trap - EXIT
