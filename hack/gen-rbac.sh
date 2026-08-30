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

# Generate the controller ClusterRole into the Helm chart and templatize its
# name so multi-release installs do not collide on a cluster-scoped resource.
#
# controller-gen emits a YAML file with a fixed `roleName=` value. We post-
# process that file to swap the static name for the chart's fullname helper,
# matching the convention used by every other resource in charts/substrate/.
#
# Invoked via `go generate ./cmd/atecontroller/internal/controllers/...`.
set -o errexit -o nounset -o pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${ROOT}/charts/substrate/templates/role.yaml"

bash "${ROOT}/hack/run-tool.sh" controller-gen \
  "rbac:headerFile=${ROOT}/hack/boilerplate/sh.txt,roleName=ate-controller" \
  paths="${ROOT}/cmd/atecontroller/internal/controllers/..." \
  "output:rbac:artifacts:config=${ROOT}/charts/substrate/templates/"

# Keep generated RBAC out of the external-control-plane-only profile and
# templatize every generated ate-controller role name. Use a temporary file
# instead of sed -i so generation is identical on GNU and BSD/macOS hosts.
TMP_OUT="$(mktemp "${OUT}.XXXXXX")"
trap 'rm -f "${TMP_OUT}"' EXIT
awk '
  !guarded && $0 == "---" {
    print "{{- if and (ne (.Values.profile | default \"standard\") \"external-control-plane-only\") .Values.rbac.create }}"
    guarded = 1
  }
  $0 == "  name: ate-controller" {
    print "  name: {{ include \"substrate.fullname\" (list \"ate-controller\" .) }}"
    next
  }
  { print }
  END {
    if (!guarded) {
      exit 1
    }
    print "{{- end }}"
  }
' "${OUT}" > "${TMP_OUT}"
mv "${TMP_OUT}" "${OUT}"
trap - EXIT
