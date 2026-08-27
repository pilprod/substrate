{{/*
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/}}

{{/*
Qualified resource name for a chart component.

Usage:
  {{ include "substrate.fullname" (list "ate-api-server" .) }}

When the release name is "substrate" (the canonical render in
hack/render-manifests.sh — `helm template substrate charts/substrate`), this
returns the bare component name, so the generated manifests/ate-install/
files keep their historical names ("ate-api-server", "ate-controller", ...).

Otherwise resources are prefixed with the release name in the standard Helm
style ("foo-ate-api-server", ...) so multiple releases coexist without
colliding.

The check is on the literal release name "substrate" rather than
$ctx.Chart.Name so this helper is context-safe: a parent chart can invoke it
with its own `.` (where .Chart.Name is the parent, not "substrate") and still
get the same prefixed name that this subchart's own templates render.
*/}}
{{- define "substrate.fullname" -}}
{{- $name := index . 0 -}}
{{- $ctx := index . 1 -}}
{{- if eq $ctx.Release.Name "substrate" -}}
{{- $name -}}
{{- else -}}
{{- printf "%s-%s" $ctx.Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
ServiceAccount name of ate-api-server, as this chart creates it. Parent
charts that need to bind additional Roles to this SA (e.g. env-source
Secret/ConfigMap reads for ActorTemplate resolution) should reference this
helper instead of hardcoding "ate-api-server":

  {{ include "substrate.ateApiServer.serviceAccountName" . }}
*/}}
{{- define "substrate.ateApiServer.serviceAccountName" -}}
{{- include "substrate.fullname" (list "ate-api-server" .) -}}
{{- end -}}

{{/*
gRPC endpoint that clients dial to reach ate-api-server. dns:/// scheme +
release-prefixed Service name + release namespace + :443. Suitable for
consumption as ATE_API_ENDPOINT / --ateapi-address:

  {{ include "substrate.ateApi.endpoint" . }}
    -> dns:///<release>-api.<namespace>.svc:443
*/}}
{{- define "substrate.ateApi.endpoint" -}}
{{- printf "dns:///%s.%s.svc:443" (include "substrate.fullname" (list "api" .)) .Release.Namespace -}}
{{- end -}}

{{/*
Plaintext HTTP URL that clients use to reach atenet-router.

  {{ include "substrate.atenetRouter.url" . }}
    -> http://<release>-atenet-router.<namespace>.svc:80
*/}}
{{- define "substrate.atenetRouter.url" -}}
{{- printf "http://%s.%s.svc:80" (include "substrate.fullname" (list "atenet-router" .)) .Release.Namespace -}}
{{- end -}}

{{/*
Build an image reference for a substrate component binary.

Usage:
  {{ include "substrate.componentImage" (list "ateapi" .) }}

Produces  {image.registry}/{name}@{digest} when image.digests[name] is set.
Otherwise produces {image.registry}/{name}:{tag}, where tag is resolved as:
  1. image.tag value, if set and not the sentinel "<none>"
  2. .Chart.AppVersion, if image.tag is empty
  3. no tag (no colon) when image.tag is the sentinel "<none>"

The "<none>" sentinel is used by hack/render-manifests.sh so that ko:// refs
are emitted without a tag, letting `ko resolve` supply the digest at build time.
*/}}
{{- define "substrate.componentImage" -}}
{{- $name := index . 0 -}}
{{- $ctx := index . 1 -}}
{{- $registry := $ctx.Values.image.registry -}}
{{- $digests := $ctx.Values.image.digests | default dict -}}
{{- $digest := get $digests $name | default "" -}}
{{- $tag := $ctx.Values.image.tag | default $ctx.Chart.AppVersion -}}
{{- if $digest -}}
{{- if not (regexMatch "^sha256:[0-9a-f]{64}$" $digest) -}}
{{- fail (printf "image.digests.%s must be a sha256 OCI digest" $name) -}}
{{- end -}}
{{- printf "%s/%s@%s" $registry $name $digest -}}
{{- else if ne $tag "<none>" -}}
{{- printf "%s/%s:%s" $registry $name $tag -}}
{{- else -}}
{{- printf "%s/%s" $registry $name -}}
{{- end -}}
{{- end -}}

{{/* Validate a required existing Kubernetes Secret name. */}}
{{- define "substrate.validateExistingSecretName" -}}
{{- $path := index . 0 -}}
{{- $value := index . 1 | default "" -}}
{{- if not $value -}}
{{- fail (printf "%s is required for profile external-control-plane-only" $path) -}}
{{- end -}}
{{- if or (gt (len $value) 253) (not (regexMatch "^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$" $value)) -}}
{{- fail (printf "%s must be a valid Kubernetes Secret name" $path) -}}
{{- end -}}
{{- end -}}

{{/* Validate a required key in a referenced Kubernetes Secret. */}}
{{- define "substrate.validateExistingSecretKey" -}}
{{- $path := index . 0 -}}
{{- $value := index . 1 | default "" -}}
{{- if or (not $value) (gt (len $value) 253) (not (regexMatch "^[A-Za-z0-9._-]+$" $value)) -}}
{{- fail (printf "%s must be a valid Kubernetes Secret data key" $path) -}}
{{- end -}}
{{- end -}}

{{/*
Validate cross-field topology contracts which JSON schema cannot express
without changing the existing chart's permissive values surface.
*/}}
{{- define "substrate.validateValues" -}}
{{- $profile := .Values.profile | default "standard" -}}
{{- if not (has $profile (list "standard" "external-control-plane-only")) -}}
{{- fail (printf "profile must be one of standard or external-control-plane-only, got %q" $profile) -}}
{{- end -}}
{{- $brokerPort := int .Values.externalProviderBroker.containerPort -}}
{{- if or (lt $brokerPort 1) (gt $brokerPort 65535) -}}
{{- fail "externalProviderBroker.containerPort must be between 1 and 65535" -}}
{{- end -}}
{{- if eq $brokerPort 443 -}}
{{- fail "externalProviderBroker.containerPort must differ from the Control API port 443" -}}
{{- end -}}
{{- if not .Values.externalProviderBroker.sessionTokenTTL -}}
{{- fail "externalProviderBroker.sessionTokenTTL must not be empty" -}}
{{- end -}}
{{- if eq $profile "external-control-plane-only" -}}
{{- if .Values.postgres.connectionString -}}
{{- fail "postgres.connectionString is forbidden for profile external-control-plane-only; reference externalControlPlane.postgres.existingSecret instead" -}}
{{- end -}}
{{- $postgresSecret := .Values.externalControlPlane.postgres.existingSecret -}}
{{- include "substrate.validateExistingSecretName" (list "externalControlPlane.postgres.existingSecret.name" $postgresSecret.name) -}}
{{- include "substrate.validateExistingSecretKey" (list "externalControlPlane.postgres.existingSecret.key" $postgresSecret.key) -}}
{{- $apiTLSSecret := .Values.externalControlPlane.tls.apiServer.existingSecret -}}
{{- include "substrate.validateExistingSecretName" (list "externalControlPlane.tls.apiServer.existingSecret.name" $apiTLSSecret.name) -}}
{{- include "substrate.validateExistingSecretKey" (list "externalControlPlane.tls.apiServer.existingSecret.credentialBundleKey" $apiTLSSecret.credentialBundleKey) -}}
{{- include "substrate.validateExistingSecretKey" (list "externalControlPlane.tls.apiServer.existingSecret.clientCAKey" $apiTLSSecret.clientCAKey) -}}
{{- if eq $apiTLSSecret.credentialBundleKey $apiTLSSecret.clientCAKey -}}
{{- fail "externalControlPlane.tls.apiServer existing Secret credentialBundleKey and clientCAKey must differ" -}}
{{- end -}}
{{- $controllerTLSSecret := .Values.externalControlPlane.tls.controller.existingSecret -}}
{{- include "substrate.validateExistingSecretName" (list "externalControlPlane.tls.controller.existingSecret.name" $controllerTLSSecret.name) -}}
{{- include "substrate.validateExistingSecretKey" (list "externalControlPlane.tls.controller.existingSecret.credentialBundleKey" $controllerTLSSecret.credentialBundleKey) -}}
{{- include "substrate.validateExistingSecretKey" (list "externalControlPlane.tls.controller.existingSecret.serverCAKey" $controllerTLSSecret.serverCAKey) -}}
{{- if eq $controllerTLSSecret.credentialBundleKey $controllerTLSSecret.serverCAKey -}}
{{- fail "externalControlPlane.tls.controller existing Secret credentialBundleKey and serverCAKey must differ" -}}
{{- end -}}
{{- $sharedTLSSecret := eq $apiTLSSecret.name $controllerTLSSecret.name -}}
{{- $privateKeyOverlap := or (eq $apiTLSSecret.credentialBundleKey $controllerTLSSecret.credentialBundleKey) (eq $apiTLSSecret.credentialBundleKey $controllerTLSSecret.serverCAKey) (eq $apiTLSSecret.clientCAKey $controllerTLSSecret.credentialBundleKey) -}}
{{- if and $sharedTLSSecret $privateKeyOverlap -}}
{{- fail "externalControlPlane.tls credential keys must not project a private-key bundle into both Pods when one Secret is shared" -}}
{{- end -}}
{{- end -}}
{{- end -}}
