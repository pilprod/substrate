# GKE preview release rail

The `GKE preview release` workflow publishes the minimal immutable Substrate
artifact set used by the external-provider GKE profile. It is a manual,
non-production handoff and does not create a Git tag, a GitHub release, or a
`latest` container tag.

## Artifact set

The selected workflow commit is built as the full `sha-<commit>` image tag for
Linux AMD64 and ARM64. The workflow publishes:

- `ateapi`, used by the external Control API and provider Broker;
- `atecontroller`, used by the external-template-only controller;
- `ateom-gvisor`, required by `WorkerPool.spec.ateomImage` even when the initial
  external provider pool has `replicas: 0`;
- the `substrate-crds` and `substrate` Helm charts.

`ko` generates SPDX SBOMs for each image. The workflow retains those SBOMs and
the packaged charts with the handoff artifact for 14 days. The repository does
not currently have an established signing rail, so this preview workflow does
not introduce an independent signing policy. Consumers must use the emitted
OCI digests, not the convenience SHA tags, as the deployment authority.
Before publishing anything, the workflow checks all three SHA image tags and
both chart versions. It fails if any coordinate already exists, and it also
fails closed when the registry lookup cannot prove absence.

## Dispatch and handoff

Choose the exact source ref in GitHub Actions, run `GKE preview release`, and
acknowledge that the output is non-production. The checkout is bound to the
commit SHA resolved by that dispatch. A successful run uploads
`substrate-gke-preview-<commit>`, containing:

- `substrate-gke-preview.json` and its SHA-256 checksum;
- packaged chart archives;
- SPDX image SBOMs.

The JSON document is the app-gcp handoff. It records the source commit, image
registry, SHA tag, exact image digests, chart version, and digest-qualified OCI
chart references. Copy values from this document into the reviewed app-gcp
vendor bundle; do not make app-gcp discover a mutable registry tag at plan or
apply time.

The application chart still needs environment-specific values. Copy the
manifest's `helm_values.image` object so `ateapi` and `atecontroller` are
deployed by digest; do not reduce it to `candidate.image_tag`. A zero-replica
external `WorkerPool` must set `spec.ateomImage` to the manifest's
digest-qualified `images["ateom-gvisor"].ref`.

This rail never contains TLS material, database credentials, enrollment tokens,
or other secret bytes. Those remain separately governed deployment
prerequisites.

## Local checks

Run:

```sh
./hack/verify/gke-preview-release.sh
make verify-external-control-plane-chart verify-crd-chart
```

The static verifier also runs as part of `make verify` because
`hack/verify-all.sh` discovers every script under `hack/verify`.
