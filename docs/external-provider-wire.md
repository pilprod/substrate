# External provider wire protocol

`ExternalProviderBroker` is the public, provider-neutral boundary for execution
slots outside the Substrate cluster. A provider opens one outbound gRPC session
and declares one or more stable slots; every slot corresponds to one durable
`Worker`. The protocol does not describe local runtimes, models, container
engines, endpoints, or routes.

## Authentication sequence

1. `Enroll` receives an out-of-band enrollment token in gRPC `authorization`
   metadata and returns a registration UID, refresh credential, and immutable
   server-owned slot capability policy.
2. `MintSessionToken` receives that refresh credential in `authorization`
   metadata and returns a short-lived Connect token plus the same non-secret
   registration policy for recovery.
3. `Connect` receives the short-lived token in `authorization` metadata. Its
   first client frame is `ConnectHello` with `protocol_version=2`; no credential
   is repeated in a frame. Token expiry is checked only at this handshake and
   does not terminate an already accepted stream.

All metadata uses `Bearer <token>`. Returned credential fields contain unpadded
base64url token bytes without the `Bearer ` prefix. These credentials are scoped
to `ExternalProviderBroker` and do not authorize the general Control API.
Production calls require server-authenticated TLS. `Enroll` consumes a
single-use token and is not automatically retry-safe after an ambiguous response
loss; recovery requires a new token or operator lookup.
The protobuf `debug_redact` option is a schema marker, not a substitute for
explicit redaction: implementations must never log credential responses or data
frames as whole protobuf messages.

### Client profile binding

A managed client persists the server-issued `profile_id` beside each local
capacity entry and sends it for every derived stable slot. It does not persist
or choose sandbox class, scheduling labels, CPU, or memory. If a registration
contains exactly one profile, a client may bind new capacity entries to it
automatically; heterogeneous policies require an explicit selection from the
IDs returned by `Enroll` or `MintSessionToken`. An unknown ID is never treated
as a request to create a profile. The existing `native|docker` launcher choice
remains local process-management configuration and is not encoded into this
policy. This requires a managed client configuration revision which adds only
the cluster-issued profile binding; it does not add client-supplied CPU/memory
authority.

## Session and channel ordering

The server accepts a zero-generation hello and replies with a nonzero generation
in `ConnectReady`. A newer generation atomically fences and closes older
sessions for the same registration; subsequent frames in both directions carry
that generation. The broker validates it against the authenticated stream rather
than trusting the value echoed by the client.

| Channel kind | Opener | Purpose |
| --- | --- | --- |
| `EXECUTION_GRPC` | Server | Worker lifecycle and execution RPC traffic |
| `ACTOR_INGRESS` | Server | Traffic from the cluster to the assigned actor |
| `ACTOR_EGRESS` | Client | Traffic from the assigned actor through cluster networking |

Client channel IDs are odd and server channel IDs are even. A channel proceeds
through `open`, `open_ack`, ordered `data`, and independent `half_close` frames;
`reset` terminates both directions. `ConnectReady` bounds every nonterminal
channel ID, including pending opens, and supplies the data-frame limit. Data is
capped at 64 KiB, while a hello and every serialized frame are bounded to 1 MiB
in the protobuf contract.

The broker derives stable execution and locality identities from the
authenticated registration and slot ID. A live connection, socket, URL, token,
or route is never persisted in a `Worker` or announced by the client.

### Live session generation boundary

The ateapi-private in-process registry is the generation fence for live session
ownership. An install is accepted only for a valid registration UID and a
nonzero generation which is greater than that registration's current live
generation. Installing the replacement and cancelling the previous lease happen
under the same registry lock. Exact lookups use both registration UID and
generation; callers must also observe the returned lease's done signal because a
newer generation may fence it immediately after lookup. Installation only
reserves the current generation: it does not prove that `ConnectReady` was sent
or that a route was published.

The registry assigns the lease identity itself. Cleanup removes an entry only
when registration UID, generation, and lease identity all match the current
entry, so delayed cleanup from a fenced stream cannot remove its replacement.
The registry retains the highest accepted generation as a tombstone after live
route removal. This prevents a slow, older claim from becoming current after a
newer stream has already disconnected. Active entries plus tombstones are
bounded by the configured maximum number of tracked registrations. At capacity,
an unknown registration fails closed while a tracked registration may still
install a newer generation. Tombstones live for the registry process lifetime;
this slice deliberately has no unsafe eviction API because safe reclamation
requires authoritative registration revocation integration. A restart cannot
replay an old session token because PostgreSQL consumed it atomically before the
original admission.

Holders of a fenced lease own that reference until their stream cleanup
finishes. Leases contain only the non-secret registration UID, generation, and
cancellation state. The registry starts no goroutines and contains no
credential, frame, channel, transport, or persistence state. A stable
per-registration lifecycle record, retained with the bounded generation
tombstone, holds at most the admitted 256 immutable Worker references which may
still be `ACTIVE`. It contains no assignment, mutable labels, sandbox class, or
client-supplied status.

The registry itself does not register a listener, receive `Connect`, or publish
a route; the bound session authority composes it with those separate owners.

## Connect admission validation

The ateapi-private admission validator is a pure boundary between the first
frame, an authenticated session claim, and later Worker reconciliation. It is
split deliberately around the one-time credential claim. Credential-free
prevalidation first accepts only a `ClientFrame` containing a protocol-v2
`ConnectHello` with zero client generation and a serialized size no greater
than 1 MiB, validates the registration identity, and normalizes a sorted unique
slot list bounded to `1..256`. A malformed hello is therefore rejected without
burning its session token. Only after the database claim does the second step
require the hello registration to equal the authenticated registration and
enforce `scope.max_slots`, the policy digest, every selected profile, and its
per-profile slot limit.

Each declaration contains only a stable 253-byte ASCII `slot_id` and a
`profile_id` issued by the server. The legacy `sandbox_class`, labels, and
capacity fields must be empty; any client assertion is rejected before the
session credential is claimed. Admission resolves exact sandbox class,
Kubernetes scheduling labels, and positive CPU/memory capacity from the
registration policy. This supports heterogeneous slots without allowing a
client to create a privileged class, spoof a scheduling label, or overstate
capacity. A launcher such as a native process or container engine is never a
sandbox class.

The policy has a versioned, order-independent canonical encoding: profiles and
labels are sorted, duplicate IDs are rejected, and a domain-separated SHA-256
digest is persisted and echoed in `ConnectHello`. Unknown versions,
noncanonical persistence, digest mismatch, invalid labels, and grants whose
profile slot total is smaller than `scope.max_slots` fail closed. The accepted
result stores no protobuf message or caller-owned map and exposes only copied,
non-secret registration, generation, slot, profile, label, and capacity data.

Neither validator claims a credential, receives a stream frame, reconciles a
Worker, or changes session/channel state. `Broker.Connect` receives and
credential-free prevalidates Hello, atomically claims the token, and passes the
two immutable results to the coordinator.

## Server-derived Worker plan

An admitted slot is translated into an immutable, non-secret Worker plan before
any persistence operation. Substrate derives the global Worker resource name,
per-slot execution identity, and per-registration locality identity with
separate domain-separated SHA-256 inputs and length framing. The resulting
lowercase base32 values satisfy the existing Worker validators and do not
contain or concatenate caller-provided registration or slot strings.

The plan copies the authenticated namespace and pool plus the policy-derived
sandbox class, scheduling labels, and exact capacity, sets provider
`ExternalSlot`, and leaves status unset so the authoritative CreateWorker path
can initialize the Worker `OFFLINE`.
Session generation and live routing are deliberately absent from the durable
identity, so a reconnect resolves the same Worker incarnation. A name collision
with different immutable provider, scope, capacity, execution, or locality
fields fails closed; only sandbox class and labels remain mutable under the
existing Worker contract.

Planning is still side-effect free. It does not list, create, update, activate,
drain, or delete Workers, and it does not make `Connect` available.

The in-process control API reconciler consumes that plan idempotently. Before
any Worker write, it resolves the exact WorkerPool pinned by the authenticated
scope and merges its metadata labels into every planned Worker. WorkerPool
labels and slot-profile labels are both server-owned; a key collision is
rejected even when both values match. The complete merged set is bounded to 64 valid Kubernetes labels,
and every candidate is preflighted before reconciliation starts. Missing or
invalid pools and listers fail closed.

The reconciler creates missing Workers as `OFFLINE` and, after checking every
immutable identity field, may refresh only `sandbox_class` and effective labels.
A WorkerPool label change therefore converges existing external Workers without
changing their UID, status, or assignment. Reconnect emits no write when those
mutable fields are unchanged. Concurrent creates and updates are retried with
the store's UID/version guards. The reconciler neither activates current slots
nor modifies slots omitted by a new plan: route installation and session
teardown own `ACTIVE`/`OFFLINE`, while drain and deletion remain operator
actions.

## Generation-safe Worker availability

The ateapi-private Worker session lifecycle joins a route-publication proof to
the existing control-plane availability primitive without making Worker status
client-writable. The required caller order is:

1. Install the nonzero generation lease; it is not routable by itself.
2. Reconcile the complete Worker plan while every new Worker starts `OFFLINE`.
3. Initialize the stream and send `ConnectReady`.
4. Atomically publish all route bindings and obtain the immutable publication
   proof from the route directory.
5. Pass that proof to Worker activation.

Activation rejects a bare lease, an unpublished or withdrawn proof, a proof for
another lease/generation, and any Worker tuple not authorized by the current
publication. Only the exact live registry lease behind that proof may enter the
per-registration lifecycle gate. Under the gate, activation proceeds in this
order:

1. Set every conservatively owned Worker from the previous generation to
   `OFFLINE`, including a slot omitted from the new complete declaration.
2. Preflight every desired Worker to `OFFLINE`. This also closes desired slots
   which a previous ateapi process may have left active.
3. Set the desired Workers to `ACTIVE` in deterministic Worker-name order.

The route is therefore Ready and published before any Worker becomes `ACTIVE`.
A replacement install uses the same per-registration gate, so it cannot fence a
lease halfway through an availability pass. Conversely, cleanup of a fenced
lease observes that it is no longer current and performs no Worker mutation;
the current generation has inherited the conservative ownership set.

Normal cleanup first changes the exact publication proof from `OPEN` to
`CLOSING`, which immediately rejects new assignments while retaining its
bindings. Under the same per-registration lifecycle gate it then sets every
owned Worker `OFFLINE`, withdraws the route, and fences the lease. The caller
may exact-remove the generation lease afterward. A
transition changes only
`WorkerStatus.state`; the control-plane primitive preserves any Actor assignment
and uses the stored Worker UID/version preconditions. Before activation, the
lifecycle validates every reconciled Worker against the server-derived plan.
Every returned transition is checked again against the retained immutable
namespace, pool, capacity, execution identity, locality identity, name, and UID;
the store's immutable mutation contract prevents those fields from drifting
within that UID.

Availability changes are separate optimistic writes, not one database
transaction. On a partial activation failure, the lifecycle closes the route,
immediately attempts `OFFLINE` rollback for every call which may have committed
including the failing call, and withdraws only after rollback succeeds. Cleanup
continues after individual errors and returns deterministic sorted `offlined`
and conservative `pending` sets. Only `pending` is retained for a retry or
inherited by a replacement, so state remains bounded by the 256-slot admission
limit and cannot accumulate across failed generations.

This is deliberately an in-process core. The default ateapi deployment still
runs multiple replicas. The opt-in external-provider Broker release profile
therefore pins ateapi to one replica with `Recreate`, so only one process owns
session routes at a time; HA requires a future distributed fencing authority.
Startup recovery makes persisted external Workers `OFFLINE` before the Broker
listener is created, and every desired Worker is preflighted `OFFLINE` on
reconnect. The scheduling guard and Connect lifecycle share the same registry
and route directory; no second binding index exists.

Every newly materialized `WorkerAssignment` also snapshots the selected
Worker's server-assigned resource UID in `worker_resource_uid`. This is a
general durable-resource incarnation pin and is separate from both the
Kubernetes-only `worker_pod_uid` and the provider's stable
`external_slot.execution_identity`. The schema accepts an absent pin only to
decode assignments persisted before this field existed. A new external
execution route must resolve the Worker by name and call the strict
`workerassignment.ValidateIncarnation` helper before using provider identity;
missing pins, deleted-and-recreated Workers with the same name, and all UID
mismatches fail closed.

## Post-Ready channel state

The ateapi-private channel state machine begins only after a valid admission and
`ConnectReady`. It snapshots the nonzero fencing generation and the complete
admitted slot set, then validates every later client frame without receiving or
sending on a transport. Server opens, acknowledgements, data, half-closes,
resets, and heartbeat probes likewise return immutable effects; the future
session router remains responsible for executing them in stream order.

The state machine enforces the 1 MiB serialized frame limit, generation match,
channel ID parity and lifetime non-reuse, opener-specific channel kinds, slot
membership, exactly-once acknowledgements, accepted-only data and close
transitions, independent half-closes, terminal resets, and heartbeat probe/ack
pairing. Peer data is copied before it leaves validation, while outbound frame
accessors return a new protobuf copy. Error and reset text must be valid UTF-8
and at most 1024 bytes and remain explicitly untrusted. Every effect also has a
monotonic session-local sequence so a transport owner can preserve transition
order when server actions originate from concurrent goroutines.

All mutable state is mutex-protected. `max_open_channels` bounds accepted and
pending nonterminal channels. A separate retained-ID limit bounds the
tombstones required to reject channel ID reuse; exhausting it requires a new
session generation instead of allowing memory to grow indefinitely. A separate
limit bounds server heartbeat probes awaiting acknowledgements. These local
resource ceilings do not alter `ConnectReady.max_data_bytes` or the public
channel semantics.

The state machine still owns no transport, route, or Worker. The Connect owner
applies its effects in stream order through a one-frame bounded receive pump.

## Transport-neutral session coordination

The ateapi-private session coordinator composes the admission, Worker, channel,
route, and availability primitives without owning a socket. The Broker
transport first receives and credential-free prevalidates `ConnectHello`, then
atomically claims the one-time session token. It passes only the resulting
non-secret `SessionClaim` and immutable prevalidated hello to the coordinator.

For one generation, the coordinator enforces this order:

1. Complete authenticated admission validation.
2. Install the non-routable generation lease.
3. Derive and reconcile the complete Worker plan, then validate every returned
   Worker incarnation and build route bindings.
4. Construct the bounded post-Ready channel state.
5. Invoke one synchronous transport callback with a copied `ConnectReady`
   frame. The callback returns success only after the frame is visible to the
   peer.
6. Publish the bindings through the existing `SessionRouteDirectory`.
7. Activate Workers through the generation-gated Worker lifecycle.

No Worker becomes `ACTIVE` before both the Ready callback and route publication
succeed. Admission, reconciliation, and channel-state failures never cross the
Ready boundary. A Ready callback failure never publishes a route. A route
publication failure never activates a Worker. An activation failure uses the
existing compare-and-withdraw plus deterministic OFFLINE rollback.
The Ready callback and route publication run under the registry's stable
per-registration lifecycle gate. A replacement cannot fence the lease between
those two operations, and a generation fenced during reconciliation never
sends Ready.

Failure before publication also enters the lifecycle gate and offlines the
bounded conservative Worker set inherited from a fenced generation. Failed
setup exact-removes its lease after that best-effort pass; any Workers whose
OFFLINE write failed remain in the bounded lifecycle tombstone for a newer
generation to preflight. A successfully established session closes in the
opposite safety order: mark its exact route `CLOSING`, set owned Workers
`OFFLINE`, withdraw the route, then exact-remove its lease. If OFFLINE fails
during normal close, the closed route retains its bindings while remaining
unavailable and the handle retains the lease for an explicit bounded retry.

The coordinator starts no goroutine and emits no log. Its callback frame and
all retained state contain no credential or data payload. Existing admission,
route, registry, channel, and Worker bounds remain authoritative; the
coordinator adds no second route map or unbounded retry.

The dedicated Broker listener is TLS-only, default-disabled, and exposes only
`ExternalProviderBroker`. It prevalidates Hello before consuming the credential,
sends Ready synchronously, publishes and activates through the coordinator,
then applies post-Ready frames with at most one frame queued. EOF, cancellation,
transport failure, protocol failure, route replacement, and unsupported effects
all enter the same CLOSING, OFFLINE, withdraw, and fence cleanup. Handler status
messages are fixed and the Broker never logs authorization metadata, frames,
payloads, peer reset text, or transport error text.

Heartbeat response frames are transport-complete. Execution forwarding is the
remaining boundary: there is not yet an authority which binds server-opened
`EXECUTION_GRPC` or `ACTOR_INGRESS` channels, or client-opened `ACTOR_EGRESS`
channels, to the exact routed Worker and its cluster data plane. Until that
interface exists, every effect requiring such forwarding fails the entire
session closed with `FAILED_PRECONDITION`; no actor or execution bytes are
accepted or silently dropped. The directory and lease are process-local. The
opt-in Helm profile therefore uses one ateapi replica with `Recreate`;
distributed route ownership is required before this mode can regain HA or
zero-downtime rollout.

## Required workload provider opt-in

Capability profiles constrain an external Worker; they do not by themselves
authorize an actor to leave Kubernetes-backed capacity. Before the broker is
enabled, a separate API migration must add an explicit Worker provider
constraint to ActorTemplate/Actor scheduling authority, default it to
`KUBERNETES_POD`, propagate it into `scheduling.Constraints`, and require an
exact match with `Worker.provider`. Only an explicit `EXTERNAL_SLOT` value may
select these Workers. That migration necessarily updates the public ateapi
protobuf, Kubernetes ActorTemplate API/CRD and generated code, control-api
translation/validation, workflow constraint construction, scheduler matching,
and their compatibility tests; it is intentionally not hidden inside this
broker-policy slice.

## Authentication implementation boundary

The first broker-auth slice is private to the `ateapi` binary and is not
registered on a gRPC listener. Its in-process issuer returns a stable,
non-secret enrollment UID for operator lookup or revocation plus a separately
redacted credential. Enrollment credentials are valid for at most 24 hours;
session credentials are valid for at most 15 minutes. Both expiries and all
revocation timestamps use the PostgreSQL clock.

PostgreSQL stores only domain-separated SHA-256 credential digests. An
enrollment is single use, its registration retains the immutable owner
atespace, worker namespace, worker pool, slot limit, and exact canonical slot
policy plus digest, and each registration has exactly one current session
digest. Existing development rows created before policy columns remain
unusable and require a new enrollment; the schema never invents authority for
them. Revoking an enrollment also revokes its registration. The schema reserves session consumption and generation fields,
and PostgreSQL now provides an atomic session claim: it validates the current
unexpired token, consumes it exactly once, and advances a nonzero generation
which fences older sessions. `Connect` invokes that primitive only after a
valid first frame. Execution-channel forwarding remains separate from
authentication persistence and fails closed as described above.
