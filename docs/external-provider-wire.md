# External provider wire protocol

`ExternalProviderBroker` is the public, provider-neutral boundary for execution
slots outside the Substrate cluster. A provider opens one outbound gRPC session
and declares one or more stable slots; every slot corresponds to one durable
`Worker`. The protocol does not describe local runtimes, models, container
engines, endpoints, or routes.

## Authentication sequence

1. `Enroll` receives an out-of-band enrollment token in gRPC `authorization`
   metadata and returns a registration UID plus a refresh credential.
2. `MintSessionToken` receives that refresh credential in `authorization`
   metadata and returns a short-lived Connect token.
3. `Connect` receives the short-lived token in `authorization` metadata. Its
   first client frame is `ConnectHello` with `protocol_version=1`; no credential
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

This boundary does not register a listener, implement `Connect`, or publish a
route.

## Connect admission validation

The ateapi-private admission validator is a pure boundary between the first
frame, an authenticated session claim, and later Worker reconciliation. It is
split deliberately around the one-time credential claim. Credential-free
prevalidation first accepts only a `ClientFrame` containing a protocol-v1
`ConnectHello` with zero client generation and a serialized size no greater
than 1 MiB, validates the registration identity, and normalizes a sorted unique
slot list bounded to `1..256`. A malformed hello is therefore rejected without
burning its session token. Only after the database claim does the second step
require the hello registration to equal the authenticated registration and
enforce `scope.max_slots`.

Each slot uses the published 253-byte ASCII slot identity grammar, at most 64
Kubernetes label key/value pairs, and nonnegative ateapi `WorkerCapacity`
fields. A nil capacity is normalized to zero, retaining ateapi's
unknown/unconstrained meaning. `sandbox_class` remains provider-neutral and
opaque: admission requires valid UTF-8 and at most 253 bytes, but does not add
an undocumented enum or nonempty constraint. The accepted result stores no
protobuf message or caller-owned map and exposes only copied, non-secret
registration, generation, slot, label, and capacity data.

Neither validator claims a credential, receives a stream frame, reconciles a
Worker, or changes session/channel state. The future `Connect` owner performs
the receive and atomic claim between the two pure steps. `Connect` remains
`UNIMPLEMENTED` and does not invoke them.

## Server-derived Worker plan

An admitted slot is translated into an immutable, non-secret Worker plan before
any persistence operation. Substrate derives the global Worker resource name,
per-slot execution identity, and per-registration locality identity with
separate domain-separated SHA-256 inputs and length framing. The resulting
lowercase base32 values satisfy the existing Worker validators and do not
contain or concatenate caller-provided registration or slot strings.

The plan copies the authenticated namespace and pool plus the admitted sandbox
class, labels, and capacity, sets provider `ExternalSlot`, and leaves status
unset so the authoritative CreateWorker path can initialize the Worker
`OFFLINE`. Session generation and live routing are deliberately absent from the
durable identity, so a reconnect resolves the same Worker incarnation. A name
collision with different immutable provider, scope, capacity, execution, or
locality fields fails closed; only sandbox class and labels remain mutable under
the existing Worker contract.

Planning is still side-effect free. It does not list, create, update, activate,
drain, or delete Workers, and it does not make `Connect` available.

The in-process control API reconciler consumes that plan idempotently. It
creates missing Workers as `OFFLINE` and, after checking every immutable
identity field, may refresh only `sandbox_class` and labels. Reconnect keeps the
same Worker UID and emits no write when those mutable fields are unchanged.
Concurrent creates and updates are retried with the store's UID/version guards.
The reconciler neither activates current slots nor modifies slots omitted by a
new plan: route installation and session teardown own `ACTIVE`/`OFFLINE`, while
drain and deletion remain operator actions.

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

Normal cleanup first asks the authoritative directory to compare-and-withdraw
the exact publication proof. Only after the proof is no longer routable does it
enter the registry lifecycle gate and attempt to set every owned Worker
`OFFLINE`. The caller may exact-remove the generation lease afterward. A
transition changes only
`WorkerStatus.state`; the control-plane primitive preserves any Actor assignment
and uses the stored Worker UID/version preconditions. Before activation, the
lifecycle validates every reconciled Worker against the server-derived plan.
Every returned transition is checked again against the retained immutable
namespace, pool, capacity, execution identity, locality identity, name, and UID;
the store's immutable mutation contract prevents those fields from drifting
within that UID.

Availability changes are separate optimistic writes, not one database
transaction. On a partial activation failure, the lifecycle withdraws the route
and immediately attempts `OFFLINE` rollback for every call which may have
committed, including the failing call. Cleanup continues after individual
errors and returns deterministic sorted `offlined` and conservative `pending`
sets. Only `pending` is retained for a retry or inherited by a replacement, so
state remains bounded by the 256-slot admission limit and cannot accumulate
across failed generations.

This is deliberately an in-process core. The current ateapi deployment runs
multiple replicas, so a listener must not enable `Connect` until route and
Worker ownership have a distributed fencing authority or all sessions for a
registration are proven to land on one authoritative replica. Process restart
also loses the conservative set: desired Workers are preflighted safely, but an
omitted Worker left `ACTIVE` by the old process requires a startup/distributed
sweeper. This slice consumes only a narrow route-authority interface; it does
not build a second route directory or binding index. It does not register a
listener, implement `Connect`, receive or send stream frames, route channels,
or send `ConnectReady`.

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

This slice does not claim a session, run a stream loop, install a route, mutate
a Worker, or register a listener. `Broker.Connect` remains `UNIMPLEMENTED`.

## Authentication implementation boundary

The first broker-auth slice is private to the `ateapi` binary and is not
registered on a gRPC listener. Its in-process issuer returns a stable,
non-secret enrollment UID for operator lookup or revocation plus a separately
redacted credential. Enrollment credentials are valid for at most 24 hours;
session credentials are valid for at most 15 minutes. Both expiries and all
revocation timestamps use the PostgreSQL clock.

PostgreSQL stores only domain-separated SHA-256 credential digests. An
enrollment is single use, its registration retains the immutable owner
atespace, worker namespace, worker pool, and slot limit, and each registration
has exactly one current session digest. Revoking an enrollment also revokes its
registration. The schema reserves session consumption and generation fields,
and PostgreSQL now provides an atomic session claim: it validates the current
unexpired token, consumes it exactly once, and advances a nonzero generation
which fences older sessions. `Connect` remains `UNIMPLEMENTED` and does not yet
invoke that primitive. Network registration, TLS listener wiring, Worker
mutation, channel routing, and deployment manifests are intentionally outside
this slice.
