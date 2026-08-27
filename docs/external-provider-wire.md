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

## Connect admission validation

The ateapi-private admission validator is a pure boundary between an
already-authenticated session claim and later Worker reconciliation. It accepts
only a first `ClientFrame` containing a protocol-v1 `ConnectHello` with zero
client generation and a serialized size no greater than 1 MiB. The hello's
registration must exactly match the claim and its sorted, unique slot list is
bounded to `1..min(scope.max_slots, 256)`.

Each slot uses the published 253-byte ASCII slot identity grammar, at most 64
Kubernetes label key/value pairs, and nonnegative ateapi `WorkerCapacity`
fields. A nil capacity is normalized to zero, retaining ateapi's
unknown/unconstrained meaning. `sandbox_class` remains provider-neutral and
opaque: admission requires valid UTF-8 and at most 253 bytes, but does not add
an undocumented enum or nonempty constraint. The accepted result stores no
protobuf message or caller-owned map and exposes only copied, non-secret
registration, generation, slot, label, and capacity data.

This validator does not claim a credential, receive a stream frame, reconcile a
Worker, or change session/channel state. `Connect` remains `UNIMPLEMENTED` and
does not invoke it.

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
