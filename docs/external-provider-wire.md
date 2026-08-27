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
but `Connect` remains `UNIMPLEMENTED` and does not authenticate or consume a
session token until an atomic claim-and-fence contract is implemented. Network
registration, TLS listener wiring, and deployment manifests are intentionally
outside this slice.
