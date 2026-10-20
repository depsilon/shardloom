<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native encoded exchange: bounded protocol prerequisite

Status: proposed finite experiment contract, October 20, 2026. Provider intake and
local foundations have been checked. No transport, negotiated native client or
Flight SQL service is implemented by this document. The
[acceptance record](../benchmarks/evidence/native-exchange-prerequisites-2026-10-20/native-encoded-exchange-acceptance.json)
separates the completed prerequisite checks from the unexecuted tests below.
The four `NATIVE-ENCODED-EXCHANGE` substeps remain open in the
[phase plan](phased-execution-plan.md). This is the scoped design slice required
by its [verification contract](native-roadmap-verification-contracts-2026-10-18.md#native-encoded-exchange),
under RFCs [0013](../rfcs/0013-streaming-zero-copy-boundary-interoperability.md),
[0035](../rfcs/0035-rest-event-remote-api-surface.md) and
[0044](../rfcs/0044-resident-runtime-resource-ownership.md).

## Decision and scope

Use the existing Vortex 0.85 IPC codec and checked receiving owner. Investigate
one explicitly selected native mode carried by low-level Arrow Flight
`DoExchange` over mutually authenticated TLS. Keep ordinary Arrow Flight/Flight
SQL results in their standard Arrow representation. A native incompatibility
must fail; it must never select Arrow conversion or another execution engine.

The first experiment is two supervised processes on loopback, synthetic data,
an ephemeral listener, private test certificates and a predeclared allowlist of
sources and operations. It transfers one finite immutable record source and
executes different later native projections, including repeated execution of a
prepared projection, after releasing producer and transport buffers. Count alone
is insufficient: metadata could answer it without consuming column values.

This slice does not define a general remote service, arbitrary SQL submission,
distributed query scheduler, durable remote writes, object-store transfer,
cross-process pointers or shared-memory handles. It does not change public
Python/CLI routing, default features or package claims. These exclusions bound
the experiment; they do not close the broader roadmap obligations.

## Provider, license and security intake

The Vortex-first decision is `use_vortex_native_provider` for serialization and
receiving arrays. Vortex's array/context/registry and IPC framing already express
the representation. ShardLoom supplies operation identity, admission, lifetime,
authorization and completion. The receiving path remains
`ResidentMemorySource::from_vortex_ipc`; no second execution substrate is added.
The [accepted local interchange contract](native-encoded-interchange-2026-10-12.md)
records the existing provider and ownership patches.

| Layer | Pinned review input | Decision |
| --- | --- | --- |
| Native representation | Vortex / `vortex-ipc` 0.85.0, message V0; current checked ShardLoom receiver | Preserve the existing closed registry and schema bounds. No inference of compatibility with a later Vortex release. |
| Flight protocol and Rust binding | `arrow-flight` 58.3.0; Apache-2.0 | Match the existing Arrow 58.3.0 graph. Use generated low-level RPCs for the native extension; high-level Arrow decoders are for Arrow results. |
| RPC and TLS integration | `tonic` / `tonic-prost` 0.14.6; MIT | Use established HTTP/2, TLS configuration, peer-certificate and request middleware APIs. |
| TLS | `tokio-rustls` 0.26.6; `rustls` 0.23.46 with `ring` | Explicit trust roots, mandatory client certificates and ordinary server-name verification. No custom cryptography or certificate-verification bypass. |
| Existing graph alignment | Arrow 58.3.0, Prost 0.14.3, Tokio 1.52.1 | Exact direct pins in an isolated review manifest. This is not yet the integrated workspace graph. |

The isolated 161-package graph passes the repository license/source/bans policy
and the advisory check after replacing the initial Rustls 0.23.45 pin. The first
audit failed on [RUSTSEC-2026-0346](https://rustsec.org/advisories/RUSTSEC-2026-0346.html),
[0347](https://rustsec.org/advisories/RUSTSEC-2026-0347.html),
[0348](https://rustsec.org/advisories/RUSTSEC-2026-0348.html) and
[0349](https://rustsec.org/advisories/RUSTSEC-2026-0349.html); each names 0.23.46
as patched. Both graph identities and audit results are retained. No finding is
waived to admit the old TLS version. The existing `paste` unmaintained advisory
remains visible under the repository's existing policy; no new exception is
added. Dependency checks do not prove application security or runtime support.

The exact registry archives, reviewed source members, enabled features and
licenses are bound in the acceptance record. Published manifests declare version
ranges; the isolated lockfile establishes the versions reviewed here. A later
workspace integration must reproduce this graph or review its differences and
rerun the applicable audit. Flight/Tonic/TLS remain absent from the production
workspace at this prerequisite boundary.

Primary references checked on October 20: [Flight RPC](https://arrow.apache.org/docs/format/Flight.html),
[Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html),
[Arrow Flight 58.3.0 manifest](https://github.com/apache/arrow-rs/blob/913bab26ba9bed8fc2bc1acda300cc52345b0da1/arrow-flight/Cargo.toml),
[Tonic 0.14.6 manifest](https://github.com/grpc/grpc-rust/blob/6cb6056b5a748bc5a29bd48f4602dbc4e552bb7d/tonic/Cargo.toml)
and [Tonic TLS configuration](https://github.com/grpc/grpc-rust/blob/6cb6056b5a748bc5a29bd48f4602dbc4e552bb7d/tonic/src/transport/server/tls.rs).
Current [Vortex serialization documentation](https://docs.vortex.dev/developer-guide/internals/serialization)
is conceptual: its navigation now shows 0.90 draft and 0.89 released. The local
0.85 codec and checked receiver, rather than that navigation, define this pin.

## Negotiated representation and identity

The extension uses a versioned, typed command in the first `FlightDescriptor`.
Its native frames occupy `FlightData.app_metadata`; `data_header` and `data_body`
are empty. Those fields retain their standard Arrow meaning. Use the generated
raw Flight client, not an Arrow-record-batch decoder. Ordinary Flight SQL
commands and `DoGet` continue to return standard Arrow batches. This deliberate
extension requires an explicit native client and is not Flight SQL compatibility.

Before accepting payload bytes, both peers must agree on the extension version,
Vortex crate/profile identity, V0 message version, exact physical-registry
allowlist, logical dtype bytes, immutable source generation and resource limits.
Version 1 is an exact-match profile; it has no optimistic cross-version mode.
The implementation must freeze its Protobuf schema and generated-code provenance
before test execution. Unknown extension-envelope fields, tags and enum values are rejected
for this strict profile; ordinary Protobuf unknown-field tolerance is not proof
of successful negotiation.

The operation binding contains the authenticated principal, authorized query ID,
attempt ID, partition ID, source snapshot/content identity, dtype digest,
ordered source-row interval, and canonical typed-operation digest. The fixture
authority independently binds source and operation to the principal; a digest
supplied by the sender cannot grant access. Identifiers have explicit length
limits, no filesystem meaning and no authority to redirect the receiver.
All payload and completion frames bind the accepted operation. Equal dictionary
indices from different sources never establish equal dictionary domains.

Use monotonically increasing sequence numbers and exact length-delimited dtype
and array-message fragments. Hash the ordered, length-delimited original IPC
messages, including dtype, with SHA-256. Reject duplicate, skipped, reordered or
cross-attempt fragments, excess/trailing bytes and inconsistent totals. A final
completion record binds the exact row/message/byte totals and digest. The receiver
also requires clean input-stream EOF before exposing the source to the native
consumer. The requester accepts the completed workflow only after response EOF
and successful RPC status. A completion record alone cannot publish a source.
No consumer sees an incomplete source.

The finite receiver retains the existing limits: combined dtype/array messages
at most 64 MiB; each message header, dtype body and array metadata at most 8 MiB;
depth 24; at most 4,096 schema/array nodes and physical buffers; at most 64
distinct admitted registry names; host buffers, uncompressed transport buffers
and alignment exponent at most 16. These are refusal ceilings, not a promise
that a particular receiving grant admits their worst simultaneous allocation.
The existing coordinate checks, unsupported-extension checks and discarded wire
statistics remain mandatory. Keep coordinate execution distinct from payload
decoding in the intake evidence.

## Authentication, authorization and transport policy

Use Tonic's server identity, explicit client CA root and
`client_auth_optional(false)`. Clients use an explicit CA and expected server
name. The reviewed server path uses Rustls's client verifier; request peer
certificates come from the authenticated TLS connection. Authentication precedes
native parsing. Map the verified leaf-certificate fingerprint to a fixture
principal and check a deny-by-default permission record on **every RPC**.
User-supplied headers, query IDs, Flight tickets and a previous handshake are
not substitutes for that check.

Authorization separately controls source read, the named native operation,
result access and cancellation. Reject a valid but unauthorized certificate,
another principal's attempt/partition, mismatched source/plan and excessive
requested grants. A per-request decision is bound to an immutable policy version
for the bounded attempt. Revocation or policy replacement cancels affected live
attempts and denies subsequent calls; connection reuse cannot preserve stale
permission. The finite harness uses no proxy identity or bearer-token mode.

Bind only loopback on an ephemeral port for the experiment. Bound listener
connections, TLS handshakes, RPC concurrency, header bytes, message bytes and
idle/overall deadlines before reading arbitrary payloads. Disable gRPC
compression, TLS early data and TLS key logging; use the ordinary provider verification path.
The harness must prove rejection of missing/unknown/expired client certificates
and the wrong server name. Archive certificate fingerprints and public fixture
parameters, not private keys or session secrets. Delete the test keys when the
owned processes terminate. A fresh advisory check is required before any future
deployment; this finite local design does not certify a deployed service.

## Resource ownership and cancellation

Retain one explicit resource owner per attempt, with separate reservations for
send queues, transport/reassembly bytes, native receive/validation workspace,
dictionary payloads and downstream results. All charge the declared total;
moving ownership transfers credit, while an actual copy requires simultaneous
credit for both live allocations. Receiving-owned arrays and extracted buffers
keep their credits after transport and session handles are dropped. Returning
an ACK cannot release credit for a still-live source or result.

Freeze these initial ceilings in the fixture protocol: 256 MiB native memory
per peer, 4 MiB queued application bytes per direction, at most four queued
fragments, 1 MiB fragment payload, 2 MiB encoded RPC message, two connections,
one active data RPC per connection and two active attempts globally. Control
messages are at most 16 KiB. The complete pair remains bounded by the existing
64-MiB IPC limit. Admit a whole receiving source before building it; fragmenting
the transport does not turn the current whole-source receiver into streaming
execution. Over-budget reconstruction fails and releases partial owners.

Set fixed HTTP/2 connection/stream windows and disable adaptive window growth.
Charge application buffers before allocation and freeze provider buffer settings
in the implementation protocol. HTTP/2 windows and queue limits are not a total
process-memory guarantee: provider/TLS/runtime and kernel allocations must be
reported separately. Observe both processes' RSS and enforce the harness's
predeclared aggregate process limit; do not label native credit as an RSS bound.
If the provider cannot uphold the declared transport envelope, record that failed
prerequisite before measuring performance.

Version 1 owns each message's complete dictionary payload. It may preserve
sharing *inside* the validated array via existing receiving ownership; it does
not cache dictionary IDs across attempts, principals or source generations.
Duplicate payloads and alignment/copy costs are charged and measured. Dictionary
reuse, eviction, buffer loans and mapped same-host handles need a separate
rights/lifetime contract and comparison. Serialized aggregate state is refused;
its version and merge semantics cannot be inferred from the array protocol.

Cancellation, deadline, malformed payload, authorization loss, peer exit and
transport error put the attempt into a terminal failed state, cancel the native
token, stop further admission and drain/join owned work before return. A slow
consumer receives backpressure, never unbounded completed buffers. Unexpected
EOF is truncation, even after the declared row count. No partial successful
result or durable side effect is allowed. Preserve failure receipts without
retaining all failed payloads.

Reconnect does not resume an attempt. Reusing its ID is refused; a fresh retry
needs a newly authorized attempt and full transfer. Bound failed/completed-ID
tombstones by the harness's finite job list; exhaustion refuses new work rather
than silently forgetting replay state. Process restart discards in-memory
state and invalidates all old attempt IDs. This is explicit fresh execution,
not durable exactly-once delivery or recovery of interrupted computation.

## Registered test targets and evidence

The following integration targets and the experimental feature are **planned and
currently absent**. Do not run these selectors against the current tree or count
zero tests as acceptance. The implementation must add the named tests, prove
positive discovery counts and freeze source/build/fixture identities first.

| Target / required tests | Required proof |
| --- | --- |
| `native_encoded_exchange_protocol`: `negotiation_identity_and_strict_native_refusal` | Exact versions/registry/schema/source/plan binding, explicit native selection, unknown/duplicate fields and deterministic incompatibility; no Arrow retry or external engine. |
| `native_encoded_exchange_protocol`: `mutual_tls_per_request_authorization_and_isolation` | Actual TLS negative cases, unauthorized valid peer, cross-principal access, policy replacement and connection reuse. |
| `native_encoded_exchange_protocol`: `framing_limits_cancellation_replay_and_cleanup` | Boundary sizes, malformed native coordinates/registry, queue/receiver denial, duplicate/gap/trailing fragments, truncated EOF, deadline, cancellation, peer death, replay refusal and fresh retry. |
| `native_encoded_exchange_two_process`: `received_native_values_execute_after_sender_exit` | Distinct child PIDs, real sockets, complete independent values/types/validity/order, preserved physical encodings, three actual prepared projections and zero live credits after the last receiving owner drops. |
| `native_encoded_exchange_two_process`: `matched_arrow_flight_and_vortex_ipc_controls` | Same source, logical operations, security, resource and completion rules; complete wire/copy/decode/memory/CPU/workflow records for all controls. |
| `native_encoded_exchange_client_interop`: `ordinary_flight_sql_and_explicit_native_selection` | An unmodified independent Flight SQL client performs a real supported query and consumes standard Arrow results; the native client selects the extension; unsupported SQL/commands fail explicitly. |

Register these exact commands for the implementation boundary:

```sh
cargo test --locked -p shardloom-vortex --features release-user-surfaces,native-encoded-exchange-experiment --test native_encoded_exchange_protocol -- --nocapture --test-threads=1
SHARDLOOM_NATIVE_EXCHANGE_EVIDENCE="$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/native-encoded-exchange-two-process.json" cargo test --locked --release -p shardloom-vortex --features release-user-surfaces,native-encoded-exchange-experiment --test native_encoded_exchange_two_process -- --nocapture --test-threads=1
SHARDLOOM_NATIVE_EXCHANGE_EVIDENCE="$SHARDLOOM_ROADMAP_EVIDENCE_ROOT/native-encoded-exchange-client-interop.json" cargo test --locked --release -p shardloom-vortex --features release-user-surfaces,native-encoded-exchange-experiment --test native_encoded_exchange_client_interop -- --nocapture --test-threads=1
```

Use the existing sequential storage/process supervisor and local-only evidence
root. Copied native binaries must retain a name recognized by its concurrency
guard. Freeze complete argument vectors, target resolution, artifact hashes,
enabled features and child-process identity. A process pair within one supervised
experiment is intentional; unrelated native builds/UAT must be absent. All
owned children and sockets must be drained after each case. Keep every failed
case and unchanged threshold in the output.

## Comparison admission and retention

First freeze independently specified complete values and renamed schemas for
empty, tiny, nullable/plain, all-null, Constant, dictionary, RunEnd, BitPacked,
wide text and mixed/nested record cohorts within the admitted IPC registry.
Include sliced buffers, shared children, differing dictionary domains, multiple
transfer sizes and subsequent different operations. Freeze the actual fixture
hashes, grants, process limit, downstream queries, sample count/order and cost
thresholds before implementing or timing the candidate. An unsupported required
fixture is a failed admission, not a reason to remove that control afterward.

Compare the candidate with two explicit control families: optimized standard
Arrow/Flight and ordinary complete-message Vortex IPC through the same TLS and
authorization envelope. Arrow controls must preserve supported dictionaries and
other useful Arrow encodings, avoid incidental row-wise conversion, and record
their actual encoder options. The reviewed Flight builder hydrates dictionaries
by default; using that default alone cannot establish the optimized control.
Include supported IPC compression options or record a source-backed reason they
do not apply, before freezing the control. All representation conversion and
ordinary IPC assembly/validation costs belong to the comparison. No control
executes a query through an external engine.

Measure actual application wire bytes with envelope overhead, sender/receiver
CPU, allocations/copies, native coordinate and payload decode/materialization,
live/peak credits, process RSS, transfer/validation and complete downstream
workflow cost. Label application bytes separately from encrypted TCP bytes;
claim the latter only with actual transport measurement. Record connection
establishment separately and include it in fresh-connection workflows. Report
native query engine time independently; it is the only clock for routine query
timing updates. No performance claim follows from payload size alone.

Require complete correctness and lifecycle checks before timings, two independent
cohorts with every predeclared target/control passing, material encoded-transfer
benefit against optimized Arrow, and no material complete-workflow regression
against ordinary Vortex IPC or nonbenefiting controls. Exact numerical gates
belong in the immutable fixture protocol; this review does not manufacture them
from unmeasured workloads. A failed or inconclusive measurement leaves the
conditional experiment open or rejects only its frozen mechanism.

## Alternatives, risks and remaining gates

Reusing only same-process transfer would leave the transport question unanswered.
Placing native bytes in Arrow `data_body` would blur its wire contract. A custom
TLS stack would duplicate established security infrastructure. Always decoding
to Arrow would discard the native comparison. A mapped-memory or dictionary
cache optimization would add rights and identity obligations before establishing
the basic boundary. The selected finite extension avoids those additional
contracts, while accepting copies in the existing validated IPC receiver.

Important remaining risks are whole-message reconstruction cost, provider
allocations outside native credit, extension interoperability and the difference
between a local authenticated experiment and a deployable service. The isolated
dependency graph has not been compiled or integrated. The protocol schema,
adversarial test harness, independently frozen fixtures and matched encoder
options remain implementation prerequisites. None is satisfied by this document
or the passing local foundation tests. Continue with those prerequisites and the
registered two-process experiment; leave client, broader protocol/ownership and
platform/publication acceptance open until their actual evidence exists.
