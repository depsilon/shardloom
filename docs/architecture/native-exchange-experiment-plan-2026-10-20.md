<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native exchange experiment: fixture and measurement plan

Status: frozen pre-candidate plan. The independent fixture and codec checks and
complete prerequisite validation pass. The
[immutable freeze record](../benchmarks/evidence/native-exchange-control-2026-10-20/protocol-freeze.json)
binds this plan, the inputs and every control identity.
No network candidate or transport timing is supplied by this document. This
plan refines the [protocol prerequisite](native-encoded-exchange-protocol.md),
under the same RFC 0013/0035/0044 and NATIVE-ENCODED-EXCHANGE scope.

## Mechanism and controls

Use one feature-gated experimental executable with five explicit modes. All
modes start with the same source admitted by `from_vortex_ipc`, the same native
grant, mTLS, authorization, completion rules and later native consumer.

| Mode | Sender representation |
| --- | --- |
| `native_fragments` | Serialize with the pinned native `MessageEncoder`; carry ordered slices of its `Bytes` parts in bounded Protobuf segments without first assembling each complete message. |
| `native_assembled` | Use the same encoder, assemble each complete ordinary Vortex IPC message, then send bounded slices through the same native envelope. |
| `arrow_none` | Explicit preserving Vortex-to-Arrow target, Flight `DictionaryHandling::Resend`, IPC V5, no IPC compression. |
| `arrow_lz4` | The same preserving control with IPC LZ4 frame compression. |
| `arrow_zstd` | The same preserving control with IPC Zstd compression. |

The candidate avoids complete-message assembly at the sender. It does not add a
Vortex codec, eliminate Protobuf/TLS copies, stream the existing whole-source
native receiver, or share pointers across processes. The ordinary IPC control
uses the same native bytes and checked receiver. All three Arrow roles are
scored; there is no choice of a favorable codec after observing candidate scores.

The independent [fixture recipes](../../shardloom-vortex/tests/fixtures/native-exchange/README.md)
define 13 cases: empty, tiny, nullable plain, all NULL, constant, small/large/other
dictionary domains, run ends, bit packing, wide text, nested records/lists and
sliced shared children. Both dictionary domains exercise every code. NULL keys,
NULL dictionary values, empty text, embedded NUL/non-ASCII text and integers above
binary64's exact integer range are explicit. All three complete projections fit
the existing 65,536-row and 8-MiB JSON collection boundary.

Each receiver prepares three different native relational projections: all fields,
the first field, and the last then first fields renamed to `last_value` and
`first_value`. It executes them in order `0, 1, 2, 1`, after the sender has exited.
The repeated call executes again. Each complete result, schema, row count and
certificate must match its independent reference. Native physical encodings are
checked below ordinary chunk containers, rather than mistaking a container for
loss of its encoded children. All final native credits must be zero.

## Concrete wire and identity rules

The checked-in [Protobuf schema](../../shardloom-vortex/tests/fixtures/native-exchange/exchange-v1.proto)
is the source of field numbers and envelope alternatives. The implementation
will use original Rust declarations with pinned `prost-derive` 0.14.3; no copied
service implementation or unrecorded code generator is admitted. Tests must
verify the schema/declaration correspondence and golden bytes. Decoding an
extension message must reproduce its exact bytes on canonical re-encoding,
followed by explicit validation of all required values, enums, lengths and
alternatives. This rejects unknown fields, duplicate singular fields,
noncanonical order/encoding and invalid discriminants. Repeated segment fields
are intentional and remain bounded.

The first request contains only a CMD descriptor holding `Open`, followed by
clean request EOF. Its binding has a fresh server epoch and attempt ID (16 bytes
each), 32-byte content/schema/operation digests, principal/source/query identifiers
of at most 64 ASCII identifier characters, partition zero, first row zero, exact
source row count and policy version. Identifiers are lookup keys, never paths.
The authority maps the verified certificate fingerprint to permissions and the
frozen fixture/operation identity. Sender-provided hashes cannot grant access.

The operation digest covers canonical `Operations` bytes, including all aliases
and the execution order. The immutable source digest covers its two original
messages, each preceded by its unsigned 64-bit little-endian length. Native
completion covers the two ordinary provider messages in the same framing.
Arrow completion covers each standard IPC header and body in stream order,
each preceded by its unsigned 64-bit little-endian length. The receiver compares
these against the independent frozen authority, not merely against the sender.
The dtype digest is SHA-256 of the complete original dtype message. The exact
ordered physical registry is the checked receiver's existing 16 literal IDs.
`Complete.frames` includes the final completion-bearing frame; `messages` is two
for native transport and the number of real IPC frames for Arrow. Payload bytes
exclude the hash's length prefixes and all envelopes. ACK repeats these complete
payload totals. Native fragment message indices are zero (dtype) and one (array).
The 16-KiB control limit applies to the envelope after clearing native segment
payloads; the complete encoded message separately obeys the 2-MiB RPC ceiling.

Native response sequence zero is ACK, followed by exact ordered dtype/array
fragments and a final completion. Each fragment identifies its message, total
message length and exact next offset; nonempty segments total at most 1 MiB.
Arrow responses retain their standard header/body. Their first Schema message
carries ACK in application metadata; the last real message also carries
completion. Empty inputs use a Schema message carrying both. An extra metadata-only
Arrow message is forbidden because the pinned standard decoder requires an IPC
header. All frames bind the same authorized operation and exact sequence.

Completion is private until clean response EOF and successful RPC status.
The parent then waits for sender termination before releasing the receiver's
downstream execution barrier. Sender/receiver reports must prove distinct child
PIDs and actual loopback sockets. Truncation after a completion marker remains a
failure. No partial source is exposed, no durable write occurs, and no native
incompatibility selects an Arrow retry.

## Allocation admission for the finite Arrow control

Arrow Flight's maximum batch size is an approximate encoding target, not an
allocation ceiling. The decoder retains dictionaries, and compressed IPC buffers
carry a signed uncompressed-length prefix. The pinned LZ4 reader can grow past
that declaration before checking the returned length. Checking the prefix alone
therefore does not establish a decompression bound.

For this finite immutable-source experiment, freeze every standard Arrow frame's
header/body identity, order, lengths, row/node/buffer metadata and declared decoded
bytes after complete successful codec validation. The receiver verifies the exact
frame identity **before** calling the standard allocating decoder. It also checks
the verified IPC metadata, nonnegative bounded ranges, codec, field nodes, length
prefixes, complete stream totals and dictionary profile. Only the prevalidated
bytes are admitted. All fingerprint work is included in the comparison.

This admission is deliberately limited to the frozen sources and encoder options;
it does not certify arbitrary compressed Arrow input. Unknown or changed frames
fail before decompression. The native path likewise verifies complete authorized
message identity before its existing checked decoder. Neither path skips its
ordinary schema/coordinate validation after identity admission. A future generic
service needs its own bounded decompression and dictionary-state contract.

## Fixed resource and transport envelope

| Boundary | Refusal ceiling or fixed setting |
| --- | --- |
| Shared native grant | 256 MiB per peer, including application queues, owned payloads and declared temporary allowances |
| Application send queue | 4 MiB of encoded queued frames and at most four frames; credit is acquired before allocation and retained through the last owning buffer |
| Native fragment | 1 MiB total segment payload, at most 128 nonempty segments |
| RPC message / control metadata | 2 MiB encoded Protobuf / 16 KiB control bytes |
| Complete native pair / Arrow payload | 64 MiB each; Arrow total declared decoded buffers also at most 64 MiB |
| Arrow framing | At most 256 frames; 64-KiB IPC header; at most 4,096 nodes/buffers per message; at most 65,536 rows |
| Flight encoder | Approximate 1-MiB target, `Resend`, IPC V5; actual encoded message ceiling checked separately |
| Connections / attempts | Two accepted/live connections including TLS; one active data RPC per connection; two active attempts globally |
| HTTP/2 | Fixed 1-MiB stream and 2-MiB connection windows; adaptive windows disabled; 16-KiB header list and frame size |
| TLS | Explicit root and server name, mandatory client certificate, early data and key logging disabled; no gRPC compression |
| Deadlines | Five seconds for TLS handshake and idle progress; 60 seconds per attempt; 120 seconds per supervised workflow; five seconds to drain owned tasks on shutdown |
| Replay state | At most 256 terminal attempt IDs per server epoch; exhaustion refuses admission |
| Process limit | 2 GiB combined sender/receiver RSS; watchdog terminates a violating pair and rejects it; complete per-child peak RSS must also satisfy the conservative sum-of-peaks check |

Connection permits are acquired before accepting and retained through the TLS
and HTTP/2 connection lifetime. A per-handler concurrency limit alone is
insufficient. Attempt/queue credits outlive handler return when a response stream
or returned buffer remains live. Provider/TLS/runtime/kernel allocations are
reported separately; native credit and sampled RSS are not an OS-enforced total
memory guarantee. The fixed fixture authority limits the actual payload family.

The harness uses fresh local test certificates and private temporary keys, then
deletes keys after all owned processes terminate. Certificate fingerprints and
public parameters remain in the results. It proves unknown/missing/expired client
certificate rejection, wrong server name, unauthorized valid principals,
per-request source/operation/result/cancellation rights, policy replacement and
connection reuse. Replacement cancels affected attempts. Failed/completed IDs
cannot resume; fresh retries need new IDs and full transfer. Restart changes the
epoch and rejects all old tickets and attempts.

The fixture authority has two reader principals with the same finite source and
operation grants but separate result/cancellation ownership, one authenticated
principal with no data grants, and a separate administrator allowed only to
replace policy. A replacement requires the current epoch and exactly the next
policy version, cancels all old attempts, and invalidates their tickets. No
principal gains another reader's result through connection reuse. The complete
allowlist and fresh certificate fingerprints are bound into each run's private
configuration before either child starts; only public identities enter evidence.

## Predeclared measurement and retention rule

The exact freeze artifact will bind this document, schema, recipes, actual
inputs/references, native message identities, Arrow control identities, grants,
queries and commands before candidate implementation or timing. The eventual
executable's source/build/hash record is frozen after correctness validation and
before timed execution. A required unsupported fixture is a failed prerequisite;
it cannot be removed or hydrated after observation.

Run one unscored complete preflight for each of 13 cases and five modes. Then run
two separately launched cohorts, each with five samples per case/mode: 325
workflows per cohort, 650 scored workflows total. Every workflow uses fresh
sender and receiver processes and a fresh connection. Within each case, rotate
the five roles by `(case_index + sample_index) mod 5`; reverse role and case order
in cohort two. Each mode occupies each position once per cohort. Do not add
thermal sleeps, discard unfavorable samples or replace a cohort with a later
faster run. Storage/process or correctness failures invalidate the affected
cohort and remain recorded.

Record these clocks separately: source preparation, fresh connection, sender
encoding/assembly, transfer and receiving admission, the actual complete parent
workflow, downstream preparation, each real prepared query's `QueryTiming`, JSON
serialization and cleanup. The parent workflow runs from process launch until
both children have exited and complete results are verified; it includes all
source/connection/conversion/codec/downstream work. No sum of stage durations is
presented as elapsed workflow time. Routine timing communication uses native
engine time only, under the standing maintainer rule.

Record sender/receiver user/system CPU and peak RSS, native live/peak credits,
application payload/envelope bytes, complete-message assembly bytes, explicit
copy/materialization counters and provider coverage limits. Application wire
bytes include every request/response Protobuf message and its five-byte gRPC
message prefix, but exclude HTTP/2/TLS/TCP overhead. Unknown provider copy or
allocation counts remain unknown, never zero.

The target set is all NULL, constant, the three dictionary cases, run end,
bitpacked, nested and sliced/shared. The nonbenefiting controls are empty, tiny,
nullable plain and wide text. All 13 retain the ordinary native IPC comparison.
Use paired samples within each case/repetition and calculate cohort results
separately. Freeze the following gates without tuning them after candidate data:

1. Every complete value/type/order/encoding, authorization, failure, lifetime,
   cleanup and independent-client check passes. All required scores and resource
   records are present. Any missing proof blocks retention.
2. Against ordinary Vortex IPC, every case's paired median complete-workflow
   increase is no greater than the larger of 5% or 5 ms. The four nonbenefiting
   controls meet the same bound against **each** Arrow codec role.
3. Each case's sum of its four engine calls has no paired median increase greater
   than the larger of 5% or 0.5 ms against ordinary Vortex IPC. Query clocks are
   actual engine clocks; transport or process clocks cannot substitute for them.
4. Across all nine targets, the geometric mean of per-case paired median
   complete-workflow ratios is at most 0.90 against **each** Arrow codec role.
   A fixed-seed paired bootstrap (10,000 resamples, seed 3347) has a 95% upper
   ratio below 1.0. Report the complete target/control table, including failures.
5. At least two distinct predeclared encoded families—constants, dictionaries,
   run ends, bitpacking—contain a target whose paired median workflow improves
   by at least 10% against each Arrow role, together with either at least 10%
   lower application wire bytes or at least 10% lower combined peer CPU against
   each role. Dictionary-family membership includes the nested and shared cases.
   Whole-message assembly is absent in the candidate and present in its native
   control; those counters must prove that the intended mechanism ran.

For each paired median, compute candidate/control ratios (or elapsed differences
for the absolute allowance) at matching sample indices, then take the median of
the five pairs. The geometric mean uses the nine per-case median ratios with
equal case weight. Bootstrap each case's five paired sample indices with
replacement, independently within each case, recompute that geometric mean for
each of 10,000 draws, and take the sorted draw at index 9,499 as the upper bound.
Initialize Python's `random.Random(3347)` separately for each cohort/control
comparison. No target, role or sample may be omitted from a complete cohort.

Both cohorts must pass all gates. Any failed cost gate rejects this frozen
mechanism; unresolved noise, process limits or missing evidence is inconclusive,
not a retained improvement. No outcome implies general remote-service support,
distributed execution, package readiness or CG-5/CG-6 competitive superiority.

## Independent client and failure coverage

Use the unmodified pinned Rust `FlightSqlServiceClient` in a separate process.
It executes one exact predeclared SQL statement over the dictionary fixture,
retrieves its ordinary Flight ticket and consumes standard Arrow results. The
service binds that statement to a real native relational projection; unsupported
SQL fails explicitly. The native client separately selects the extension. This
proves that exact Rust client/service interaction, not ADBC or universal SQL
compatibility.

The registered protocol tests must include unknown/duplicate fields and enums,
profile/version/registry/schema/source/operation/attempt/partition mismatch,
extra request data, sequence gaps/duplicates/reordering, trailing or truncated
payloads, malformed native coordinates, wrong Arrow fingerprints, message/header
limits, receiver/queue denial, slow consumers, cancellation, deadlines, peer
death, replay, policy replacement and a clean fresh retry. Prove all refusal paths
release partial owners and drain owned tasks. Existing local checked-decoder
corruption tests remain required alongside actual network failure tests.
