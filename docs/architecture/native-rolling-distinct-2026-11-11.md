# Bounded rolling COUNT DISTINCT investigation

Status: first fixed candidate rejected, November 11, 2026. The accepted runtime
and CI policy are restored byte-for-byte. Portable replay and all 23 corruption
controls pass. Hosted closeout remains pending; no candidate runtime change is retained.

This closes the first recommendation 10 experiment in the
[shared-pipeline investigations](native-shared-pipeline-investigations-2026-11-10.md),
within item 20 and RFC 0044. It does not complete that item, the broader RFC or
CG-1 through CG-23. Investigation A is next under the
[maintained phase plan](phased-execution-plan.md#planned).

## Fixed decision and complete cohort

The accepted base is `591fdb42f6ffcd5e9003b4a5c130324d7a34a62c`, the merge of
compatible-window PR #1679. Its 58 PR checks, 55 exact-main checks, clear review,
verified retirement and post-retirement replay passed before this experiment.
Its ordinary release runtime remains the accepted baseline.

The protocol was fixed before candidate implementation and timing. It selects
218 original finite-ROWS window requests, 218 arithmetic compositions, 218
moments compositions and 436 new COUNT DISTINCT requests. The new requests use
either the original argument or a unique row ID, retaining the original frame,
partition, order, population, public surface, format and output limit. The five
public surfaces are CLI SQL, Python SQL, typed relations, DataFrame and embedded
workflows; Vortex, Parquet and Arrow IPC input routes are represented.

The baseline reuses 654 exact accepted observations, including complete requests,
results, schemas, references, certificates, runtime identities and engine clocks.
Each of the 436 missing baseline requests ran once. Each candidate request then
ran once in isolation, with all other task work and agents idle. There were no
warmups, repeats, profiling, cache manipulation or timing-based selection changes.

| Family | Cases | Baseline engine seconds | Candidate engine seconds |
| --- | ---: | ---: | ---: |
| Original windows | 218 | 60.624434461 | 61.844093292 |
| Arithmetic compositions | 218 | 72.880150194 | 73.823331366 |
| Moments compositions | 218 | 62.575584459 | 63.658341259 |
| DISTINCT, original argument | 218 | 55.836157794 | 55.966344597 |
| DISTINCT, unique row ID | 218 | 58.773976132 | 56.122255216 |
| Complete selected cohort | 1,090 | 310.690303040 | 311.414365730 |

All 1,090 baseline and 1,090 candidate results pass independent saved-result
replay, including complete values, schemas, certificates and valid engine clocks.
The candidate's complete engine sum is **0.724062690 seconds higher (0.233049658%)**.
The frozen binary rule therefore drops it. The unique-row-ID family improves,
but the other four family sums rise; all remain in the denominator. These
single-pass observations do not establish a causal regression in unchanged
controls or statistical confidence. They do determine the predeclared outcome.
The separate allocated-storage result below does not override this rule.

The [portable evidence packet](../benchmarks/evidence/native-rolling-distinct-2026-11-11/README.md)
retains the exact protocols, reference data, all saved results, diagnostic
observations, source snapshots and failures. Replaying it executes no query.
This is not a new complete 10k or Full43 result, and it does not repair the
original 10k observation's 96 failures. No isolated saving is added to the
independent shared-index or compatible-window results.

## Rejected candidate and provider boundary

The candidate admitted only resident COUNT DISTINCT with a bound argument and
finite ROWS bounds: CurrentRow or preceding/following Count offsets. It retained
the existing filter, null, hash and exact native key comparison. GROUPS, RANGE,
calendar and unbounded frames, additive DISTINCT and stored/spilled evaluation
kept their existing owner. The ordinary frame traversal was mechanically shared;
the general State layout and compatible-window family were unchanged.

One private power-of-two bucket vector stored a full hash, representative native
row position and occurrence count. Final departure removed the entry and closed
the probe-chain hole with backward shifting. It retained no tombstones or stable
entry IDs. Representatives remained valid through the immutable resident input
owner even after leaving the frame. All departures across the three disjoint
exclusion intervals preceded arrivals. Replacement allocations were separately
credited while the previous allocation remained live. Cancellation and errors
discarded the partial state and refunded its credits; resumability was not added.

The provider decision was `implement_shardloom_kernel` inside the existing frame
owner. Pinned Vortex 0.93.0 aggregate accumulators do not provide this moving
membership/removal/exclusion and ShardLoom credit contract. Its
`all_non_distinct` array equality is not a rolling SQL COUNT DISTINCT state.
The candidate reused the admitted native input and exact key operations. It
added no dependency, external engine, Arrow executor, source/sink boundary or
decode/materialization shortcut. Native output and both no-fallback certificate
fields remained unchanged.

## Allocated-storage proof and limits

For maximum non-null distinct membership M among completed frames, the candidate
retained at most `max(8, 4*M)` buckets, with at most `max(8, 6*M)` buckets during
replacement overlap. M=0 allocated no bucket backing. Each bucket occupied 24
bytes; a separately credited fixed state occupied 112 bytes. The bound covers
this auxiliary owner, excluding resident input, peer arrays, output and whole
query memory. It does not promise that retained capacity shrinks when M falls.

The 216-case allocation matrix checks 257/4,097/65,537 rows, widths 1/8/65,
unique/repeated/reappearing values, filters and all four exclusions. Independent
sets check every one of 5,032,152 output values and final membership; allocation
credits refund on completion. For 65,537 unique rows and width eight, both
implementations have M=8. The baseline retains 65,537 historical entries and
131,072 count slots, with 6,555,335 retained and 7,734,990 peak credited bytes.
The candidate retains 16 buckets, with 496 retained and 688 peak credited bytes.
These diagnostic allocations are distinct from query throughput and are not
accepted runtime behavior because the candidate was dropped.

Ordinary tests cover 1,152 small-frame combinations, full-hash and low-bit
collisions, wrapped probe chains, rehash/deletion/reappearance, departed
representatives, denied overlap allocation, cancellation, refunds and retry.
Broken-chain and historical-capacity controls demonstrate failure detection.
Public relational tests cover binary, decimal, date, timestamp, nested and
dictionary-text keys, filters, directions and every exclusion through resident
and stored evaluation. Following-frame cases preserve lazy invalid values.

## Validation, restoration and retention

The exact candidate source passed formatting, strict default/native workspace
lint, 3,323 default tests with no skips, and 7,101 native tests with 40 named
diagnostics intentionally ignored. The allocation diagnostic ran explicitly.
Actual CI discovery found 4,144 native tests and 36 registered ignored tests,
with all nine new ordinary tests included in the four partitions. Nineteen
corruptions of diagnostic evidence were rejected. The fresh ordinary release
build and all 1,090 public requests used this same source fingerprint:
`85ff2f22e81409581abb4d6765300428f888cd507a9c7b45010231274e83009d`.

The original client preflight failure, malformed new-test interval, strict-lint
failure, exposure-audit failure, two archive preflight failures and two
duplicate-work preflight refusals remain
recorded with corrections or dispositions. No failed scored request was retried.
The refusals launched no additional test, build or query process.

Archive preflight first exposed the restored CI policy at a path whose frozen
proof correctly required the candidate version, then two inherited client
modules missing from the portable collection. The archive now binds the exact
candidate snapshot, proves current accepted-source restoration, and includes
both clients with predecessor-protocol identities and unchanged original file
generations. Complete source-bound and relocated replay pass; all 23 deliberate
corruptions are rejected. Five failed check receipts and both refusals remain
intact. No query was executed during packaging, replay or corruption controls.

After the decision, the five modified runtime/test/CI files were restored from
the exact accepted base, and the two candidate-only modules were removed after
verifying their retained source snapshots. The candidate's test additions,
debug counter, ignored-test registration and dispatch helper are not shipped.
The accepted compatible-window runtime remains current. No post-score tuning
or rescoring was performed.

During preparation, verified retirement removed 10,162 reproducible debug files
among 11,933 entries after completed tests, lint and actual CI discovery.
Repeated identities and closed-handle checks preserved current inputs, accepted
runtimes and the release cache. No storage floor changed and no payload backup
was created. Final retirement of the rejected runtime and temporary evidence
copies follows portable replay and hosted closeout; compact results and exact
candidate sources remain retained.

A materially different future candidate requires its own attribution and frozen
protocol. This first result does not authorize another trial, a resource-only
exception, a broader frame owner or a full10k/Full43 rerun.
