# Native input growth under shared admission

This unit replaces fixed cumulative input, top-level schema-width and generated
range ceilings with admission of their native owners. Finite producers can
supply more batches, wide records can retain more fields, and compact Int64
ranges can exceed the former total-length limit. Actual retained payloads,
growing containers and schema metadata use the query's shared memory grant.

The change belongs to PERF-03/06/07/10/11/12. It preserves the existing native
Vortex execution and output contracts. Published 0.5.1 artifacts retain their
release-time behavior; no version change or package publication accompanies
this work.

The [design](../architecture/native-input-growth-2026-10-09.md),
[evidence index](evidence/native-input-growth-2026-10-09.json),
[portable packet](evidence/native-input-growth-2026-10-09.json.xz) and
[independent inspection](evidence/native-input-growth-2026-10-09-inspection.json)
bind the complete source and result evidence. Hosted integration is complete in
[PR #1539](https://github.com/depsilon/shardloom/pull/1539); its
[receipt](evidence/native-input-growth-hosted-2026-10-09.json) binds the merge
and actual production verification.

## Ownership and native providers

Pinned Vortex 0.85.0 supplies `ChunkedArray` for resident batch composition and
`Sequence` for compact generated-range metadata. The scanner constructs only
the requested admitted interval. Signed endpoints, intermediate arithmetic,
total length and logical bytes remain checked. No new dependency or external
execution provider is introduced.

Growing containers reserve their replacement overlap before allocation. Empty
batches validate their declared schema without accumulating an unbounded list
of empty arrays. Copied and transferred payloads keep their capacity and schema
credits through child aliases. Whole records and nested value columns have
separate metadata admission; sorting includes retained record metadata in its
spill decision. Wide native writers admit schema serialization workspace before
creating their footer builders.

The finite input contract remains completion-aware: release the previous native
input before asking for another; validate every frame and the explicit end;
drain input even when a result limit is zero; report success only after complete
consumption, source checks and output completion. Incoming frame, JSON and typed
conversion workspace use the same query pool.

These are native reservations with conservative metadata allowances. They do
not measure all allocator/provider overhead, caller-owned Python objects or
total process RSS. Per-frame limits and bounded collection remain explicit.

## Complete growth and pressure workloads

The frozen public growth family contains 20 cases. Its wide fixture has 4,097
fields across the four input domains, reversed output order, NULLs, Int64
endpoints and escaped Unicode text. Row, resident-batch and finite-streaming
sources exercise complete collection and incremental delivery. Row/resident
sources use all eight existing writers; finite streaming uses its admitted
native Vortex destination. Every requested field and value is compared.

Cumulative controls supply 4,099 nonempty or 8,193 empty batches in both resident
and finite-streaming modes, through collection and native write/reopen. The
range fixture writes 1,000,017 descending Int64 values, checks every reopened
value and also checks complete COUNT/SUM/MIN/MAX results. A late arithmetic
failure after 4,100 batches must still fail behind a result limit and remove
the owned output. Cancellation after 4,097 batches must close the producer
without reporting successful completion.

All 20 cases pass on the corrected executable. The wide cases retain 17 writer
artifacts across their admitted source/destination combinations. The complete
range produces a 40,193,232-byte Vortex artifact; its original bounded readback
compares every value, with the ordered Int64-byte digest
`d12272e02888b66d071d1cb7fa7780597628a6a966749dc9d5fe945db00dacf2`.
Every cumulative producer opens once, reaches its explicit end and closes.
The late-failure case removes its owned output; cancellation closes the producer
without a successful-completion report.

Native plan checks exercise 255-node trees beyond the former operator count:
one repeats a resident source through UNION ALL and compares all 384 result
rows; another joins ordinary sources with exactly one finite producer. A
128-KiB binding control must deny metadata growth and refund its reservations.
These checks do not remove the separate recursive depth guards.

Five fresh input-pressure controls each declare 1,152 batches representing
4,851,019,008 logical input bytes. They require complete streaming results under 1 GiB and
6 GiB grants, resident denial at 1 GiB, resident completion at 6 GiB, and a
1-GiB streaming native Vortex write with complete bounded readback. Every
producer must close; the constrained resident control must stop before consuming
the complete source. These are admission/correctness controls, with no speed
comparison or claim that a native grant bounds total process RSS.

Both streaming iteration controls return all 1,152 expected rows and peak at
21,324,175 reserved bytes. Resident intake denies after 251 batches under 1 GiB
and closes the producer; under 6 GiB it consumes all 1,152 batches, returns every
expected value and peaks at 4,882,685,031 reserved bytes. Streaming native output
peaks at 30,952,427 reserved bytes and produces a 5,249,964-byte Vortex file whose
complete bounded readback matches all 1,152 expected rows. All five producers
close, and successful controls observe the end of input.

Two focused native pivot controls also preserve the existing spill contract.
Each compares every value of 6,145 output rows from 24,580 input rows. The
19,342,360-byte input exceeds the 16-MiB grant; resident execution denies that
grant, while both spill runs peak at 15,669,949 reserved bytes. Each writes
775 runs, performs 387 merge passes and completes owned cleanup. The ample
resident control peaks at 291,402,941 reserved bytes. These observations retain
an existing native pressure family; they do not admit dynamic one-shot input.

## Corrections retained with their failures

The first wide-record check exposed footer workspace that did not grow with
schema width. The correction reserves that workspace before writer creation.
Primitive-root inputs and aliases remain part of the native writer regression.

The first broad public run then exposed table-schema admission repeated for
each retained nested pivot value. A 65,537-row nested pivot exhausted its 1 GiB
grant before reaching the expected collection row-limit denial. The correction
separates whole-record and nested-value metadata ownership, preserving both the
grant and the expected collection boundary.

The following native suite exposed a spill estimate that counted payload and
keys but omitted compact-record schema metadata. The 133,137-group pressure
case exhausted its unchanged 16 MiB grant. A smaller 1,025-batch regression also
failed before the correction under 8 MiB. Including record metadata in the spill
estimate makes that regression pass and allows both DISTINCT and grouped
aggregation to complete under the original 16 MiB grant, with every value,
ordering, ownership release and owned-run cleanup checked.

Three duplicate-field diagnostic expectations also required updated wording in
two pivot harness modules. Result oracles and collection/resource limits remain
unchanged. The prior failed runs, diagnostic probes, source patches and receipts
are retained. The corrected candidate requires every engine and public stage to
run freshly; prior successful partial stages provide no current acceptance credit.

## Verification and provenance

The clean source commit is
`61a813b71d871e0e7213dcc3740f77c900f50eba`, with 1,020 frozen runtime,
test, harness, manifest and vendored assets. Their manifest identity is
`326b8e4ac0c371d98149b4e92747ce21c3e4ac0321ef1ea8491cfd631d4e2d1b`.
The macOS arm64 release executable uses Rust 1.99.0 and
`release-user-surfaces`; its SHA-256 is
`8a4ef90fe0925dbbdedaf5d3a7c1d4e582671de59d823d6b8fa4163fc244b607`.
The corrected candidate changes fourteen of the predecessor's source assets;
the other 1,006 remain byte-identical. Every required source and workflow stage
runs freshly against the corrected candidate.

| Validation | Result on the corrected source |
| --- | --- |
| Source/build feature, MSRV, formatting, lint and test gates | All 15 pass |
| Default workspace tests | 3,147 pass |
| Native library tests | 2,513 pass; 24 ignored |
| Native CLI / example tests | 1,321 / 17 pass |
| Optional Python tests | 620 pass |
| New growth workflows | 20 pass |
| Retained streaming/operator/protocol workflows | 442 pass |
| Input-pressure controls | All five pass, including the expected constrained resident denial |
| Full ordinary public regression | 32,497 cases; 18,595,284 complete rows compared |
| Direct unary regression | 202 cases; 131,734 complete rows compared |
| Resident batch / format fidelity adapters | 48 / 19 pass |
| Admitted semantics / golden workflow stages | 145 / 9 pass |
| Focused native pivot pressure | Two complete 16-MiB spill observations pass |
| Independent inspector contract | 311 positive/negative checks pass |
| Full43 retained-input regression | All 129 complete results pass |

Suites overlap; their counts must not be added as independent workloads.
Required workspace formatting, lint and tests run alongside the native feature,
MSRV, example, CLI and optional Python checks. Cargo manifests, lockfile and
vendored providers are unchanged. Complete public families use preexisting
literal oracles and independently checked result values.
The 24 ignored native tests remain explicit manual benchmark, attribution,
external-fixture or fixture-regeneration helpers. This unit does not claim new
results for those separately gated experiments.

Full43 executes all 43 queries three times against the retained
15,682,956,489-byte Vortex input and preexisting complete-result references.
The configured grant is 24 GiB with maximum parallelism 12; the observed host
has 16 GiB physical RAM and ten logical CPUs. The sum of per-query minima is
72.207259460 seconds, the sum of medians is 73.851977832 seconds, and all 129
native calls total 222.519547002 seconds. The complete guarded stage takes
256.680166959 seconds. Maximum observed native-process RSS is 5,134,401,536 bytes.
These are retained-input correctness-regression observations, with source
prehashing and uncontrolled OS cache and ordinary host activity. They include
no fresh ingest, paired performance comparison or enforced process-RSS claim.

The compressed packet contains 54,856,828 bytes with SHA-256
`1f320c2735b7e9c9ff6d44dc881cb6a9b1fcb3a03dc3bcb6159843a44e93e69c`.
Its complete decompressed JSON contains 4,639,818,207 bytes with SHA-256
`9a8d69df4998b994273f5ca9f679f0996ceecf292c0c7e27df0987dcd2cb8b75`.
Finalization reconstructs and compares all saved value files, checks retained
artifact hashes and reopens historical archives before serialization, then
checks the compressed and decompressed identities. The range's every-value
comparison belongs to its original bounded native readback; finalization
independently reconstructs its expected ordered-value digest.

Independent streaming inspection verifies the complete decompressed identity and
all 125 expected coverage fields, including raw execution reports, the 32,497
public cases, complete growth/pressure evidence and all 129 Full43 routes.
All current check-failure, invalid-status, unsupported-claim and fallback
violation counters are zero.
The inspector and verifier are themselves bound to the 311 positive/negative
contract checks and their recorded hashes. Both finalization and inspection
finish within their guarded deadlines with their process groups drained.

The predecessor's Full43 preflight stopped before queries at the unchanged
252-MiB log-admission threshold. Two completed historical cohorts were
compacted with original identities and every JSON/companion byte checked before
redundant containers or loose files were removed. This recovered 4,546,560
accounted log bytes. Finalization independently reopens all 1,032 members.
Failed and incomplete observations, resident inputs and storage ceilings remain
unchanged. The corrected candidate's fresh Full43 preflight and full run pass.

The packet also retains the original file-only control-buffer expectation
failure, diagnostic wording corrections and all three runtime regression
observations described above. A stale source-manifest reference in the site
checker was corrected before its first execution; its original and corrected
drivers are retained. No historical partial run is combined with the corrected
candidate, and no failed observation receives acceptance credit.

## Documentation acceptance

All six support checks pass, including ten generated-site steps, followed by
three complete native executions of the published resident and streaming
examples. Contribution governance also passes. The rendered Python, resource
and limitation guides are checked at desktop and mobile widths, in dark and
light themes, with working search and mobile navigation. No browser console
warnings or errors are observed. The temporary preview is closed and its process
group drained. The evidence index binds the support receipt, final documentation
hashes and a separately reopened archive containing the checks and screenshots.
All 1,020 accepted runtime assets and the executable remain byte-identical.

## Hosted integration

PR #1539 merged at `7d8230b2e69c23073c329ff07d75e3e5658decdd` after all 39
checks passed on `bdb6ec334e8a790734f9b1964ddaae7d6254d3df`, including CodeQL.
The accepted head and merge trees are identical. Primary source review found
no blocking findings. Hosted automated review was unavailable because the
account review quota was exhausted; no independent review or submitted approval
is inferred.

The [hosted receipt](evidence/native-input-growth-hosted-2026-10-09.json)
retains 19 records and eight screenshots, including the successful preview and
production deployments. Actual desktop/mobile Python, resource and limitation
guide text and growth links match between preview and production. Search for
`cumulative` reaches the batch-consumption section. The public acceptance link
opens the merged report, whose GitHub bytes match the accepted source. Temporary
tabs are closed and viewport overrides are reset. No package publication or
version change accompanies integration.

## Remaining implementation

Richer decimal, binary, temporal and nested intake; repeated use of a streaming
source within one plan; multiple streaming producers; safe dynamic-schema input;
streaming compatibility destinations and
fanout; cancellable automatic preparation; and scalable deep traversal remain
with their existing phase owners. The current public input domains are nullable
Int64, finite Float64, Boolean and UTF8. One finite streaming producer is used
once through its admitted operators and terminals.

Nested value-schema bounds, recursive plan/expression depth, SQL frontend limits
and operator-specific limits are unchanged. Per-frame input remains 2,048 rows
and 8 MiB, schema declarations retain a separate 8 MiB envelope, and complete
collection retains its 65,536-row and 8 MiB bounds. These are remaining
implementation and transport contracts, not a declaration that the broader
workflow task is complete.

All six remaining areas, eight conditional investigations and CG-1 through
CG-23 remain visible. This acceptance does not establish comparative speed,
general execution recovery, distributed execution, production certification or
total process memory enforcement.
