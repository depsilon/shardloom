# Post-0.3.3 performance experiments

The maintainer authorized ship/drop testing after the 0.3.3 release train.
PR #1491 closed that train at `84fb2e169a7352ebacbfc2da9f89080633ca9e88`;
four selected package channels and the production website passed verification.
This is a finite follow-up under PERF-03/04/05/08/09/10/12, not a new implementation
phase or permission to reopen the 29 completed experiments unchanged.

## Starting evidence and order

The [September 30 profile](performance-profile-refresh-2026-09-30.md) records
53.171336 seconds across 43 queries, using each query's best of three complete
CLI calls. That runtime predates release dependency consolidation. The last
matched full-size ingest screen is September 27: 87.708583 to 81.104577 seconds,
with byte-identical 15,682,956,116-byte artifacts. Later unpaired observations
are separate evidence. The released-binary attribution below is instrumented
and is not a paired speed comparison.

Order prioritizes potential avoided work and breadth. These are hypotheses with
an admission step, not diagnosed bottlenecks or promised gains. After each
retained change, re-evaluate the remaining cost before the next dependent test.

| ID | Experiment and shared boundary | Admission evidence | Retain/drop decision |
| --- | --- | --- | --- |
| P033-1 | Avoid unused text pre-compression statistics through the existing Vortex `CompressingStrategy` configuration. | Both released-binary samples show the extra pass; the selected Zstd output discards these source statistics. File pruning statistics are accumulated separately. | **RETAIN:** initial best complete ingest time falls 8.456%. Final source includes the non-text passthrough correction and passes complete artifact equality, broad tests and Full43 acceptance below. |
| P033-2 | Avoid redundant text-buffer compaction before the existing Zstd provider gathers length-prefixed values. | Released-binary attribution records 0.010031 seconds over 22,876 calls, with identical input/output byte estimates. Upstream compaction already has cheap no-op checks. | **DROP at admission:** negligible observed work. Keep the existing compaction and its sparse/null/sliced/shared-buffer behavior; no runtime prototype. |
| P033-3 | Preserve string views across the Parquet-to-Vortex handoff using the existing reader schema hint and Arrow/Vortex adapters. | P033-1 still records 16.089–16.153 summed seconds in Arrow conversion and 4.912–4.919 seconds in text canonicalization. Pinned Parquet 58.3 and Vortex 0.85 support the same UTF-8 logical type through string views. | **RETAIN after one correction:** best complete ingest time falls 4.504% against P033-1, with lower observed RSS and byte-identical output. The original metadata-routing failure and invalid timing remain preserved. |
| P033-4 | Remove repeated decoding/materialization inside the shared native provider boundary, motivated by Q23. | Q23 is 4.444 s; its 4.205–4.625 s provider spans overlap accessor time. New samples show codec work without array identity proving duplicate execution. | **DROP at admission:** no specific reusable transition established. Existing progressive selection stays; no duplicate scanner or unproved cache. |
| P033-5 | Reuse source-backed dictionaries and bound metadata through compound grouping and exact union, motivated by Q19/Q6. | Q19 is 4.381 s and Q6 3.256 s. New source/stack inventory confirms the proposed source ownership, cached hashes, miss-only promotion and preunion are already present. | **DROP at admission:** existing mechanisms receive no duplicate credit; no further matching reuse boundary established. The slower triple-sort replacement stays dropped. |
| P033-6 | Reuse bound string-length/measure kernels in grouped accumulation, motivated by Q28. | Q28 is 2.279 s; caller-update spans are 2.182–2.214 s. Attribute repeated accessor work versus required aggregate updates. | Retain a shared measure-path improvement with exact aggregates, overflow/null behavior, HAVING and complete outputs. Avoid a query-number-specific route. |
| P033-7 | Adjust measured ownership/work admission in existing dictionary/count completion, motivated by Q29 and Q34/Q35. | Q29 is 4.826 s; Q34/Q35 approach 5 GiB RSS. Preserve Q35's original +13.75% observation and separate +4.36% best/+0.70% median follow-up. Attribute active work, waiting, reconciliation and live owners. | Change only an identified contention or lifetime boundary under the existing CPU grant and ordered jobs. More threads or a generic queue is not an admitted fix. Require complete results and report both local wins and regressions. |

P033-1/2/3 begin with one common full-size ingest attribution run; that run can
reject a weak hypothesis without building a prototype. A candidate may be revised
once when the evidence identifies a concrete correctable mechanism. Otherwise
remove it, preserve the evidence and move on. No broad codec, topology, PGO or
native-Python-binding sweep is authorized by this packet.

## First attribution run

- Release source: `e15f2e66faf6d359bba944e9d295fc58ce3bf7d4` (0.3.3).
- Frozen unstripped binary SHA-256:
  `5ebd38bea243a12cc2b6934fb226d536dff047ca76f85ee7db218bf847efc4e3`.
- Resident Parquet: 99,997,497 rows, 105 source columns, 14,779,976,446 bytes;
  SHA-256 `a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f`.
- Retained optimized Vortex: 15,682,956,116 bytes;
  SHA-256 `31cc61cfc347cf19a0328c196d59cd1eb431679311294cdc92263fef31062b35`.
- Use `scripts/run_clickbench_ingest_uat.sh`: Parquet input, 24 GiB admission
  policy, parallelism 4, 600-second deadline, 17 GiB artifact reservation,
  12 GiB free headroom, 100 GiB workspace and 256 MiB log limits. The admission
  policy is not a process RSS cap. Reuse the actual public prepare route.
- Record complete native-process wall/user/system CPU/RSS, existing scoped stage
  counters and two bounded 20-second all-thread stack samples. Elapsed stage
  spans overlap. Stack residence includes waiting and is not exclusive CPU time.
  Current Command Line Tools do not provide `xctrace`; do not invent CPU shares
  from these samples. Add more precise attribution only if the first evidence
  cannot decide a specific candidate.
- Hash input/reference before execution and record uncontrolled cache and accepted
  unrelated host activity. Exclude other native builds/tests/queries. Profiling
  overhead is included; this is not a paired speed comparison.
- Validate successful native certificates, all row-count checks and the complete
  persisted artifact. Byte equality to the retained artifact is a regression
  oracle, not a fresh independent SQL oracle. A changed artifact must be preserved
  and investigated with full value/schema/metadata comparison before acceptance.
- Save all receipts and raw stacks under
  `/Users/dylan/LocalData/shardloom/performance-candidates-20260930/`.
  Remove only the exact run-owned duplicate after verified equality; preserve
  the source, retained reference, release binaries and all evidence.

## Attribution result and first decisions

The September 30 released binary completed the guarded 99,997,497-row public
prepare call in 81.895060 seconds, including the two stack collectors. Native
CPU counters were 182.817570 user seconds and 10.238828 system seconds; peak RSS
was 3,047,440,384 bytes. All native row-count and no-fallback checks passed.
This is attribution evidence, not a speedup relative to the September 27 runs.

The initial driver's exact-file assertion failed and its failure receipt is
preserved. The new artifact is 373 bytes larger. A complete byte comparison
against the pinned Vortex 0.85 footer schema establishes:

- All 15,668,951,852 bytes before the footer are identical.
- DType (5,264 bytes), layout (11,428,296 bytes), file statistics (7,256 bytes)
  and segment directory/registries (2,563,280 bytes) are identical.
- The current public prepare route adds the 285-byte
  `shardloom.prepared-source.v1` provenance record. Its postscript adds 88 bytes
  and relocates those four footer sections by 285 bytes. This accounts for the
  entire difference; file versions are identical and there are no unexplained
  gaps or overlapping regions.

This proves unchanged persisted values and native data metadata by exact
representation equality; it is not a new independent decoded-value oracle.
The full new SHA-256 is
`5a6e7d7b521b891e522f32398ef2582367e814791ab537f354003ecd2f7a488d`.
The failed initial receipt, raw samples, bounded comparison script and separate
acceptance evidence are retained in the local packet directory above.

P033-1 is admitted to a bounded prototype. Both samples contain the provider's
`CompressingStrategy -> compute_all -> is_sorted -> varbin_to_canonical`
chain. Source inspection shows the selected text Zstd constructor creates new
array parts without inheriting those statistics. File pruning statistics are
computed upstream of the layout strategy; numeric and dictionary-code
compressors already suppress their extra pre-compression statistics. Use the
existing `with_stats` configuration, then verify unchanged complete artifacts,
file statistics, nulls, empty inputs and UTF-8 values before retaining it.

P033-2 is dropped as recorded above. P033-3 remains a separate candidate:
Arrow conversion recorded 16.820817 summed elapsed seconds and the large-source
Parquet reader still supplies offset strings. Reassess its remaining conversion
work after P033-1; removing duplicate conversions and changing the source view
handoff must not receive duplicate credit. Stage spans and nested sample counts
are not additive CPU shares.

## P033-1 initial screen and P033-3 admission

Four sequential complete public prepare calls ran in control/candidate/candidate/
control order with the same policy and no stack collectors. The frozen control
is the released binary above; candidate source is `1d3a6c68`, binary SHA-256
`9c34e3a4b9c3a7d8941bf22d093e4130574a4b0c757ef5783b9187bfff40227e`.

| Sample | Native wall seconds | User + system CPU seconds | Peak RSS bytes |
| --- | ---: | ---: | ---: |
| Control 1 | 78.945102 | 194.551153 | 2,938,880,000 |
| Candidate 1 | 72.269194 | 181.162550 | 2,909,929,472 |
| Candidate 2 | 72.294363 | 182.055482 | 2,903,441,408 |
| Control 2 | 81.144828 | 199.668139 | 2,909,732,864 |

Best complete time falls 8.456%; medians are 80.044965 and 72.281779 seconds.
Each output has all 99,997,497 rows and exactly matches the complete current
public prepare artifact's SHA-256 above, including provenance. Each owned
process group drained before the verified duplicate was removed. Raw logs,
per-call receipts and pre-removal identities remain in `text-stats-screen.json`
and its referenced local files. Input/output hashing is outside the native
clock; cache state is uncontrolled and unrelated host activity was accepted.
These are local ingest observations, not an official benchmark or superiority
claim. The executed driver's preflight failure handling has an audit limitation:
errors before manifest creation would lack a cohort receipt. This successful
cohort retains all four calls; the next driver must record preflight failures.

Review then identified the strategy's caller-selected non-text passthrough.
Unlike compressed UTF-8, a passed-through array persists its original source
statistics. A new regression test fails with absent sortedness and passes after
restoring the provider's full statistics only in that branch; it also checks
native values and file min/max/sum/null count. The frozen measurements above
predate that correction and are initial mechanism evidence. Final-source
acceptance must include the correction.

For P033-3, the remaining conversion spans justify testing the existing source
view handoff. Only top-level `Utf8`/`LargeUtf8` physical Arrow representations
change at the existing large-source threshold; field/schema metadata,
nullability, other types and small-source dictionary hints are preserved.
The `vortex::arrow` provider imports the views into native `VarBinView`; existing
intake copies and charges every referenced buffer. Tests cover dictionary/plain
Parquet pages, ordered serial/parallel reads, UTF-8/null/long values, sliced
multi-buffer views, last-owner credit release and partial-allocation denial.
Final complete artifacts and performance still determine retain/drop.

The first P033-3 candidate (`e89ab9e0`) failed the complete-artifact check.
Its native call completed in 67.161519 seconds, but the output contains 111
columns instead of the control's 112 (105 source columns plus seven derived
columns). The CLI selected metadata policy from the source status string and
recognized only the old plain-UTF-8 marker. The new view marker therefore lost
five lean text-derived columns and selected four additional time-derived columns.
That call is invalid as performance evidence; its 15,551,050,829-byte artifact,
raw output and failed receipt are preserved for diagnosis.

This concrete metadata-routing regression admits the packet's one candidate
revision. Recognize both physical string representations in the existing lean
metadata policy. The regression fixture compares all source and derived values
for offset strings and views, including null and non-ASCII values; it reproduces
the missing metadata before the correction. Repeat the frozen comparison with
new receipt names and the original complete artifact as the equality oracle.

## Retained ingest batch acceptance

The corrected source is `2f5a99fb31f1c54ae0cc7128c5a4a43b52d58e56`, with
frozen binary SHA-256
`8bec33a5eaf2eeff44598933ad42838f70b542e78689f29d7c039cda55f1e071`.
The control is P033-1's frozen binary, so this cohort measures the additional
string-view change. The non-text statistics correction is included; the public
prepare route selects UTF-8 fields for this dataset's text strategy.

| Sample | Native wall seconds | User + system CPU seconds | Peak RSS bytes |
| --- | ---: | ---: | ---: |
| Control 1 | 73.068434 | 183.115115 | 3,096,264,704 |
| Candidate 1 | 69.777290 | 174.417266 | 1,986,478,080 |
| Candidate 2 | 72.336568 | 181.089121 | 2,165,637,120 |
| Control 2 | 75.193775 | 190.675153 | 2,997,518,336 |

Best complete ingest time falls 4.504%; medians are 74.131105 and 71.056929
seconds (4.147% lower). Both candidate RSS observations are lower. All four
calls produce exactly the complete 15,682,956,489-byte public-prepared artifact,
including its 112 columns, pruning statistics and provenance. All owned process
groups drained and the four duplicates were removed only after identity checks.
Do not add or compound these results with the earlier 8.456% screen into a
measured overall gain: the cohorts have separate baselines and host observations.

The stage evidence retains a tradeoff. Summed Arrow-conversion spans rise from
16.439/17.808 to 20.425/21.663 seconds, while text canonicalization falls from
4.963/5.150 seconds to 0.000876/0.001267 seconds. The complete call and observed
RSS improve; Arrow conversion itself does not. These overlapping spans are not
exclusive CPU time. Numeric/text output byte counts and dictionary-preservation
calls are unchanged.

Final-source formatting and workspace Clippy passed, as did 3,436 workspace
tests, 2,048 native Vortex tests and 1,521 native CLI tests. The 22 pre-existing
ignored native tests remain ignored. Native all-target Clippy passed for the
affected Vortex and CLI code. Red/green fixtures cover discarded versus persisted
statistics, view-buffer ownership and the metadata-routing correction.

The released 0.3.3 binary and final candidate also completed Full43: all 258
complete outputs match the retained reference's canonical JSON SHA-256 exactly,
without float tolerance. All 43 archives and 1,032 raw members were verified.
The sum of query best times is effectively flat, 56.537951 versus 56.496367
seconds. Every sample and slower observation is retained. Q34's original best
time is 8.696% slower and all three original pairs are slower; one predeclared
reversed-order follow-up gives exact outputs for six more calls and is faster
in all three pairs (5.290% lower best, 5.406% lower median). That gap did not
repeat; neither cohort establishes a query speedup from this ingest change.
All 264 recorded native PIDs were absent at final acceptance.

The [portable evidence bundle](../benchmarks/evidence/post033-native-ingest-2026-09-30.json.xz)
is 952,136 bytes, SHA-256
`970ec474dab9f7012d01026ca21ec8facb9699d0add2f873ed2df4dee39c6b16`.
It contains all 11 completed ingest calls and their 99 raw log files, the original
failures and two stack samples, build/test receipts, frozen-source manifest and
patch, both query cohorts with 1,056 archived members, and 43 complete retained
reference logs. Local machine path prefixes are replaced; original byte hashes
and separate portable-text hashes are recorded. Complete query values are
verified unchanged by that replacement. Binaries and full data payloads remain
local; the bundle is regression evidence, not a fresh independent SQL oracle.
P033-1 and P033-3 passed review and all 40 remote checks. PR #1492 merged at
`b4f439d3fe44919deee860aa1dbfe27058ea00b4`; its tree matches the reviewed head.

## Query attribution and remaining admission

Seven sequential instrumented complete calls on that merged runtime reproduce
the retained complete Q23/Q19/Q6/Q28/Q29/Q34/Q35 values exactly. All 42 raw file
hashes, 2,420 cited stack lines and 21 exited native/supervisor/collector PIDs
were verified. `query-attribution-primary-acceptance.json` records acceptance.
Samples are stack residence, including waits, not exclusive CPU percentages or
a speed comparison. A bounded footer inventory confirms 817 chunks and 112
fields in the current artifact; flat text layouts do not establish array codec
identity or repeated decoding.

- **P033-4 DROP at admission.** Q23 shows real Zstd decompression and view
  construction in both provider predicate work and accessor canonicalization.
  The samples lack array identity and cannot establish an avoidable duplicate
  transition. Pinned Vortex 0.85 already propagates progressive selection; its
  exposed scan metrics report I/O/selectivity, not per-array decode reuse.
  Its Zstd reduction rules expose slice/cast adaptation, not a certified shared
  decoded-result cache. A replacement scanner or unproved cache is outside this
  hypothesis. This decision does not claim that all decode work is necessary.
- **P033-5 DROP at admission.** Existing `Utf8ChunkDictionary` already borrows
  duplicate keys, retains source-backed owners on first insertion, and caches
  hashes for growth. Global binding uses exact borrowed lookup and promotes
  ownership only on a new global key. Exact preunion/union is already retained.
  The observed construction/binding work does not identify a further reusable
  transition with unchanged key/epoch semantics; those existing wins receive
  no duplicate credit, and the dropped triple-sort replacement stays dropped.
- **P033-6 prototype admitted.** Q28 still visits numeric type/accessor dispatch
  from compact measure updates. Its URL length is already a native derived
  numeric column. Bind current native numeric owners and validity once per block,
  using the existing bound-measure pattern, while retaining measure order,
  null/overflow behavior, source-order admission and unbound native paths.
- **P033-7 prototype admitted for work admission.** Q34/Q35 execute the existing
  single-string complete-key partitions with nine count workers, 20 maximum
  outstanding chunks, and no pressure handoff/retry in these calls. Their
  canonical count loop validates every row before lookup, although 99,997,497
  rows produce 29,104,999 local partial entries. Validate a key before its first
  insertion and reuse that proof only after complete byte equality. Preserve
  dictionary/constant paths, invalid-input errors, leases, cancellation and
  partition ownership. Q29's bounded dictionary jobs retain their owners through
  consumption; no additional queue/thread tuning is admitted by these samples.

The provider decision for these two prototypes is `implement_shardloom_kernel`:
they update existing ShardLoom aggregate states from already admitted Vortex
primitive/VarBinView owners. Vortex provides those representations and decode
operations; it does not own ShardLoom's compact grouped state or complete-key
count partitions. No new decoding boundary, provider, scheduler, external
engine, or persistence format is introduced. Semantic tests and matched complete
query screens determine retain/drop, followed by broad acceptance for retained
code. P033-7's independent small prototype is screened before P033-6; its result
will not be counted toward Q28's measured gain.

## P033-7 count-completion screens

The frozen count prototype is `3148ad056e79304b248f3ec34f7ba6721e711116`;
the control remains the merged ingest runtime `2f5a99fb`. It changes only
validation placement inside the existing canonical count loop. Twelve focused
tests pass, including malformed first/later/outlined UTF-8 keys, duplicate keys
with hash-bucket collisions, dictionary/constant paths, cancellation, pressure,
worker counts and lease release.

| Cohort / query | Control seconds, runs 1/2/3 | Candidate seconds, runs 1/2/3 | Best-time reduction |
| --- | --- | --- | ---: |
| Initial Q34 | 3.671914 / 4.630470 / 3.411177 | 4.609571 / 3.789386 / 3.324016 | 2.555% |
| Initial Q35 | 3.697170 / 3.349759 / 3.717516 | 3.808414 / 3.656863 / 3.481641 | -3.937% |
| Reversed Q34 | 3.162290 / 3.131932 / 3.204449 | 3.328272 / 3.100246 / 2.927497 | 6.527% |
| Reversed Q35 | 3.543138 / 3.486391 / 3.293819 | 2.981946 / 2.950431 / 3.172416 | 10.425% |

The first cohort has mixed complete wall time: Q34's median is slower and Q35's
best is slower. One reversed-order follow-up was declared before execution.
Its medians are lower for both queries, with one slower Q34 pair still retained.
Total user+system CPU is lower in all twelve matched pairs across both cohorts;
the count-work spans also fall, while partial entries remain 29,104,999 per call.
All 24 complete returned values match the retained reference exactly, all 96
archived members pass hash verification, and all native PIDs have exited.

RSS is not improved consistently: the reversed cohort's candidate peaks reach
5,112,643,584 bytes versus 5,032,443,904 for its controls across both queries.
The change leaves the existing memory admission, worker count and queue window
intact; faster local counting can change how simultaneous owners overlap.
That explanation is an inference, not measured allocation attribution. Retain
the small shared change for broad acceptance based on avoided validation,
repeated CPU reduction and the complete follow-up calls, without a memory-win
or uniform wall-time claim. Both cohorts and every negative observation remain
in `query-count-screen-1/2` receipts and strict analyses.

P033-6's prototype binds numeric types and validity at block entry, reusing
the ordinary bound-measure implementation. Compact COUNT and SUM/AVG preserve
their own error ordering: a non-finite sum is rejected after its count and sum
update, while COUNT of a non-null NaN remains valid. Six new semantic tests and
the nine existing ordinary-measure tests pass, including all primitive widths,
nulls, empty/rebound blocks, sparse/repeated row selections, overflow, measure
order beyond four inline states, source-order limits and HAVING. Performance
acceptance remains pending.

After the ingest merge and portable evidence acceptance, the diagnosed first
P033-3 payload was retired. Its full SHA-256 and file generation matched the
failed-call receipt before removal; all five protected source/reference/control
generations remained unchanged. `failed-ingest-payload-cleanup.json` records
15,551,053,824 allocated bytes removed. The original failed receipt and raw logs
remain in the immutable ingest bundle; recreating that obsolete layout requires
its recorded source and configuration.

## Comparison and completion gates

Freeze candidate/control revisions, binary hashes, workload, configuration and
retention rule before each timed comparison. Use matched sequential calls with
reversed order and the same fastest-valid-run rule on both sides; retain every
sample, complete output, negative observation and failed run. For initial ingest
screening use two calls per side; do not include the instrumented attribution run
as a timing control. Useful 4–9% gains remain eligible; there is no blanket
one-second rejection threshold. Smaller positive results require enough mechanism
and repeated-call evidence to justify maintenance cost. Memory or storage wins
must report their ingest/query lifecycle tradeoffs explicitly.

Run focused semantic/ownership/cancellation tests before performance screens.
Retained runtime changes require the repository's fmt/clippy/workspace tests,
applicable native/public-surface tests and complete Full43 UAT at the end of a
cohesive batch. Persisted-layout changes also require complete data/schema/
statistics verification and first/repeated-query acceptance. Drop failed
prototypes with their evidence; successful changes receive a coherent reviewed PR.

The Vortex-first decision for the initial writer hypotheses is
`use_vortex_native_provider`: pinned Vortex 0.85 `CompressingStrategy`,
`VarBinViewArray`, statistics and Zstd APIs inside the existing feature-gated
`shardloom-vortex` writer. No provider replacement, query-engine integration or
new encoding abstraction is proposed. Keep the public route's Native I/O and
execution certificates, representation boundaries, `fallback_attempted=false`
and `external_engine_invoked=false` intact.

PulseWeave/capillary work and dynamic admission matter at ownership and CPU/memory
boundaries; this packet does not create another scheduler. Metadata-first behavior
must survive any removed traversal. Timing surfaces and evidence tiers remain
separate. Broader spill/recovery, serving, all-I/O physical-layout policy and
CG-1 through CG-23 retain their own obligations. The interrupted format pulse
and large CSV/JSON/JSONL performance tests remain paused. No new publication
train is implied by this packet.
