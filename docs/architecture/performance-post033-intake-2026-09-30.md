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
| P033-1 | Avoid unused text pre-compression statistics through the existing Vortex `CompressingStrategy` configuration. | Both released-binary samples show the extra pass; the selected Zstd output discards these source statistics. File pruning statistics are accumulated separately. | **Retain for final acceptance:** the initial four-call screen lowers best complete ingest time by 8.456% with byte-identical output. A subsequent non-text passthrough correction preserves the provider's original statistics; final-source acceptance remains required. |
| P033-2 | Avoid redundant text-buffer compaction before the existing Zstd provider gathers length-prefixed values. | Released-binary attribution records 0.010031 seconds over 22,876 calls, with identical input/output byte estimates. Upstream compaction already has cheap no-op checks. | **DROP at admission:** negligible observed work. Keep the existing compaction and its sparse/null/sliced/shared-buffer behavior; no runtime prototype. |
| P033-3 | Preserve string views across the Parquet-to-Vortex handoff using the existing reader schema hint and Arrow/Vortex adapters. | P033-1 still records 16.089–16.153 summed seconds in Arrow conversion and 4.912–4.919 seconds in text canonicalization. Pinned Parquet 58.3 and Vortex 0.85 support the same UTF-8 logical type through string views. | **Prototype admitted:** request `Utf8View` for large-source offset strings, retaining the existing dictionary policy and copied-buffer admission. Measure against P033-1; the dropped source-dictionary variants stay dropped. |
| P033-4 | Remove repeated decoding/materialization inside the shared native provider boundary, motivated by Q23. | Q23 is 4.444 s; its 4.205–4.625 s provider spans overlap accessor time. Obtain stack/counter attribution to a specific avoidable transition. | No duplicate scanner: provider selection propagation and conjunction ordering already exist. Retain only an exact, shared improvement to the observed transition with complete Q23 and regression results. |
| P033-5 | Reuse source-backed dictionaries and bound metadata through compound grouping and exact union, motivated by Q19/Q6. | Q19 is 4.381 s and Q6 3.256 s. Separate dictionary construction, binding and union work; verify full key/epoch ownership and budget lifetimes. | Extend existing builders/owned jobs only when semantics match. Existing preunion gets no duplicate credit; the slower triple-sort replacement stays dropped. |
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
- Resident Parquet: 99,997,497 rows, 112 columns, 14,779,976,446 bytes;
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
