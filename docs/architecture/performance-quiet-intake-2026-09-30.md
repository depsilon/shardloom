# Source review of the quiet-workstation performance proposals

Status: **admitted; experiments in progress**. The maintainer approved execution
of the complete batch after the source review. This assessment checks
the maintainer-supplied September 30 review against main
`6bcec8e61acf6ca269e6f7a2789537797dbb3e6b`, the frozen measured executable and
the original query evidence. It proposes a finite continuation under existing
PERF-03/04/05/08/09/10/12 owners. It does not reopen the completed seven-candidate
packet or admit a new implementation phase. The source-review evidence retains
its original pre-experiment status and hashes; new run receipts record execution.

## Execution admission

The finite batch follows the order below. Ingest uses six complete calls in
P4/P6/P8/P8/P6/P4 order, two per configuration, on the unchanged frozen runtime.
The best valid call per grant is the comparison statistic; every call, median,
CPU/RSS observation and actual owner topology is retained. A useful gain below a
historical numerical target is eligible for retention when exact output,
repeatability, implementation cost and resource tradeoffs support it.

Each query prototype receives three calls per role and target using the existing
paired runner's alternating order at both the pair and query boundaries. Odd
query IDs start control/candidate; even IDs start candidate/control. Targets are
Q26/Q27 for borrowed sort reads, Q28 first for the compact numeric kernel,
Q6 for scalar DISTINCT, and Q34/Q35 for each single-string directory screen.
Q17 and renamed/null/skew/collision fixtures enter acceptance where shared
code is affected. Freeze each pair's sources before building/timing, and keep
the other implementation unchanged. Report fastest valid and median timings,
CPU, RSS, complete exact results and relevant work counters for both roles.

A correctness failure stops that candidate for diagnosis. One bounded correction
or reversed-order follow-up is allowed when evidence identifies a concrete cause
or a noisy regression. Avoid repeated tuning to obtain a favorable sample. Drop
absent or unprofitable mechanisms with their evidence retained. The final
retained composition receives complete paired Full43, workspace/native-feature
checks, adversarial review and a cohesive PR through CI/merge acceptance. Large
builds and native calls remain serial under the existing guards. This approval
does not publish another package release or resume paused format workloads.

## Baseline and interpretation

The maintainer clarified that user-managed competing workstreams were paused
for the [current complete observation](../benchmarks/current-runtime-e2e-2026-09-30.md).
Use its 65.806017-second P4 ingest and 55.251837-second single Full43 pass as the
working baseline for this shortlist. Their sum is 121.057854 seconds of native
work; the observed workflow is 136.696318 seconds including intervening checks.
No new baseline campaign is needed to begin source investigation.

The clarification changes interpretation, not the original data. Older mixed-load
measurements, slower samples, rejected experiments and their hashes remain
intact. Attribute an individual regression only when it repeats with both roles
under the same paused-workstreams condition. Preserve rejection reasons based
on absent work, duplicate mechanisms, correctness or added work regardless of
host contention. Ordinary desktop activity, a single query pass and uncontrolled
OS cache remain explicit parts of the latest observation.

The [review evidence](../benchmarks/quiet-runtime-source-review-2026-09-30.json)
pins source hashes, verifies the original Q6/Q28 stdout archives, recalculates
all 43 CPU/wall ratios, and retains the relevant frozen-binary disassembly.
Accounted CPU seconds divided by wall seconds describes average process CPU
concurrency; it is not a hardware utilization measurement or a speedup forecast.

## Proposed finite experiment order

| Order | Experiment | Source finding and admission condition |
| --- | --- | --- |
| 1 | Existing ingest grant P4/P6/P8 | The current owner policy supports the screen without a new scheduler. Freeze runtime, source, metadata policy and output identity; record the coupled prefetch change. |
| 2 | Borrowed UTF-8 reads in native sort blocks | Three `bytes_at` reads remain in validation, comparison and winning-value construction. Reuse the existing borrowed helper; keep independent ownership for retained output. |
| 3 | Concrete block loop for grouped numeric measures | The release executable retains indirect per-measure callbacks. Start with the general integer-key, numeric AVG and COUNT shape represented by Q28; preserve arithmetic and error order. |
| 4 | Scalar exact text DISTINCT partitions | Q6 currently inserts chunk contributions into one scalar set during scanning. Attribute dictionary construction, promotion and exact insertion before admitting a bounded scalar partition path. |
| 5a | Vacant-slot reuse in the single-string dense directory | A miss is currently probed again on insertion. Reuse its slot only while directory capacity/generation is unchanged. |
| 5b | Hash-tag rejection in that directory | Lookup currently dereferences the dense record before checking its full hash. Screen a tag separately after the vacant-slot decision, accounting for all extra memory or ordinal limits. |

These are five proposal families with two separate directory screens. Source
evidence establishes work, not a promised saving. Stop a candidate at admission
when attribution finds negligible or already removed work. Retain a prototype
only after the frozen focused comparison and complete acceptance described below.

### 1. Ingest grant: configuration evidence

[`IngestCpuLanes::pipeline_demand` and `partition`](../../shardloom-vortex/src/ingest_cpu_lanes.rs)
allocate the positive grant by subtraction, retaining the caller and prioritizing
the admitted source owner. For this one-source-worker pipeline:

| Requested grant | Caller | Source workers | Conversion workers | Provider drivers | Prefetch slots |
| --- | ---: | ---: | ---: | ---: | ---: |
| P4 | 1 | 1 | 1 | 1 | 3 |
| P6 | 1 | 1 | 1 | 3 | 4 |
| P8 | 1 | 1 | 1 | 5 | 4 |

Actual topology fields from each prepare call must match the intended path.
These counts scope ShardLoom-owned drivers; they do not count blocking I/O or
source-library internal threads. A faster larger grant is a configuration gain,
not a same-resource software speedup. Do not change the conversion owner,
codecs, provider runtime or queue topology during this screen.

### 2. Borrowed sort reads: visible boundary, small implementation

[`native_sort_block.rs`](../../shardloom-vortex/src/local_primitives/native_sort_block.rs)
reads UTF-8 through `bytes_at(row).as_slice()` at validation, comparison and
retained-value construction. The existing
[`native_utf8::borrowed_bytes`](../../shardloom-vortex/src/local_primitives/native_utf8.rs)
uses the Vortex view/buffer API to borrow inline or external bytes without a
temporary buffer-handle clone. It leaves validity and UTF-8 validation to the
caller, exactly as the present access method does.

Keep validation of every currently validated value, including malformed values
that lose the Top-K cutoff. Winner construction still copies into an independent
`String`; borrowing the source bytes must not change output lifetime. Existing
tests cover nullable parents, invalid losing UTF-8, ties, offsets, source addresses
and empty inputs. Add direct assertions for inline/external views, sliced arrays,
multiple external buffers and retained output after source-owner release.

### 3. Concrete measure loops: machine-code evidence strengthens admission

[`BoundCompactNumericUpdates`](../../shardloom-vortex/src/local_primitives/bound_numeric_updates.rs)
binds native types/validity once, then invokes a boxed callback for each measure
of each row. This is not merely a source-level suspicion: in the original
ThinLTO release binary, `GroupedAggregateState::update_compact_measures_from_direct_row`
loads a vtable callback at `0x1013bc088`, calls it through `blr x9` at
`0x1013bc098`, then loops at `0x1013bc0b4`. The evidence file retains the complete
function disassembly, command and executable hash.

The original Q28 output records an embedded rewrite to
`__shardloom_derived_utf8_len_URL`, native numeric owners for that column and
`CounterID`, and `compact_count_sum_avg_group_state`. This resolves the difference
between an already-derived numeric value and an unevaluated `Length` transform:
the binder admits the former and rejects the latter. Its accessor summary's
`direct_i64`/`direct_u64` labels are semantic categories; do not infer original
physical widths from those labels.

Q28 records 2.042902 seconds in the compact group-update scope out of 2.147939
seconds for the full native process. That scope includes group lookup and other
row work; it does not isolate callback cost. A first prototype should move shape,
physical type and validity dispatch outside a concrete block loop while retaining
the same row visitation, group lookup, original measure order, checked counts,
floating additions, selected-row order and partially updated state on error.
Admit by semantic shape, never query number or column name. Q10 is a separate
consumer and should not force the first kernel to become a general code generator.

Parallel AVG remains a separate design. A trusted generation-bound proof such
as nonnegative integer inputs with `N * Lmax <= 2^53` can support a narrowly exact
reordered sum, but current evidence does not establish that proof. Keep the
existing accumulation order in the dispatch experiment.

### 4. Scalar DISTINCT: revise the boundary before implementation

The original Q6 output reports `dictionary_arc_direct_exact`. Source inspection
of [`local_primitives.rs`](../../shardloom-vortex/src/local_primitives.rs) shows:

- The scalar scan calls `SimpleAggregateStates::update_direct_from_chunk`.
- `aggregate_direct_count_distinct_update_utf8_dictionary` marks referenced,
  nonnull dictionary codes and inserts their values into one persistent scalar set.
- `aggregate_direct_count_distinct_insert_utf8_dictionary_values` calls
  `to_owned_arc()` before each used value's set insertion. The lazy independent
  copy is shared within that dictionary entry; this is not a global miss-only
  promotion check across chunks.
- Scalar finalization returns the set cardinality. It is not a late merge of
  existing scalar worker partials. The existing count/distinct workers admit
  grouped keys and do not establish scalar worker admission.

Consequently, this is a proposed bounded scalar ingestion/union path, not just
parallel finalization. Attribute decoding, dictionary construction, used-code
marking, per-entry promotion and global insertion before choosing its scope.
The current scalar timing fields do not separate those stages. The recorded
dictionary strategy also does not pin whether the source was a native Vortex
dictionary or a constructed chunk dictionary; do not infer the codec from a
flat file layout or from the strategy label.

A prospective implementation can reuse native owners, full hashes, byte equality
and bounded worker infrastructure. It must establish scalar-specific memory
reservations, owner lifetimes and cancellation before retaining partials. Equal
values must reach the same deterministic partition; collisions still compare
complete bytes. Checked partition-cardinality summation is valid only after
exact within-partition union. Unused dictionary values, code/value nulls, the
empty string, repeated values across chunks, cancellation and allocation denial
need explicit fixtures. Do not produce grouped output rows or maintain occurrence
counts merely to obtain one cardinality.

The admitted prototype uses a single identity UTF8 COUNT(DISTINCT) measure with
no grouping, predicate, spill, or source-order limit. The existing chunk job
window owns one caller and the remaining admitted CPU lanes; provider drivers
are restored before scanning if admission declines. Each chunk retains its
native canonical owner. Workers build the existing source-backed dictionary for
canonical rows; its entries are already referenced and nonnull. Native
dictionaries retain their codes and mark only referenced nonnull values. Both
representations union values into 64 content-hash partitions.
Partitions retain independent bytes only on an exact global miss, with full
hash and byte equality. Directory, dense record, byte arena, task and metadata
capacity are reserved from the shared live pool, including replacement peaks.
No occurrence counts or grouped output are constructed. Cancellation drains
jobs; post-admission failures abort the operation. Final cardinality is a checked
sum after all jobs finish, and the ordinary scalar result/HAVING surface consumes
that value. This is an in-memory path, with no new spill behavior.

Existing Q6 receipts already separate provider execution and chunk dictionary
construction. The one-second sampled stack also contains independent string
promotion and global set insertion. Used-code marking has no separate sampled
frame or clock, so its cost is unresolved rather than assumed negligible. The
prototype covers the complete dictionary/mark/union unit; it does not claim that
finalization alone or any one of those stages accounts for the prospective gain.

### 5. Dense directory: keep the screens separate

In [`string_count_partitions.rs`](../../shardloom-vortex/src/local_primitives/string_count_partitions.rs),
`update` calls `find`; a miss reaches `insert`, which searches again after its
capacity checks. Preserve the first vacant position only when no directory growth
invalidates it. Dense-page or byte-arena growth does not change directory buckets,
but failed allocation/retry must not publish a key or consume an entry credit.

`find` reads an ordinal, fetches its dense record, then compares the full hash
and exact bytes. A hash tag can reject a mismatch before that dense-record read.
Use tags only as rejection filters; equal tags retain full hash/byte checks.
Charge side-array capacity and transient resize peaks, or prove checked bounds
for a packed ordinal. A tag chosen from already-consumed partition/bucket bits
can have poor rejection power; measure real probe and record-fetch work.

Existing tests force full-hash collisions, first-page and directory growth,
multi-page state, count overflow, allocation denial, retry and lease release.
Extend them for stale-vacancy invalidation and deliberately equal tags before
timing. Begin with this inspected single-string implementation; compound
directories require their own matching-source check and measured retained benefit.

The tag prototype packs a 16-bit high-hash tag with a 48-bit ordinal-plus-one
in an eight-byte directory entry. Zero remains empty. Check ordinal encoding
and limit directory capacity to `2^48`, so bucket bits and the existing partition
bits (32 through 37) never consume the tag bits (48 through 63). Capacity leases
use the entry's actual size, including on 32-bit targets. Full hash and exact
bytes remain authoritative. Probe counters are test-only: an initial slot-reuse
prototype with production counters regressed Q34, and its retained correction
removed them before the separate tag comparison. A separately guarded real-input
diagnostic records probe and dense-record work without adding production overhead.

## Comparison context

The supplied Polars figures are reproducible from the official
[August 24 c6a.4xlarge result, pinned to the inspected repository revision](https://github.com/ClickHouse/ClickBench/blob/80d24dca3797ac5014cbf69ce3a2e6a67ef2f620/polars/results/20260824/c6a.4xlarge.json).
Summing the minimum second/third sample for each query gives 45.347 seconds.
The arithmetic difference is 9.904837 seconds, or 17.9267% of the current
ShardLoom single-pass total. Q6/Q26/Q27 contribute 5.565458 seconds to that net
raw difference. The evidence file preserves the retrieved result and calculations.

This compares different machines and sampling protocols, and the result file
does not pin its engine version. It can suggest shapes to investigate; it is
not a same-hardware ranking, expected saving or retention criterion.
[DuckDB's published aggregation design](https://duckdb.org/2022/03/07/aggregate-hashtable)
provides conceptual precedent for hash-tag rejection and independent hash
partitions. It supplies neither ShardLoom performance evidence nor runtime code.

## Validation and completion contracts

Before each screen, freeze its control/candidate identities, workload, relevant
results, configuration, paired order and retain/drop rule. Reuse the
[guarded local procedure](local-development-storage.md#current-runtime-observation-procedure).
Confirm the paused-workstreams condition for the new measurement; a report about
the earlier run does not establish current host conditions. Keep builds, tests,
profilers and native workloads serial. Preserve every sample, slower observation,
complete output and raw receipt. Do not repeat the whole baseline campaign.

For ingest, compare P4/P6/P8 through the same public prepare route and verify
complete artifact byte equality before retiring each run-owned duplicate.
The first P6 call exposed an additional existing grant effect: the public
workflow divides its source batch byte budget by `max_parallelism`, yielding
131,072-row batches at P4 and 65,536-row batches at P6/P8 on this source. The
original byte check stopped and preserved the changed artifact; its failure
receipt remains intact. All 99,997,497 rows and all 112 source/derived columns
then passed a complete native value comparison, with exact schema, whole-file
statistics and embedded provenance bytes. Physical layout and directory bytes
are different. The continuation admits an already verified complete hash or
the same full-value/metadata proof for a new hash. It retains one changed-layout
artifact for a separate paired Full43 comparison using the unchanged executable.
Record topology, coupled source batching/prefetch, wall/CPU, RSS and cache
context. Keep the original P4 observation labeled P4. This remains a resource
configuration comparison, not an isolated provider-thread or software speedup.

For query candidates, use focused quiet paired comparisons against an unchanged
control, then complete Full43 acceptance for a cohesive retained implementation.
Keep the existing symmetric fastest-valid-run rule and retain all samples.
Exact results and semantic fixtures are mandatory; add the required workspace
format, Clippy and test gates plus the affected native-feature tests. A dropped
prototype leaves evidence, not runtime complexity. Review performance claims
against the exact final-source executable before PR acceptance.

Vortex-first classification remains `implement_shardloom_kernel` for existing
sort/aggregate consumer improvements over admitted native Vortex owners; the
ingest grant screen uses the existing native provider unchanged. This review
adds no array format, scanner, scheduler, dependency, JIT, arithmetic relaxation,
external query-engine execution, package publication or capability claim.
The scalar path requires a design/admission check against the existing memory,
streaming and cancellation contracts before implementation. Native Vortex input
and output, deterministic unsupported diagnostics and false fallback/external
execution certificates remain required. Broader PERF obligations, CG-1 through
CG-23 and paused format workloads keep their current status.

## Review checks completed

The primary verified both independent source inventories against current files,
read the material call sites, resolved Q28's lowering uncertainty from its
original hash-checked output, and verified the frozen executable and assembly.
The Polars arithmetic was independently recalculated from its pinned result.
Three review objections are addressed above: an unproved callback cost, a
nonexistent scalar final-union stage, and confusing cross-machine arithmetic
with a performance target. Original benchmark JSON and compressed evidence are
unchanged. No new native workload, Rust build or Rust test ran for this
documentation/source-review change.
