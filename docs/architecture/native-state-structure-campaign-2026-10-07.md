# Native state and structure campaign

Status: source-grounded implementation and experiment queue for the maintainer's
October 7 direction. No strategy in this campaign has been implemented or timed.
Builder resource acceptance, packet inspection and hosted integration have passed
in PR #1529, preserving all 941 accepted runtime source assets. This campaign
follows the completed five hardware decisions and composed-COUNT decisions;
their drops stay dropped. Published v0.4.0 and broader PERF/CG status are unchanged.

The later October 7 [conditional exact work intake](native-conditional-work-campaign-2026-10-07.md)
adds six distinct ship/drop candidates. Exact decimal accumulation and conservative
membership filtering were subsequently dropped after their frozen screens;
completion-aware input remains the next architectural capability. The additional candidates do not replace the
five tracks here or grant a speedup claim from research in another system.

The inspected HEAD is `094c50b83235cd0594e1858acfcc7aa5a3f4760a`. The builder
candidate is identified separately by its complete 941-file snapshot
`a7308b726410569306bae14bf60ddd07f57bead73d44d423f5274b5b17c10c5f`, not by
HEAD alone. All 941 assets subsequently match accepted implementation commit
`53cd1582a5975ac4e206a21e45415c919c5b6b4f`. The original intake was written
outside the checkout while that acceptance was frozen; this queue does not alter
the builder's historical packet or claim a new measured implementation.

## Decisions and order

1. Completion-aware input streaming is the next capability unit under
   PERF-03/07/11/12 and CG-5/19/20/21. Start with a single-use input feeding the
   existing pure filter/project and local output path. Demonstrate a complete
   input larger than the native grant, bounded retained input, and safe failure.
2. Investigate selective rematerialization alongside that source-lifetime work
   under PERF-03/06/07/12. First measure genuinely retained intermediates. The
   newly added predicate blocks are not currently a retained mask cache.
3. Prepare an independent constraint-guided inner-equijoin experiment under
   PERF-02/03/10/12 and CG-5/20/21. Native workload runs remain sequential on this
   workstation, even when source analysis proceeds in parallel.
4. Admit a nested-identity prototype only after repetition and recursive visits
   are measured in actual grouping/join/pivot operations. Keep it query-local.
5. Replay real relational ordering run sizes before choosing a new merge
   schedule. Carry any promising schedule into the next coherent native spill
   unit under PERF-03/06/12, including reader and codec resource proof.

These are finite work units attached to existing phase owners, not replacement
phase IDs. Keep CG-1 through CG-23 intact. A version bump waits for a substantial
capability milestone with operational evidence; it is not attached to each
retained experiment.

## What source inspection establishes

All paths below are repository-relative. Source references were checked in the
current worktree, with the builder candidate frozen. No workload measurements
were inferred from these reads.

| Boundary | Evidence | Consequence for the campaign |
| --- | --- | --- |
| Resident batch input | `shardloom-vortex/src/resident_memory_batches.rs:29` owns `Vec<ArrayRef>`; `push_columns` retains each batch; `finish` at line 126 composes native chunks. `shardloom-cli/src/python_batch_protocol.rs:171` requests every input before returning the source. | Existing demand-driven intake is bounded resident collection. True input streaming must move consumption into execution and release completed batches. |
| Shared native transforms | `shardloom-vortex/src/local_primitive_relational_transform.rs:21` projects columns; line 47 evaluates a filter and builds a batch-local row selection. `local_primitive_relational_scan.rs:55` iterates ranges of an already resident source. | Extend source delivery into these owners. Do not make a Python-only execution loop or reconstruct generic scalar rows internally. |
| Predicate lifetime | `shardloom-vortex/src/local_primitives/native_relational_predicate_block.rs:156` owns temporary column handles and computes 64-row truth words. The returned Boolean array is consumed by the current filter callback. | No retained predicate-output cache was found in this execution path. Evicting these ephemeral words cannot remove a persistent pressure source. Measure another retained target before building rematerialization policy. |
| Provisional results | `shardloom-vortex/src/local_primitive_prepared_relational.rs:566` delivers native/JSON batches before final validation; `python/src/shardloom/_batches.py:271` acknowledges result batches and requires a successful terminal report. | Early delivery remains provisional. Source end and the final acknowledgement are separate from emission. |
| Native file publication | `shardloom-vortex/src/local_primitive_native_sink.rs:596` creates owned staging; lines 695–709 drain a healthy writer on producer error; lines 745–750 validate then commit. `OwnedOutput::commit_with_unlink` at line 929 publishes a complete file and reports a distinct post-publication unlink failure. | Late input failure must prevent commit. Keep the existing published-but-cleanup-failed diagnostic distinct from pre-publication failure. |
| Binary join | `shardloom-vortex/src/local_primitives/native_relational_join.rs:43` retains a right table and index; `build` at line 60 indexes keys; `consume` at line 91 enumerates each duplicate chain into bounded pairs. | Bounded output does not itself avoid doomed intermediate combinations. A multiway candidate must include access-path preparation and exact multiplicity. |
| Nested identity | `shardloom-vortex/src/local_primitives/native_relational_nested_keys.rs:122` recursively compares values; line 188 hashes children; line 233 writes exact keys. `local_primitive_unary_values.rs:303` already caches native key owners. | Reuse the existing owner/comparator infrastructure. Existing owner caching is not structural hash-consing. Exact unary keys and relational floating equality have different policies. |
| Relational ordering spill | `shardloom-vortex/src/local_primitives/native_relational_spill.rs:286` appends runs and merges adjacent equal-level pairs; line 324 opens two readers. `local_primitive_query_run_store.rs:226` exposes native run rows and bytes. | There is a concrete schedule experiment over current native runs. Byte-aware buffer admission is already present; byte-aware merge selection is not present in this path. |
| Numeric sort spill | `shardloom-vortex/src/local_primitive_sort_spill.rs:500` already chooses 8/4/2 readers from its reservation. | Reuse its resource-geometry lessons. Do not present bounded multiway merging as entirely absent from ShardLoom. |

## Source mechanisms and transfer limits

| Research | Established mechanism | ShardLoom transfer and disanalogy |
| --- | --- | --- |
| [Checkmate](https://arxiv.org/abs/1910.02653) and [Dynamic Tensor Rematerialization](https://arxiv.org/html/2006.09616) | Checkmate plans recomputation schedules using profiled costs; DTR makes online eviction decisions that account for dependencies. DTR assumes pure operations and cannot regenerate external constants. | Compare retaining, supported native spill and bounded regeneration of immutable derived buffers. Include pinned dependencies, metadata and reconstruction headroom. Do not import a tensor runtime, MILP solver, one-shot producer replay or the papers' performance claims. |
| [CALM](https://arxiv.org/abs/1901.01930) and [Timely progress tracking](https://timelydataflow.github.io/timely-dataflow/chapter_5/chapter_5_2.html) | Monotonicity constrains coordination needs; tracked capabilities constrain which later messages may arrive. | Borrow explicit completion reasoning for one local execution. End-of-input is the first proof. Neither result automatically permits early final SQL aggregates, anti-join output, or guesses that a key will not recur. No distributed runtime is introduced. |
| [Datalog and constraint solving](https://people.cs.kuleuven.be/~tom.schrijvers/Research/papers/ciclops2013.pdf) | The paper derives Leapfrog Triejoin by narrowing compatible variable domains and advancing ordered iterators. | Explore avoiding invalid assignments across eligible inner equijoins. The formulation does not certify ShardLoom's bag multiplicity, null/numeric/error/order contracts; those need independent proof. |
| [Type-safe modular hash-consing](https://gallium.inria.fr/ml2006/accepted/5.html) | Controlled construction shares structurally equal immutable values under a chosen equivalence relation. | Query-local tokens can avoid repeated recursive work only after exact collision resolution. Domain identity and the operation's equality policy must travel with a token; token order is not SQL order. |
| [Multiway Powersort](https://www.wild-inter.net/publications/cawley-gelling-nebel-smith-wild-2023.pdf) | Stable ordered merge trees can reduce repeated transfers for unequal runs. | Test native compressed-run schedules. The paper's internal sorting and element-transfer objective is not a measured external-spill latency model. Account for compressed bytes, decoding, footers and overlapping readers/writers. |

The pinned Vortex 0.85.0 source already provides `ArrayIterator`,
`ArrayIteratorAdapter`, `ArrayStream` and `ArrayStreamAdapter`. They carry native
arrays and a dtype. `ArrayIterator::read_all` collects all chunks, so it is not the
new streaming execution route. The iterator requires its implementer to enforce
dtype consistency; the stream adapter's equality check is a debug assertion.
ShardLoom must validate every incoming batch in release builds and preserve typed
errors. Reuse native arrays/iterator concepts, the session allocator, the shared
relational consumer and native sinks. A completion/admission wrapper is required;
an upstream query-engine integration is not.

## First capability contract: completion-aware input

Bind the declared schema before requesting payload. Classify the complete native
plan before consuming a one-shot source. The first admitted chain has one source
consumer and row-local pure filter/project operations. Source data is copied only
at the existing compatibility intake boundary, then stays native through execution
and output. Batch limits and byte credits remain explicit.

Each input batch must survive until every current consumer and any queued sink
owner releases it. Request the next batch only when that overlap is admitted.
Record actual maximum retained input batches/bytes, cumulative intake bytes,
output queue bytes, and first provisional delivery. A source that ends without
rows still has an exact declared empty schema.

End-of-input is an observed protocol event, not inferred from a filter, a limit,
or a temporary absence of keys. A finite output limit does not excuse accepting
an unvalidated later input silently: define and test whether an eligible plan
drains remaining input or explicitly terminates its declared source contract.
Keep limits out of the first admission if that policy is not established.

A repeated source reference, self-join or data-dependent binding must use its
declared retained or admitted spool path, or fail before consuming the source.
Do not replay a one-shot producer. Existing resident execution remains explicit
for its admitted shapes; unsupported streaming is not an implicit unbounded
collection path. Later grouped/ordered completion requires an enforced scope or
ordering contract.

Writers may create a private staging file and deliver batches into it, but may
publish only after source completion, producer validation, sink drain and final
validation. Preserve destination and exact staging cleanup on a late malformed
batch, producer exception, cancellation, exhausted grant, disk quota or sink
failure. For result iterators, delivered batches remain provisional until the
successful final report and all acknowledgements.

Bounded input retention does not imply constant total memory for unlimited output:
native writer footer/chunk metadata and other retained state must remain admitted
and may impose an explicit limit. Measure that separately rather than calling the
whole process bounded from an input-batch count.

Acceptance covers one-shot input substantially larger than the native grant,
exact complete output and types, empty batches, nulls, Unicode, integer limits,
slow consumers, cancellation, late schema/value/source failure, complete cleanup,
and existing resident/repeated-source controls. Reuse current Python batch framing
and native API/sink tests. No second query invocation or answer cache may produce
the measured result.

## Experiment admission

| Candidate | Predicted mechanism and measurements | Required controls and refusal conditions |
| --- | --- | --- |
| Rematerialization | First identify a retained derived owner and when it is reused. Measure bytes freed minus dependency bytes pinned, the complete regeneration chain, reserved reconstruction peak, spill traffic avoided, regeneration count and full workflow latency. | Grant sweep plus ample-memory controls; immutable owned inputs, exact original errors, bounded regeneration count. Exclude effects, mutable sources, arbitrary generators and ordered aggregate-state reconstruction. If no meaningful retained target exists, record that finding instead of constructing an artificial cache. |
| Multiway joins | Compare existing binary execution to one native compatible-domain strategy on cyclic `R(a,b)`, `S(b,c)`, `T(a,c)` and controls. Count candidate assignments, exact comparisons, native preparation/index bytes, intermediate bytes, output rows and peak state. | Include first-use preparation, separate generation-bound reuse, ordinary star/two-table controls, duplicates, skew and near-empty results. Preserve exact bag/null/mixed-numeric semantics, error ordering and required output order. Exclude outer/anti joins and arbitrary failing predicate movement. |
| Nested identity | Measure real recursive visits and repeated logical values before allocating a registry. Charge canonicalization, collision comparisons, row-to-token mapping, representative payload and later consumers. | Whole-value/subtree repetition and all-unique controls; forced collisions; null parents/children, empty lists, field order, fixed shape, decimal metadata and operation-specific floating equality. Token domains cannot mix; sort/MIN/MAX retain the logical comparator. Prevent large source pinning for tiny survivors. |
| Merge schedule | Capture actual native run rows, physical bytes, logical bytes and merge lineage. Replay stable adjacent schedules before implementing the most promising one. Measure actual bytes read/written, decode work, merge levels, first output, peak native buffers/metadata and total workflow time. | Equal and skewed runs, long strings/nested payloads, stable duplicate ties, tiny grants, cancellation, corrupt runs and disk quota. A schedule that requires unadmitted readers or an unsupported codec workspace is rejected. Reuse the existing run store and owned cleanup. |

## Causal measurement and disposition

Freeze a candidate's eligibility, fixtures, scales, grants, mechanism counters,
first-use/reuse boundaries, controls, order schedule and quantitative retain/drop
thresholds before timing. Do not use one threshold for all five mechanisms.
Capability acceptance is correctness and resource proof; a speedup is not required
to remove an existing supported-workflow size barrier.

Where feasible, select strategies before execution within one binary, check exact
complete results, alternate order and repeat the declared confirmation cohort.
Then confirm the retained production build separately. Counter instrumentation
must not selectively burden one strategy. First-use preparation, reusable
preparation, pressure/spill execution and first provisional output have separate
clocks. None is a ClickBench speedup by relabeling.

[Coz](https://arxiv.org/abs/1608.03676) motivates measuring effects on completed
progress. [Stabilizer](https://people.cs.umass.edu/~emery/pubs/stabilizer-asplos13-draft.pdf)
shows why layout can confound binary comparisons. Same-binary selection limits
some differences but does not reproduce Stabilizer or prove causal attribution.
Unexpected controls remain unexplained until measured evidence establishes a
cause; do not blame cache noise or independent source-analysis work.

Check tooling against the actual platform. The current
[Coz repository](https://github.com/plasma-umass/coz) describes a macOS backend
using private profiling facilities with privilege/security constraints. Do not
install or change workstation security for this campaign; borrow the method and
use existing supported observation tools.

Preserve all raw samples, failures, negative controls, source/executable hashes,
complete-output checks and frozen decisions. No previously dropped directory,
reservation-transition, locality or composed-COUNT prototype is reopened by this
campaign. A changed strategy needs its own distinct mechanism and evidence.

## Next concrete artifacts

- The canonical phase queue now carries `NATIVE-INPUT-COMPLETION` under existing
  PERF owners. Preserve the inspected Git/snapshot identities above and distinguish
  them from later implementation revisions.
- Write the streaming design against the shared binder/source/consumer interfaces,
  including release-build schema validation, source-use classification, completion
  states, sink metadata limits and failure evidence.
- Select and instrument actual retained intermediates for the rematerialization
  screen; do not start from ephemeral predicate words.
- Define the independent cyclic-join fixture/oracle and exact ordering contract,
  followed by native strategy preparation. Record nested repetition and actual
  relational run-size distributions when their coherent units are reached.

These artifacts authorize no external engine execution, new dependency, new
remote effect, broad spill family or package publication. Any retained code still
needs focused semantics/resource checks, existing whole-engine regression gates,
adversarial review and hosted integration under the standing task authorization.
