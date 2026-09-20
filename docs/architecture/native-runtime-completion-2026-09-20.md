# Native runtime completion

Status: implementation in progress after `d0c656d3` / PR #1454. The maintainer
explicitly prioritizes spill/recovery, prepared/public availability and concurrent
serving, including currently unsupported native operator families. Ingest CPU and
persisted-storage experiments follow their implementation, validation and merges.
Q10 optimization is deferred. Availability fixes do not need a speedup to ship.

## Contract and reuse

Every advertised execution route must perform the requested work through
ShardLoom-native operators over Vortex input, returning complete values and an
accurate certificate. A planner descriptor or a successful diagnostic is not an
executed result. Preparation retains source generations and immutable lowering;
every execution owns fresh state. No external query engine or cached answer may
complete missing work.

Preserve the existing ClickBench physical choices: typed native accessors,
dictionary identity, physical-key proofs, complete-key count and DISTINCT
partitions, weighted reduction, exact global selection, and owned result/sink
routes. New relational composition must call those shared components rather than
replace them with a generic row engine. Full43 remains a regression acceptance
surface alongside renamed-schema and non-ClickBench complete-value tests.

The work belongs to PERF-02/03/04/05/06/07/10/11/12 and the existing CG-5/6/20/21/23
obligations. No new phase IDs, publication authorization, production certification
or competitive claim follows from this plan. Individual evidence must identify
its operator, source, resource and failure envelope.

## Completion sequence

- [ ] Complete native spill pressure transitions for existing COUNT and DISTINCT
  worker families; carry the same quota-accounted native run store into additional
  stateful families instead of introducing another temporary file subsystem.
- [ ] Prove cancellation, quota denial, corruption, source mutation, crash cleanup
  and owner release through each admitted spill route. Recovery must reject a
  live workspace, preserve unknown/replaced files and permit interrupted cleanup.
- [ ] Extend prepared execution from current count/filter/project/integer-aggregate
  shapes to the existing native aggregate, ordering, distinct/duplicate, reshape
  and rolling families, including their currently rejected supported expressions,
  schemas and explicit spill policies. Cover complete public CLI/SQL/DataFrame/
  Python calls, not only internal reports.
- [ ] Implement missing native relational families, including joins, through the
  same source, typed-key, selection, resource, spill and owned-output contracts.
  Inventory the exact missing shapes before each implementation and keep
  capabilities aligned with tested execution rather than scenario names.
- [ ] Complete owned computed results and the existing native/compatibility sink
  boundary so unsupported ownership alone cannot block an executable family.
- [ ] Implement bounded cancellable serving admission, actual CPU/I/O ownership,
  read/read and short-read/write progress, and source-generation isolation.
  Measure fixed-arrival workloads with queue/service/complete latency, throughput,
  full result validation and drain evidence. A held test callback does not prove
  production latency, and arbitrary blocking callbacks are not preemptible.
- [ ] Run focused negative/boundary tests, workspace/native gates, full public UAT
  and independent review; merge cohesive implementation units.
- [ ] Then attribute ingest CPU and physical bytes/encode/decode lifecycle costs
  and implement only material, verified ingest/storage candidates.

## First implementation unit: ownership and prepared aggregates

Implemented at `739a311e05eaa9388486c3eb805ba3c89e86594b`. Local tests, native and
workspace Clippy, formatting, full-size Full43, held-out/public-call UAT and bounded
serving load checks pass. Targeted timing comparisons are recorded below; remote
acceptance remains pending:

- Shared native run directories now exclude live-owner and competing recovery.
  Real child-process exit tests cover numeric sort, integer DISTINCT and weighted
  UTF8 COUNT; cancellation and interrupted cleanup preserve retryability.
- Ordinary prepared aggregate admission delegates measure validation and lowering
  to the existing native executor. It no longer imposes separate integer-only,
  two-key, 64-measure or identity-expression restrictions. Native tests exercise
  text/float/null keys and values, complete triple keys, derived outputs, measure
  transforms/offsets and 90 measures; every repeated result is checked in full.
  The Q36 native partition fixture now retains its derived outputs in prepared calls.
- Explicit weighted UTF8 COUNT and exact integer DISTINCT reuse the held source
  and existing run adapters. Spill lowering/state/runs remain fresh per call and
  certificates say so. Supplied provider sessions cannot also start aggregate
  workers. The CLI worker reuses its cancellation identity for identical parsed
  spill configurations and rebinds changed resource policies.
- Bounded serving admission, cancellation and owned I/O completion have native
  tests and fixed-arrival receipts in `concurrent-native-serving-2026-09-20.md`.

This unit does not complete the checklist above. General native joins, remaining
prepared operator families, broader owned computed results, compound/DISTINCT
spill transitions and production-scale serving acceptance remain subsequent work.
Full UAT passes for this unit. No performance improvement, merge or release is
claimed yet.

### First-unit validation receipts

All commands ran sequentially on the local Apple M5, 10 logical CPUs, macOS 27.
Cargo output resolves to `/Users/dylan/.cache/shardloom/cargo-target`; bulk logs,
fixtures and frozen binaries remain under `/Users/dylan/LocalData/shardloom/`.
The candidate was built with `release-user-surfaces` at the runtime commit above:
`ship-drop-20260919/candidate-739a311e`, SHA-256
`f0c6fcf730f872726dce11c5271b8d0e6a37988c51f0b932d356f41c66a17e5f`.
The paired baseline is `candidate-dc4ce81a`, SHA-256
`494ae33284ed864b1387f40c36b139a04455748a1676464c73cf6e25e2706e26`.

| Check | Result |
| --- | --- |
| `cargo test --workspace --all-targets -- --test-threads=1` | 3,424 passed across 102 targets |
| CLI all-target native feature tests | 1,496 passed; the initial combined command subsequently failed Vortex fixture assertions, which were corrected and rerun below |
| Vortex all-target native feature tests | 1,898 passed, 10 explicitly ignored, zero failed |
| Combined CLI/Vortex `--lib --features release-user-surfaces` | 1,900 passed, 10 explicitly ignored, zero failed; preserves combined feature unification |
| Workspace and CLI/Vortex native all-target Clippy | Passed with `-D warnings` |
| Lean `vortex-local-primitives` and `vortex-file-io` feature checks | Passed separately with `--no-default-features` |
| Formatting, diff whitespace and four public-status/version/architecture validators | Passed |
| Fixed-arrival native serving harness | Explicitly invoked at 1,000 and 100 microseconds; complete values and final ownership drain passed; see the serving receipt |

Raw command logs and adjacent JSON receipts are in `ship-drop-20260919/` with
the `admission-runtime-completion-` prefix. The ten ordinary ignored native tests
are not counted as passing; the serving load harness was invoked explicitly.

Held-out acceptance:
`clickbench-100m-uat/logs/heldout_operators_20260920T134008210536Z/summary.json`.
All **760/760** requested records passed: 19 cases, 131,072 rows, worker requests
1/2/4/8/12, three samples plus one warmup per binary. This includes 40 expected
checked-overflow rejections and complete values for the other 720 records.
Both candidate DISTINCT cases satisfy the required worker-execution assertions.
The earlier `heldout_operators_20260920T132708850303Z` attempt is retained as
incomplete: the unchanged log quota stopped it after 629 validated calls, without
a value mismatch. Older completed raw output was losslessly archived and verified
before the complete rerun; inputs, summaries and guard limits were preserved.

Public-call acceptance:
`clickbench-100m-uat/logs/resident_call_paths_20260920T134614991715Z/summary.json`.
All **2,232/2,232** calls passed across 12 cases, three public call paths, two
binaries, 30 samples and one warmup. The paths are fresh CLI process, persistent
worker and actual Python client. This is a 32-row fixture, not production-scale
serving or broad SQL parity. Candidate per-case medians were 0.40–0.90 ms for the
worker and 0.68–1.34 ms for Python; fresh CLI medians were 6.08–6.91 ms and about
0.12–0.27 ms above the paired baseline. These measurements establish bounded
availability and preserve the observed overhead; no speedup is claimed.

Full-size regression acceptance:
`clickbench-100m-uat/logs/full43_20260920T135737243464Z/summary.json`, SHA-256
`b9fc117fdc5e1892a3ad1a2d19e0635fd1abee2e6c240df775c5e3e4210eb659`.
All **129/129** executions across all 43 queries passed complete returned-value
comparison against the retained native references. Those references are not an
independent oracle. The unchanged 18,591,586,804-byte native source and frozen
binary identities are recorded in the summary. Each query ran three times with
fresh CLI processes, complete output and exit; OS cache and other host activity
remain uncontrolled. The sum of query minima is **69.107057s**, with 23 queries
below one second. The previous separate suite recorded 64.551416s. Q29/Q34/Q35
account for 2.945s of the 4.556s difference. This is not an ingest or
production-serving measurement.

Targeted counterbalanced comparison:
`clickbench-100m-uat/logs/paired43_20260920T140403407500Z/summary.json`.
All 18 calls passed complete values and final binary/source identity checks.
The control/candidate minima were Q29 **8.032773/8.017879s**, Q34
**5.024494/4.581915s**, and Q35 **4.688470/5.153102s**. Q35's control/candidate
medians were **5.153275/5.300794s**; individual pair deltas changed sign.
One predeclared reverse-order Q35 follow-up also passed all six calls:
`clickbench-100m-uat/logs/paired43_20260920T140656060623Z/summary.json`.
Its minima were **5.204393/5.370159s** and medians **5.425180/5.547851s**.
Thus the initial one-second Q35 gap did not persist, but these receipts still
show a smaller candidate slowdown; they do not prove unchanged performance or
attribute the difference solely to host contention. Both roles used the same
10-lane native worker/scan grants and existing physical family. Retain the batch
for tested availability and ownership behavior, without a speedup claim, and
preserve this timing cost for the later profiling cycle.

### PR #1455 review follow-up

Review identified two additional ownership boundaries. Waiting-queue bounds must
not reject an immediately runnable call on a free reserved metadata lane; direct
admission still honors earlier waiters in the same class, cancellation, closed
admission and the CPU ceiling. Separately, cancelled prepared spill handles now
provide `renew_spill_cancellation(&mut self)` to retain the source/configuration
with a fresh cancellation scope. Old policy clones remain scoped to the old call;
exclusive mutable access prevents renewal during active execution. Tests exercise
the public cancellation/renewal methods rather than resetting a private flag.
The fixes at `af047f5c694d726883e9ee13fd92634197abc39f` passed 3,415 combined
native CLI/Vortex all-target tests (10 explicitly ignored), 3,424 workspace tests,
workspace/native Clippy, formatting and the lean native feature check. The
1,000-microsecond serving rerun completed all 96 requests with zero engine errors
and zero final reservations. Its peak was four CPU lanes and one positional read;
the exclusive mode completed 57/rejected 39. This remains bounded debug-fixture
evidence, not a production comparison. The receipts use the
`admission-runtime-completion-review-` prefix.

Full43 on that frozen runtime passed **129/129** complete results and recorded
**65.769553s** summed minima. Receipt:
`clickbench-100m-uat/logs/full43_20260920T143949591215Z/summary.json`, SHA-256
`8e62163af531f3517bb979755441cff1965cfaf475649f3ad39f0d2440e3d984`.
Binary `candidate-af047f5c` SHA-256:
`d525d026d86ea21ab58c3a0bbfdb2884fd1266ee91d230a966598a6551b1fed3`.
The earlier held-out/public-call receipts remain attributed to `739a311e`.

Subsequent review identified unbounded width-dependent allocation in preparation.
Prepared requests now admit at most 1,024 measures, checked before request cloning
or aggregate-state construction. This retains the tested 90-measure behavior and
adds complete repeated results at the exact ceiling, plus rejection at 1,025
before any source open or retained reservation. It is a schema ceiling, not an
RSS/accounting claim. The 165 focused prepared tests and native all-target Clippy
passed after this guard. Final frozen runtime `f4245c53` passed **129/129** Full43
complete results, recording **68.524554s** summed minima. Receipt:
`clickbench-100m-uat/logs/full43_20260920T145420087484Z/summary.json`, SHA-256
`694a536d547a17ee2ac52e9a9cb21e7c8f1242bb0513011a80b48c48be9d2483`.
Binary `candidate-f4245c53` SHA-256:
`e5591d73274eb45ce31bc397b14e84125ee678881f7ee869dc71161cd501c7a9`.
These separate Full43 runs remain unpaired, with unchanged source and timing
boundaries; no performance improvement is claimed.

Linux CI exposed a race in the blocking-completion test's final assertion:
the read buffer and I/O permits were released, but the completion destructor
could still hold its last scope reference and 80-byte metadata reservation after
waking the drain. The test now uses the existing bounded wait for that destructor
epilogue, while keeping immediate zero-I/O assertions and eventual zero-memory
ownership mandatory. Runtime behavior is unchanged by this test correction.
The exact CI native-library command passed locally with normal test concurrency:
1,885 passed, ten explicitly ignored, zero failed. Its log SHA-256 is
`111e3a7709fafd1d6868e36fb13b03b628ef7dc411af0219990bd7389767213f`
(`admission-runtime-completion-ci-native-parallel-1`). An additional immediate
assertion requires only the measured scope metadata to remain after drain,
preserving a direct payload-release check before waiting for final metadata drop.

## Finite availability inventory

The completion scope is the existing native unary families, the four relational
gaps already represented by the public parser/API, and their shared execution
plumbing. Parser support, a decoded reference implementation, prepared Vortex
input and a retained native operator are separate capabilities. Do not label one
as another. This does not silently expand into every SQL-standard feature,
arbitrary callbacks, recursive SQL or external-effect/platform integrations.

| Family | Execution to reuse | Remaining native availability |
| --- | --- | --- |
| Count/filter/project | Prepared count/count-where/projection and native scan lowering | Preserve admitted residual selections in owned collect; complete public parity |
| Aggregates | Existing typed accessors, physical-key proofs, weighted counts, exact COUNT DISTINCT, SUM/AVG/MIN/MAX, HAVING and ordering | Broad owned computed results/sinks and general public lowering; wider spill transitions |
| Sort/Top-N | Native exact ordering, secondary keys, partition selection and late payload gathering | Retained prepared handle/owned output and broader spill schemas |
| Distinct/deduplication/duplicate mask | Existing exact row-key and first/last/all survivor logic | Retained execution, owned survivor selection and row-state spill |
| Tail/sample | Existing deterministic source ordinals and weighted/replacement sampling | Prepared/owned composition preserving repeated rows and seed semantics |
| Expressions/casts/nested access | Existing typed rewrite and aggregate-transform kernels | Unify public lowering with native kernels; explicitly resolve parsed function/type gaps |
| Melt/explode/pivot | Existing flat melt, list/FSL explode and single-index/value pivot | Prepared/owned composition; separately define wider pivot/nested shapes |
| Source-order rolling | Existing sum/mean/count/min/max and centered lookahead | Prepared/owned execution with unchanged source order, nulls and min-periods |
| General joins | Native typed key owners, exact byte equality, row ordinals and native take | Duplicate-preserving equijoins first; then existing cross/non-equi shapes with explicit semantics |
| Set operations | Native batches and exact retained-row membership | UNION ALL/DISTINCT, INTERSECT and EXCEPT with explicit null, dtype and multiplicity rules |
| Analytic windows | Native partition/sort keys and late payload gathers | Existing parsed ranking, navigation and distribution functions; frame semantics remain explicit |
| Scoped subqueries | Native membership, semi/anti joins, scalar results and aggregates | Lower existing parsed forms with three-valued logic, scalar cardinality checks and explicit correlation scope |

For each row, acceptance must cover native execution, resident reuse, CLI, SQL,
DataFrame/Python, owned output/sinks, pressure/spill applicability, cancellation/
recovery applicability and complete-value UAT. A row is not complete because one
scenario matcher executes it. In particular, NULL-aware NOT IN cannot reuse an
ordinary anti-join without retaining the right-side null/empty-set information.

Source anchors: `shardloom-vortex/src/query_primitive.rs`,
`local_primitive_collect.rs`, `local_primitive_aggregate_owned.rs`,
`local_primitives.rs`, `shardloom-cli/src/sql_local_source_runtime.rs`,
`shardloom-cli/src/public_workflow_route.rs` and
`python/src/shardloom/query.py`. The public route rejects direct decoded
compatibility execution; historical parser/smoke coverage does not override that
boundary. Update conflicting front-door coverage documentation with each actual
native promotion.

### Composition prerequisite for joins

Add a source-based aggregate preparation boundary sharing existing lowering and
an owned-result-to-immutable-source boundary that borrows the current execution
context. The existing `MemoryFileGeneration` is a bounded one-Struct prototype,
not this general bridge. Preserve dtype for empty results, use typed chunked
children, bound rows/bytes/metadata and report native serialization costs. Do not
claim zero-copy composition or reopen a file for every downstream operator.

Join output must retain duplicate row identity, unlike COUNT/DISTINCT reduction
partitions. Scan selections contain sorted unique ordinals; use them for fetching
payload once, then native nullable take indices for duplicates and outer rows.
Reserve index, duplicate-chain, matched-row and output capacity before publication.
Define numeric/float equality, null matching, output names and order explicitly.
Extend `QueryRunStore` with a join adapter for partition/schema identities,
original ordinals, duplicate cross-products, unmatched rows and skew; existing
COUNT merge reducers cannot substitute for those contracts.

## Native run recovery ownership

Provider decision: `implement_shardloom_kernel` for cooperative query-workspace
ownership, reusing pinned Vortex 0.85 file/array/Flat-run providers and the existing
`QueryRunStore`. Vortex does not own ShardLoom's explicit recovery policy. No
encoding, runtime dependency or execution-provider change is required.

Hold an exclusive OS lock on the private run directory through the store's
lifetime. Recovery opens and exclusively locks that same directory before
inspecting the existing bounded ownership marker. A live store or competing
recovery is rejected before deletion; process exit releases the lock. The marker
continues to name only owned run identities. Revalidate ownership before removing
each run and preserve missing-file tolerance for interrupted cleanup. Cancellation
leaves a retryable marker and never turns recovery into execution replay.

This is cooperative same-user ownership, not protection against a hostile process
renaming entries between validation and unlink. Historical markers remain cleanup
compatible; a process running an older binary did not participate in this lock
protocol. The safe standard-library file lock API is available below the workspace
MSRV; no unsafe code or lock dependency is introduced.
