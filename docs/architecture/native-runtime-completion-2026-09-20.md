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

Implemented on the working branch. Local tests, native and workspace Clippy,
formatting and bounded serving load checks pass; frozen-binary UAT and final
acceptance review remain pending:

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
No performance improvement, full UAT completion, merge or release is claimed yet.

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
