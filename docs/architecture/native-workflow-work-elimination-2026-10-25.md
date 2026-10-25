<!-- SPDX-License-Identifier: Apache-2.0 -->

# Eliminate repeated work across complete native workflows

Status: requested October 25, immediately after the combined ownership and
[dependency-maintenance PR #1605](dependency-intake-2026-10-25.md) and before
the [ingestion batch/window speed pass](native-ingest-batch-window-scheduling-2026-10-25.md).
The subsequent connected-programming, universal-I/O, rich-data, analytical,
Foundry and AI packages retain their order. Version milestones stay attached
to universal I/O, Foundry and AI acceptance.

The handoff reviews main `3239ade0` and the description/evidence of draft #1605;
it does not report new benchmark execution. Refresh its source observations on
the integrated baseline before implementing each mechanism. Existing RFC 0044
native ownership, RFC 0033 workflows, RFCs 0013–0017 streaming/resources/semantics/
optimization/recovery, and RFCs 0021–0022 expression/plan contracts apply. This
adds no phase ID, alternate executor or completion claim for CG-1 through CG-23.

## Verified source owners

The October 25 source check at `20f03396` confirms the following boundaries.
These are implementation observations, not measurements of their cost. In
particular, single-use batch inputs already have one-computation output replay;
the ordinary retained-source route still invokes execution per destination.

| Mechanism | Existing owner and current boundary |
| --- | --- |
| Multi-output execution | `shardloom-vortex/src/local_primitive_relational_writer.rs`, `PreparedVortexRelational::write_many_controlled`, invokes `write_controlled` per target. `local_primitive_relational_writer_fanout.rs::write_many_with_input` already shares one execution for single-use batch inputs. |
| Correlation | `shardloom-vortex/src/local_primitive_relational_correlated.rs`, `PreparedVortexRelational::run_subquery`, runs selected singleton parameters; `run_uncorrelated_subquery` separately reuses a completed result with evaluation guards. |
| External ordering | `shardloom-vortex/src/local_primitives/native_relational_spill.rs`, `Ordering::finish`/`complete_run`, finishes and reopens the final run. `local_primitive_relational_transform.rs::run_sort` passes `output_prefix` only to the in-memory branch. |
| Composed file scan | `shardloom-vortex/src/local_primitive_relational_scan.rs::run_file` uses ordered concurrency one and waits for each bounded range. The separate prepared primitive scan's configurable concurrency does not change this composed route. |
| Python delivery | `shardloom-python/src/stream.rs` owns acknowledgement/backpressure and an optional byte-bounded output window; `plan.rs::output_window_source_allowed` constrains its source route. |
| Projection and expressions | `python/src/shardloom/query.py` builds typed filter/project declarations; `shardloom-cli/src/sql_native_relational_pruning.rs` owns SQL backward pruning. Their equivalent coverage requires a consumer-specific review. |
| Plan composition | `shardloom-python/src/relation.rs`, `NativeRelation::child`, clones the typed tree. `shardloom-cli/src/native_runtime_plan.rs`, `NativeRelationDeclaration::new`, traverses structure/expressions and counts debug-format bytes. |
| First-use preparation | `shardloom-cli/src/public_relational_sources.rs`, `Sources::resolve`/`prepare_compatibility`, prepares non-Vortex file inputs through the existing native preparation owner. |

## Decision and evidence boundary

Optimize complete first executions by eliminating repeated computation,
intermediate materialization, avoidable reads/writes and repeated plan work.
The accepted first-use/later-use characterization is not a controlled
storage-cold experiment: its differently selected calls and operating-system
cache state cannot support that comparison. The later source-retention
candidate reduced opens but failed its benefit and first-use controls and was
removed. Preserve that finite decision; it does not exhaust every possible
reuse mechanism.

#1605 measures 61.2% fewer copied bytes and byte-identical output, but fails
the frozen initial 3% complete-ingest speed gate. Its explicit retention is a
copying/correctness decision, not proof of a repeatable speedup. Prefer changes
that remove an execution, pass, intermediate result or read/write cycle. Neither
cache-hit counts nor copied-byte counters alone establish workflow benefit.

## Package 1: execute once

**One computation for one explicit multi-output request comes first.** The
reviewed ordinary relational `write_many` path calls a writer for each
destination and repeats plan execution while sharing retained sources and the
resource budget. Reconcile it and primitive per-destination callbacks with the
existing single-producer/native result-delivery machinery. This promotes the
multi-output implementation previously queued under universal I/O into this
earlier package; do not implement it twice.

Execute scan → transform → join → aggregate once, then feed every requested
destination from that result. Use credited retention for a small result,
bounded native batches, or explicitly authorized native replay/spill when
consumers cannot progress together. Sequential writers may consume one completed
result; concurrent writers are not a requirement. Prove one computation through
the public request, complete independent readback of each output, and bounded
resources. Preserve each destination's cancellation, failure and publication
contract. Sharing computation does not make independent publications atomic.

Then admit explicit combined branches over the same source snapshot: for
example, one filter/join prefix feeding two summaries and a detailed export.
Keep optimizer visibility through lazy plans and the existing context, rather
than requiring users to collect an intermediate manually. Share only within
that declared execution, where cost and ownership justify it; later independent
executions recompute normally. Define source/effect identity, branch demand,
consumer abandonment and last-owner release. This introduces no global answer
cache. [Polars combined collection](https://docs.pola.rs/api/python/stable/reference/api/polars.collect_all.html)
is a conceptual capability reference for shared subplans, never a runtime
dependency or fallback.

**Decorrelate eligible subqueries as plan rewrites.** The reviewed correlated
runner iterates selected outer rows, builds a singleton parameter and executes
the inner query per row; dynamic inner shapes may bind inside that loop.
Uncorrelated subqueries already have a distinct reusable path. Start with
equality-correlated aggregates and eligible EXISTS/membership families that
can become ordinary native aggregation and joins. Keep the correct existing
execution for unsupported correlation shapes. Do not replace this work with
a last-parameter cache.

Preserve duplicate outer rows, NULL and empty-input semantics, scalar-cardinality
errors, guarded evaluation, error order where required, and effect restrictions.
Do not hoist work across an evaluation guard merely to share it. Use explicit
admission/diagnostics and family-specific positive/negative controls. The
[DuckDB decorrelation explanation](https://duckdb.org/2023/05/26/correlated-subqueries-in-sql)
illustrates the algorithmic category; ShardLoom must implement and certify its
own native rewrites. Full43 does not cover all such reporting workflows.

## Package 2: keep data moving

**Stream the final external ordering merge to its consumer.** The reviewed
spill path persists a final merged run, reopens it and reads it for delivery.
Investigate removing that complete final write/read cycle while preserving the
existing in-memory no-spill fast path. This is distinct from the rejected
merge-order investigation; it changes the delivery boundary, not the pairing
order of the same writes.

Reuse the owned run store and bounded merge readers. Hold input runs until the
consumer finishes or cancels, account for readers/output buffers, preserve
stable order and ties, and clean up after every failure. Specify which complete
input/generation/corruption validations must finish before observable output or
publication. Early consumer termination cannot silently suppress required
errors. A streaming final merge must not leave unfinished work outside the
measured lifecycle.

After that mechanism, investigate finite demand in external ordering. The
reviewed composed in-memory sort receives `output_prefix`, while the spill
branch does not carry it to ordering. Propagate eligible demand and reduce
unnecessary payload reconstruction without skipping required full-input
validation. Do not relabel the earlier in-memory limited-sort work as spill
support or begin with a broad replacement sort algorithm.

**Overlap scan and delivery work within existing resource ownership.** The
reviewed composed file scan uses ordered concurrency one and waits for bounded
row ranges before downstream delivery to avoid speculative reads competing
with operator state. Investigate a small byte-accounted window only where it
reduces observed producer/consumer waiting. Preserve requested order, prompt
release under backpressure, cancellation and progress within the same CPU and
memory grants. The existing optional Python output window is a foundation to
extend, not evidence that all output overlap is missing.

Separate physical read size, native operator work size, result-delivery batch
size and destination storage units. Small requested output batches must not
unnecessarily force tiny upstream work units; large internal work units must
not force consumers to retain their entire backing allocations. Coordinate
the shared policy with the later ingestion pass without adding another pool
or buffering layer.

**Reduce intermediates where real consumers permit it.** Compare effective
column requirements for typed/DataFrame compositions with the existing SQL
backward-pruning implementation, including sort/window keys and required
validation. For wide joins feeding narrow aggregates, eliminate demonstrably
unused payload before retained state. This is an investigation into equivalent
coverage, not a claim that column pruning is universally absent.

Carry selections, dictionaries and other encoded owners through compatible
consumers instead of automatically reconstructing payload. Charge the complete
retained backing allocation; copying a small selection can be better than
retaining a large owner. Fuse eligible adjacent filters/projections and feed
single-use expression values into consumers through existing programs, masks
and typed operators. Preserve lazy branches, NULL behavior, floating-point
evaluation order and required errors. Separate nodes alone do not prove a copy,
and this package does not introduce a new JIT engine.

## Package 3: make setup cheap

**Use shared immutable plan structure with incremental metadata.** In the
reviewed native relation wrapper, `child()` clones the accumulated plan, and
new declarations revisit nodes/expressions and debug-format into a byte-counting
sink. The sink avoids allocating a complete debug string but still performs
the traversal and formatting work. Verify construction cost on realistic deep,
wide and shared plans before selecting the representation change.

Add a declaration referencing immutable children and update structural facts
without repeatedly copying or formatting the whole prior tree. Preserve exact
source dependencies, metadata admission, validation, full semantic binding
where required, and safe resource accounting for shared nodes. Mutable execution
state remains per operation. This is a prerequisite foundation for the later
[connected programming package](native-connected-programming-model-2026-10-24.md),
which still owns named plans, SQL/DataFrame composition and user interfaces.
Measure construction/binding and small complete workflows; a scan-dominated
Full43 is not sufficient evidence of improved notebook/setup cost.

**Choose appropriate first-use input preparation through universal I/O.**
Strengthen the existing [selective input strategy](native-universal-io-performance-coverage-2026-10-24.md#then-first-use-strategies-family-coverage-and-dataset-access),
not a separate connector project. The reviewed public non-Vortex resolver
prepares a native file before ordinary relational execution. Admit a suitable
narrow one-off workflow as selected source columns/regions → bounded native
normalization → existing operators → result/output, without first persisting
an entire reusable copy. Durable preparation remains valid for repeated broad
work. The shared execution setup/admission belongs here; format-specific
pushdown, streaming readers and broader adapter coverage remain with universal
I/O and reuse this contract.

Keep strategy choice under the ordinary input interface. Preserve native
Vortex representations, generation identity, exact values/schema, validation,
effects and complete-workflow accounting. No lower-quality engine, hidden
preparation, advanced Vortex tuning requirement or projected artifact posing
as a complete source is admitted.

## Finite acceptance and sequencing

Prioritize single-computation multi-output writes, final-merge streaming and
cheaper native plan composition as immediate concrete targets. Branch sharing
and decorrelation follow their semantic prerequisites; scan/output overlap and
intermediate elimination require an activating workflow. Use the three packages
as coherent implementation boundaries, not a requirement to combine every
operator and public API change into one PR.

For each mechanism, freeze complete-value/schema/order, ownership, error,
resource, lifecycle/publication and benefit/control gates before measurement.
Use cases that actually activate it: expensive shared producers and several
independently read destinations; correlated duplicate/NULL/cardinality cases;
spilled large sorts with early/failing consumers; deep and shared plan building;
large streamed outputs; wide-to-narrow compositions; selective fresh inputs.
Measure useful work eliminated and complete workflow behavior, preserving all
failed observations and first-use controls. Keep single-request latency and
multi-request throughput separate, with CPU and retained-memory tradeoffs in
their diagnostic evidence.

Retained runtime changes receive the usual isolated Full43 engine-time
regression checkpoint. Do not combine its independent queries into shared
execution and present that as the same benchmark. Its 381 returned rows cannot
characterize bulk output or platform integration. No additional cache, thread
pool, transport protocol, broad compiler campaign or predicted speedup is
required by this handoff. Keep native Vortex, explicit effects and no external
query-engine fallback throughout.
