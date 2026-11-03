<!-- SPDX-License-Identifier: Apache-2.0 -->

# Where shared native execution loses useful source facts or does extra work

The maintainer schedules this investigation immediately after the **1.1.0 source
milestone and before item 19**. Keep it one work unit under the existing source,
operator, ownership, preparation and compute-pool performance owners in the RFC
0044 program. The first deliverable is a reproducible analysis of saved evidence.
The analysis can select a shared mechanism, identify one measurement needed to
select it, or close a candidate with a documented reason. [Active queue](phased-execution-plan.md#planned).

## Start with complete saved operations

Freeze an input index before analysis. Bind each operation to its original
cohort, workload and ordinal, runtime and profile, source/input generation,
artifact, resource grant, receipt, complete result and reference validation.
Use the full stdout envelope or worker outcome when available. Compare its
coverage with the corresponding summary. Record missing originals or malformed
fields explicitly and preserve previous observations.

Keep these sources distinct:

- The fourteen-operation source-strategy probe compares original Parquet and
  existing Vortex for the same seven queries on the checked `ci-native` parent
  runtime. Each route has 46 independently validated rows, 24 GiB and a maximum
  of twelve workers. [Probe and identities](native-source-strategy-investigation-2026-11-02.md).
- The clean release runtime's fresh preparation, publication and 43 prepared
  queries constitute a separate workflow with 381 independently validated
  result rows. Its creation, publication and summed query engine times are
  38.936, 0.085 and 44.580467538 seconds. Keep the earlier retained preparation
  report used for copy attribution under its own hash and runtime. [Workflow
  acceptance](../benchmarks/native-preparation-contracts-2026-11-02.md) ·
  [Ownership observations](native-preparation-query-investigations-2026-11-02.md#de-source-and-preserved-run-findings).
- The fresh 347-case cohort exposes additional analytical shapes on its own
  five Parquet inputs with 2 GiB and two lanes. It validates 12,565 rows and
  totals 12.006862126 engine seconds. Keep its denominator, runner, data and
  observation independent of the seven-query probe and Full43. [Fresh 347](../benchmarks/evidence/native-preparation-fresh347-2026-11-02.json).

The query runner's `execution_report()` projects selected prefixes and exact
keys. `execution_resource_*` and `native_vortex_result_export_replay_decision_*`
fall outside that filter. The full envelope may contain them when the route
emits them. Inspect actual presence and applicability for each operation.
Start with the saved files before changing either the runner or the engine.
[Compact report](../../scripts/run_clickbench_query_uat.py) · [Process observations](../../scripts/timed_native_command.py).

Produce a machine-readable operation table and a readable component ranking.
Keep provider/route, admission and decline reasons, engine and owner timings,
physical I/O, decode, copy/transfer, batch geometry, worker/queue and pressure
observations alongside their units, source field and measurement scope.
Separate process CPU, RSS, faults and block-operation counts from engine clocks,
native reservations and physical reader bytes. Preserve observed zero, omitted,
inapplicable and unavailable as distinct states. Deduplicate the same observation
represented in both full and compact evidence. A resident lifetime delta needs
the correct preceding snapshot, same owner and an uninterrupted comparison
boundary. Peak values and overlapping work spans need their declared semantics.
[Resident counter rules](../../scripts/native_report_evidence.py).

## Five component investigations

| Initial priority | What the evidence establishes | Decision to make from full records and source |
| --- | --- | --- |
| Direct-source access to existing operators | All seven direct cases record `multiple_or_nonfile_sources_require_relational_execution`. Direct Q19 and Q34 consume 12,218 batches each, up to 8,192 rows, with one input batch held. Q19/Q34 account for 66.497254751 engine seconds, 74.81% of the **net seven-query** route difference. Q3 is faster direct, requests 2/105 columns and observes zero input-buffer copy bytes. | At each producer and consumer identify which type, representation, selection, order, domain/epoch, generation and ownership facts exist, survive conversion and satisfy the actual operator's admission. Distinguish a legitimate decline from a lost fact or an unrecognized equivalent proof. Preserve Q3 and metadata-count controls. The probe changes representation, helpers and operator choice together. Nine observed active aggregate workers on prepared Q13/Q34 describe that owner only. [Provider admission](../../shardloom-vortex/src/local_primitive_prepared_relational.rs) |
| Residual ownership copies | The retained preparation records 30,974,099,552 copied and 72,299,123,989 transferred region bytes. Direct Q34 records 9,036,164,542 visible input-copy bytes. These cumulative scopes exclude various provider and downstream costs. | Attribute copy counts and bytes to disjoint, observable reasons at the existing handoff. Separate offsets/alignment, capacity economics, failed uniqueness, external/unknown ownership and necessary representation conversion where the APIs actually distinguish them. Preserve an unknown bucket. A lifetime token alone cannot prove total backing capacity. Any sharing change requires complete reservation and release proof and a measured eligible subset. [Buffer admission](../../shardloom-vortex/src/ingest_arrow_ownership.rs) · [Owned batch](../../shardloom-vortex/src/universal_format_io/owned_batch.rs) |
| Selected payload reads | Direct Q24 ranks 99,997,497 rows and visits 3,250,534 payload rows for ten winners. `prepare_selected` validates ordered positions and selects containing row groups. The rank and payload phases report their own rows, groups and read bytes. | Inspect phase bytes, projection and region geometry. Check the pinned provider's ability to act on exact winning positions below row-group granularity. Account for compressed pages, decode floors, duplicate/order semantics, required validation and errors. Estimate reducible work before introducing a mask or gather. [Selected source](../../shardloom-vortex/src/universal_format_io/demanded/ranked.rs) · [Rank/payload accounting](../../shardloom-vortex/src/local_primitive_relational_batch_ranked.rs) |
| Preparation stages | Thirteen native work scopes cover validation, projection, Arrow conversion, reader-lock/ordered handoff waits, text canonicalization/compaction/Zstd, numeric probe/compress/preserve, dictionary preservation and ownership. Work spans can overlap. Most provider-stage bytes are array-memory estimates. | Examine calls, rows, input/output units, `work_nanos` and active-time boundaries against the creation stage. Look for repeated representations, constrained admitted workspace and ordered handoff delays. Keep actual copied regions, allocation charges and provider estimates separate. Select a stage for further attribution only when its cost and eligible work support a decision. [Ingest stage evidence](../../shardloom-vortex/src/vortex_ingest_stage_timing.rs) |
| Worker and pool coordination | Instrumented aggregate owners expose busy, inline, submit, join-wait, merge, hash/equality and selected partition costs. Pool snapshots also hold queued job/byte peaks, enqueue wait and currently waiting producers. Report coverage varies by owner. | Distinguish source starvation, full queues, caller merge, contention, accessor construction and pressure-driven window changes using actual observations. Export an already-collected pool field only where its lifetime and sampling point answer the question. Use the existing PulseWeave/capillary owners for any selected change. [Aggregate report](../../shardloom-vortex/src/local_primitives/aggregate_count_workers.rs) · [Pool snapshot](../../shardloom-exec/src/compute_pool.rs) |

Q19 and Q34 illustrate investigation priority within the parent-runtime probe.
The saved comparison provides no equivalent general-provider control on Vortex.
Group findings by shared component, retain opposite-sign controls and keep
each cohort's denominator explicit. No route-wide concurrency conclusion,
break-even threshold or release-profile attribution follows from those two cases.

## Capability decisions and finite follow-through

Classify each capability opportunity using the instrumented owner boundary.
The decision schema counts outcomes, accumulates optional observed work, and
stores bounded reason sets. One reason appearing in a merged set does not
establish its frequency or byte volume. Respect empty/lazy work and the scope
of each accumulator. [Decision contract](../../shardloom-core/src/native_io_decisions.rs) ·
[Serialization](../../shardloom-cli/src/native_decision_evidence.rs).

| Observed outcome | Required disposition |
| --- | --- |
| Capability absent from the observation | Check workload opportunity, route and capture coverage. Mark missing attribution. |
| Considered and legitimately declined | Preserve the semantic or resource restriction. Record its cost if measured. |
| Declined after loss of a provable fact | Identify the producer, losing boundary, consumer and exact equivalence proof. |
| Admitted without execution | Inspect empty input, lazy demand and actual operator work before diagnosing a wiring gap. |
| Executed with little useful work avoided | Quantify admission, conversion and coordination cost against eligible savings. |
| Executed with lower work and better complete-workload timing | Apply the existing correctness, resource, ownership, no-fallback and complete-cohort acceptance gates. |

First rank candidates by attributable cost, demonstrated eligible work,
coverage, confidence and remaining proof. Then add the smallest bounded
measurement that could change a decision. Candidates include per-reason copy
counts/bytes, per-column read/decode work, reasons weighted by applicable work,
and existing queue fields absent from an owner report. Specify the unit,
lifetime, overlap, reset/merge semantics and negative controls before adding
one. An opaque owner stays unknown until its provider supplies a valid proof.

Retain the demonstrated writer-digest publication path, sparse frame indexing
and fresh Q19 minute-helper substitution. The outstanding Q19 question is
per-column work. Q34/Q35's zero dictionary-worker counters and `not_inspected`
physical status leave persisted frequency metadata deferred pending actual
domain, consumer and first-use cost evidence.

Acceptance is a reproducible saved-evidence analysis, a ranked component table
and a disposition for each lane. A proposed mechanism needs a named common
implementation owner, evidence of eligible work and a prospective complete-
workflow check. Preserve source generation, resource ceilings, full backing
charges, exact values/order/errors, cancellation and late output ownership.
Preserve explicit source investment and native Vortex execution. Use the
existing binary complete-cohort gain rule when applicable and retain earlier
scoped tradeoffs. The initial analysis executes zero new engine queries. Any
later timed comparison needs its own justified protocol and authorization.
