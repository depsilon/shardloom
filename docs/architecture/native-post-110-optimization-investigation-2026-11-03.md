<!-- SPDX-License-Identifier: Apache-2.0 -->

# Where shared native execution loses useful source facts or does extra work

## Saved-record results

The offline replay reconstructed 406 operations from 2,628 retained files.
It compared every ClickBench and broad-query result with its independent
reference and verified both preparations against their original acceptance,
process, runtime, source-generation and artifact records. It also compared the
earlier preparation with the exact stdout used for D/E ownership attribution.
The investigation ran zero engine queries. The three original anchors and
their checked-in archives supply 183 reused files. A content-addressed
supplement preserves the other 2,445, including worker requests, receipts and
independent expectations. [Component report](../benchmarks/evidence/native-post-110-optimization-2026-11-03/report.json) ·
[Operation table](../benchmarks/evidence/native-post-110-optimization-2026-11-03/operations.jsonl.xz).

| Cohort | Operations | Complete rows | Engine seconds | Scope |
| --- | ---: | ---: | ---: | --- |
| Direct Parquet probe | 7 | 46 | 98.624293789 | Original Parquet, parent ci-native build |
| Prepared Vortex probe | 7 | 46 | 9.732908874 | Same seven statements and binary, different representation and providers |
| Fresh preparation | 1 | 99,997,497 input | 38.936 creation + 0.085 publication | Clean release workflow |
| Fresh prepared Full43 | 43 | 381 | 44.580467538 | First complete pass against that new artifact |
| Fresh analytical cohort | 347 | 12,565 | 12.006862126 | Five original Parquet inputs, 2 GiB and two lanes |
| Prior preparation used for D/E | 1 | 99,997,497 input | 36.945 creation + 6.695 publication | Earlier runtime and artifact |

All 406 complete records expose `execution_resource_*` fields. None of the
corresponding compact projections retain that family. The ClickBench summaries
include native I/O decision rows. The 347-case physical summaries retain only
`native_source_demand_*`, although every full worker report contains the
decision schema. No record in these cohorts contains the writer-specific
`native_vortex_result_export_replay_decision_*` family. These prepare and
collect operations cannot measure activation of that writer capability.

### Ranked decisions by shared component

1. **Source-to-operator admission has the largest observed route difference.**
   Q19 and Q34 contribute 38.729017292 and 27.768237459 engine seconds to
   the 88.891384915-second net difference across the seven pairs. The direct
   reader carries projection, a held generation, validation, bounded batches
   and detached ownership into relational execution. The specialized provider
   requires one native Vortex file and no batch or memory source. The records
   establish the admission boundary. They do not establish equivalent encoded
   string domains, an equivalent epoch or an equivalent prepared minute
   accessor on the incoming batches. Preserve that restriction. A future
   admission needs a named operator, an equivalent proof under its input
   contract and a same-representation control. Q1 already uses a metadata
   count. Q3 requests two of 105 columns, copies zero visible buffer bytes and
   is 0.399643625 engine seconds faster direct.
2. **Measure copy reasons first at the shared Arrow admission function.**
   Four direct cases account for 20,449,529,657 visible bytes copied. Q24
   contributes 9,938,729,407, Q34 contributes 9,036,164,542 and Q13 and Q19
   contribute 737,317,854 each. The broad cohort has 36,047,398 bytes across
   145 cases. Its string and projection families contribute 20,489,283 and
   12,304,799 bytes. Both preparations independently report 30,974,099,552
   bytes copied and 72,299,123,989 transferred. Full new copy charges were
   30,985,042,016 bytes in each. The source, ingest and decision counters
   overlap at this boundary. The parent probe's 48,305 representation-retention
   declines record the set `required_representation_copy`. They do not count
   unique backing allocations. The first information that can change transfer
   eligibility is the failed proof at the actual copy branch, weighted by
   region count and visible length. No saved observation can recover that
   split. [Shared ownership implementation](../../shardloom-vortex/src/ingest_arrow_ownership.rs).
3. **Selected payload reads have a measurable upper bound.** Direct Q24
   reads 3,055,708,051 rank bytes in 226 group visits, then 458,940,301
   payload bytes in seven visits. It decodes 99,997,497 and 3,250,534 rows
   respectively for ten winning positions. Five broad cases independently
   use the same path. Their payload phase decodes 40,960 rows for 135 positions
   and reads 1,163,011 bytes. Those payload byte totals bound what a deeper
   selection could remove from the reported reads. The captures lack positions
   relative to compressed pages and a proof that finer reads preserve the
   current winning-group validation/errors. Defer reader changes until those
   constraints and the physical page floor are known.
4. **Preparation work narrows the ownership question.** The fresh report
   records 64,028,919,946 text-Zstd work nanoseconds and 23,146,711,661
   active nanoseconds across selected text scopes. Numeric compression records
   36,947,464,486 work nanoseconds. Stream Arrow conversion and Arrow ownership
   record 22,930,577,408 and 22,900,584,305 work nanoseconds across the same
   503 batches. The conversion boundary includes ownership work. These
   overlapping spans cannot be added or subtracted from 38.936 creation
   seconds to predict savings. Pair the copy measurement with the existing
   ownership span. Keep the selected text packing, frame index and writer
   digest. Publication reread zero artifact payload bytes for fingerprinting.
5. **Worker coordination warrants operator-specific evidence.** Three prepared
   probe operations and 12 fresh Full43 operations expose aggregate worker
   fields. Fresh Q34 reports nine peak active workers, 1,848,250 caller-merge
   nanoseconds and 3,207,692,452 caller-join-wait nanoseconds. Its accumulated
   partition lock wait is 7,204,486,543 nanoseconds across workers. Those spans
   have different clocks and can overlap. Q19 has a different aggregate owner
   and reports submitted chunks without that timing breakdown. This evidence
   does not identify a producer blocked on a full queue. Defer queue policy
   changes. A queue-pressure comparison would require pool-lifetime peaks and
   enqueue-wait deltas bound to the same owner and interval.

The fresh Q19 scan and materialized-accessor lists select the minute helper
and exclude raw EventTime. They measure the selected columns, while per-column
payload reads and decode work remain unavailable. Fresh Q34 and Q35 expose a
root-only layout inventory and `dictionary_layout_availability_not_inspected`.
Zero native-dictionary worker chunks cannot establish an eligible persisted
frequency consumer. Neither missing attribution justifies new metadata.

The decision records also distinguish activation from opportunity. Across the
seven direct cases, the metadata-count owner considered, admitted and executed
once with `exact_held_metadata`. Three operations report 904 region decisions,
of which 243 were admitted and executed and 661 declined. The merged decline
reasons include `required_value_evaluation` and `exclusion_not_proved`.
Assigning 661 refusals to either individual reason would overstate the record.
The specialized-provider refusal is a separate plan-level decision. It can
coexist with successful metadata, ordering or representation decisions in the
same relational execution. [Native decision contract](../../shardloom-core/src/native_io_decisions.rs).

Run `python3 -B scripts/verify_native_saved_operations.py` with Python 3.11 or
newer from a checkout containing the three anchors. Add `--operations PATH`
to export the verified table to a new JSONL file. Each row retains its receipt
labels, process ID, runtime, input generation, workload identity, declared
and admitted limits, owner fields and scope. For example, both Q24 probe
requests allowed twelve lanes. The direct route admitted ten and the prepared
route admitted twelve. The table keeps these limits separate from observed
worker activity. The report defines units and source fields for the comparable metrics.
A numeric zero is an observation. The strings `omitted`, `inapplicable` and
`unavailable` preserve the three different coverage gaps. Native I/O decision
counts are instrumented owner outcomes, and each reason list is a set. The
six cohorts contain no certified preceding same-declaration resident snapshot
pair. Every lifetime delta therefore remains null.

### Forward measurement selected from the replay

The admitted change measures the shared Arrow copy boundary. Preparation
reports `public_workflow_preparation_vortex_ingest_ownership_copy_reasons`.
Completed relational batch intake reports `native_input_columnar_copy_reasons`.
Each field contains a JSON map with `regions` and `visible_bytes` for each
observed reason. The corresponding `copy_reason_scope` field identifies the
boundary. One region contributes to exactly one bucket. A copied zero-length
region increments its region count. A completed Arrow boundary with only
transfers emits an empty map. A route that observed no Arrow regions omits
the map. Transferred regions continue to use their existing counters.
Historical envelopes predate both fields.

| First failed proof | What the measurement can decide |
| --- | --- |
| `policy_requires_copy` | The caller supplied the data through a validated-copy boundary. Changing that category requires new producer provenance. |
| `empty_region` | An aligned empty buffer requires its existing minimal allocation. Its visible byte volume is zero. |
| `nonzero_pointer_offset` | A slice starts inside the allocation. Its visible length cannot price the complete retained owner. |
| `unknown_alignment` or `misaligned_pointer` | The physical type lacks an admitted alignment or the pointer fails its known requirement. |
| `unique_standard_owner_unproved` | Arrow could not recover an exclusively owned standard allocation. Shared and opaque owners can both reach this condition. Its byte volume is only an upper bound on eligible shared storage. |
| `capacity_exceeds_copy_charge` | The complete standard capacity is known. Keeping it would cost more retained credit than the aligned copy. |

The same fixed counters cover value, offset, validity and child buffers. Ingest
merges them across batches. Relational execution merges completed intake into
its own report. The branches retain their existing validation, reservation,
copy and release behavior. Compare the visible-byte sum with the existing copy
total and pair preparation counts with `stream_arrow_ownership_work_nanos`.
The older 406 operations supply no measured per-reason byte volumes.

**Decision for the next eligible capture.** If `unique_standard_owner_unproved`
accounts for material work, determine whether a controlled producer can supply
complete backing identity, capacity and lifetime for that subset. If slices,
alignment, copy policy or excess capacity dominate, investigate that specific
producer or preserve its restriction. A change in transfer eligibility still
requires the frozen complete-workflow correctness, resource and engine-time
comparison. The saved analysis and reason-reporting tests establish no speedup.

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
