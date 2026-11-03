<!-- SPDX-License-Identifier: Apache-2.0 -->

# Source and preparation strategy investigation

Status: the November 2 bounded investigation is complete, with explicit source
choices retained. It was brought forward before the next Full43 measurement,
alongside the preparation/query investigations and justified cleanup. This is an investigation within the existing
source/workflow/planner owners, not a new implementation phase or competitive
gate. The [phase plan](phased-execution-plan.md#planned) owns the queue.

The maintainer originally requested this review after the version bump, then
explicitly moved it before the next Full43 measurement. The investigation
dispositions and resulting cleanup are complete. The subsequent release-build
[preparation-plus-Full43 observation](../benchmarks/native-preparation-contracts-2026-11-02.md)
passes local acceptance and retains A/B/C under the combined engine-time rule.
It does not alter this investigation's explicit source choices.
The supplied review's revision-specific CI status, timing table and Full43 hold
are historical context; verify current source and saved results when this work
starts. They are not current acceptance evidence or automatic policy thresholds.

## Decisions to investigate

Keep three concerns distinct in the public contract:

- Source investment: read the original generation, explicitly prepare it, or
  validate and reuse an existing prepared artifact.
- Physical execution: the shared planner selects eligible native scan,
  aggregation, join, ordering and spill strategies from proved capabilities.
- Output: collection, streaming and format selection remain independent of
  whether the source was prepared.

Clarify existing source policy names and their actual behavior, with an explicit
compatibility plan before changing callable identifiers or accepted values.
Document that ordinary direct routing and adaptive work sizing do not themselves
constitute an optimizer choosing whether preparation pays for a workload.
Keep preparation explicit and preserve source generation, ownership, resource,
validation and no-fallback contracts.

## Evidence before implementation

Start with a small representative set covering broad scans, grouping, string
and temporal expressions, ordering, and selective retrieval. Compare on one
accepted build with the same logical generation, CPU/memory ceilings, complete
results and declared cache context:

| Case | Source and execution | Question |
| --- | --- | --- |
| A | Original source, ordinary direct relational execution | What does direct work cost? |
| B | Prepared Vortex, equivalent general relational execution where an admitted diagnostic control exists | What changes with representation/preparation while keeping operator families comparable? |
| C | The same Vortex artifact, normal eligible provider selection | What does specialized provider selection contribute? |
| D | Fresh preparation/publication followed by the declared workload | Does preparation repay its complete engine cost? |

Do not force B by enabling spill: that changes resource policy. Any new diagnostic
control needs its own narrow design and correctness proof. Record or control
derived-field rewrites and other physical differences before attributing costs.
Capture source owner, selected provider and decline reason, derived substitutions,
columns/regions read, decode/intake work, actual workers and batch geometry,
copies, materialization, spill and pressure where observable. Missing counters
remain unavailable rather than inferred from names or configured lane limits.

Report preparation/publication engine time separately from query engine time.
For workload costs P, D_i and V_i, preparation repays its investment when
sum(D_i - V_i) > P. Their sum is engine work, not application elapsed time.
Historical observations from different revisions cannot establish that threshold.

### Bounded investigation protocol before final measurement

Run the original Parquet route and the accepted existing Vortex route on the
same already validated dependency-batch runtime, one execution each of Q1, Q3,
Q13, Q19, Q20, Q24 and Q34. These cover metadata count, broad numeric measures,
text grouping, temporal compound grouping, selective retrieval, ranked wide
retrieval and high-cardinality text grouping. This is fourteen executions and
two seven-query observations, not another Full43 or a repeatability study.

The selected runtime is the retained dependency-batch r6 binary, SHA-256
`4be92d1d5ffc8c0d4b5fb0576a08b58b968aec321587a816bda576aa7f94187e`,
with its original build revision, exact source overlay and installed-wheel proof.
It uses `ci-native` with checked arithmetic and debug assertions. Both routes use
that exact executable, 24 GiB memory, a maximum parallelism of 12 and the standard
120-second query deadline. The standard selected-query runner compares every
returned value with the frozen independent reference. Freeze both source hashes,
runtime, submitted SQL and harness before execution; verify them afterward.
Keep all other task work idle during queries, with the existing lightweight
workload/storage guard. Do input hashing and broad inventories outside execution.
The operating-system cache is uncontrolled; do not flush it or add pacing.

This probe establishes current parent-runtime route evidence before selecting
further cleanup. It excludes the pending A/B/C implementation and uses the
existing artifact's original helper schemas. It cannot accept A/B/C performance,
provide a release-profile result or determine preparation's break-even point.
There is no admitted equivalent general-provider control for B; record it as
unavailable rather than changing spill, floating-value policy or query semantics.
Any A/C difference includes representation, helpers and operator selection and
does not isolate their individual costs. Fresh preparation and the final
release-build Full43 remain a subsequent, separately frozen workflow measurement.

### Observed route evidence and disposition

The [matched probe](../benchmarks/evidence/native-source-strategy-probe-2026-11-02/acceptance.json)
passes all fourteen executions, complete independent result replay, unchanged
input/runtime/harness identities and drained process groups. Each route returns
46 reference rows across its seven queries. Engine seconds are:

| Query | Original Parquet | Existing Vortex |
| --- | ---: | ---: |
| Q1 metadata count | 0.001633874 | 0.000013542 |
| Q3 broad numeric measures | 1.058663332 | 1.458306957 |
| Q13 filtered text grouping | 9.945541667 | 0.469869834 |
| Q19 temporal compound grouping | 42.576934917 | 3.847917625 |
| Q20 selective retrieval | 1.325318958 | 0.071384625 |
| Q24 ranked wide retrieval | 13.068311374 | 1.005764083 |
| Q34 URL grouping | 30.647889667 | 2.879652208 |
| Seven-query total | **98.624293789** | **9.732908874** |

These are parent-runtime `ci-native` observations under the limitations above.
Q3 is faster on the original source in this observation. The selected seven-query
total cannot select a universal default or supply a Full43 score, and no fraction
of the difference is attributed to decoding, concurrency or a specific operator.

The recorded work narrows the next decisions:

- All seven Parquet reports select general relational execution with
  `multiple_or_nonfile_sources_require_relational_execution` and write zero
  prepared-input bytes. Source inspection confirms the single-native-file
  admission condition and the unchanged schema, semantics and resource checks.
- Direct Q1 reads no payload rows or payload bytes. Direct Q3 requests two of
  105 columns and transfers 399,989,988 visible native input buffer bytes with
  zero input buffer copy bytes. Direct execution already avoids useful work;
  source format alone does not establish a costly path.
- Direct Q19 and Q34 each consume 12,218 batches, with at most 8,192 rows and
  one retained input batch. The Vortex Q13/Q34 reports record nine peak active
  aggregate workers. These facts describe particular owners and measured
  workers; the configured ceiling does not prove equivalent parallel execution.
- Direct Q34 copies 9,036,164,542 visible input buffer bytes. That scope excludes
  decoder internals and later operator copies. The report still cannot separate
  eligible shared owners from required alignment, slicing or provenance copies.
  This strengthens the need for attribution before changing the ownership proof;
  it does not authorize retaining a whole batch or uncharged backing storage.
- Q24 retains ranked retrieval: 99,997,497 rank rows, 3,250,534 payload rows and
  ten selected rows. Its 103,248,031 decoded-row total includes both stages.
  The source route is not simply decoding every column for every ranking row.
- Existing Vortex Q13/Q24 report stored length-helper rewrites. Existing Vortex
  Q19 still lists `EventTime` and `UserID` as numeric accessor inputs, consistent
  with the old nullable-helper guard. C's subsequent fresh-artifact observation
  proves removal of that raw time input from scan/accessor demand; this earlier
  probe does not do so and supplies no per-column I/O/decode measurement.
- Existing Vortex Q34 records zero native-dictionary worker chunks and values.
  The probe supplies no physical frequency-domain or first-use cost proof for E.
  Missing source/decode/worker counters remain unavailable rather than zero.

**Disposition before final measurement:** retain explicit source investment and
normal capability-based provider selection. No automatic preparation threshold,
provider-forcing switch, lane increase, dictionary-frequency payload or shared
allocation admission is introduced. The observations justify prioritizing native
batch-to-operator capability continuity and residual-copy attribution in future
source work, but do not isolate a safe additional optimization to ship here.
That follow-up must reuse current source/resource/compute owners, preserve all
provider equivalence checks and include the broader route/scale coverage below.
The existing 347-case denominator and all earlier results remain unchanged.

The concrete pre-measurement code cleanup is the A receipt-retention fix:
report-only writes select the existing validated commit without constructing a
discarded receipt. A/B/C contracts and controls continue to final acceptance;
D/E and the additional source strategy mechanisms have explicit no-change
dispositions. Broad local validation and completed test-cache retirement now
pass. The subsequent clean release build, 303 installed integration tests and
frozen preparation/Full43 workflow also pass. That workflow totals 83.601467538
engine seconds versus the retained 84.232222712, with the higher query total
recorded and accepted. The historical source probe remains separate from that
observation and establishes no automatic preparation payback threshold.

### Existing public source choices

| Intent | Existing behavior |
| --- | --- |
| Query an original local compatibility source | Ordinary default routing, canonically `vortex_middle`, reads demanded input directly without durable preparation. |
| Prepare explicitly, then query | `prepare_once`, or the explicit preparation APIs and a chosen Vortex target, create or validate reusable prepared state. |
| Query an existing native artifact | Vortex input / `read_vortex(...)`; `native_vortex` explicitly requires Vortex input. |
| Choose result delivery | Collection, streaming and supported writers remain separate from source investment. |

The CLI accepts `auto`, `vortex_prepared` and `prepare_or_native` as aliases for
`vortex_middle`; none is an optimizer deciding whether preparation pays. The
legacy literal `direct` is rejected on the public local compatibility route.
Python's in-process dispatch does not share every CLI alias normalization path,
so ordinary defaults and the canonical policies above are the portable guidance.
Keep existing callable names and accepted values; any future rename or unified
source-strategy parameter needs an explicit compatibility plan.

## Follow-through and acceptance

Use the comparison to select the shared component needing work: decoding/intake
and scheduling, capability-proved access to specialized operators, reusable
expressions/representations, or policy/explain clarity. Preserve existing provider
admission checks and original rejected experiments; a new experiment needs a
materially different, grounded mechanism. Avoid query-specific dispatch and
duplicated execution engines.

Add representative route and scale coverage while preserving the original
347-case denominator and history: direct/prepared, small/large, cardinality,
width/string content, selective/broad demand, provider activation, pressure, and
single-query/multi-query workloads. Coordinate this with the larger coverage
campaign already scheduled after item 19; do not replace its separate scope.

Automatic preparation is a later decision requiring explicit artifact permission,
generation validity, reuse horizon, time/storage budgets, and an objective such
as first-result latency or total workload cost. Select it only after the evidence
and an explainable policy exist. Current defaults, release authorization and
CG-1 through CG-23 are unchanged by this investigation.
