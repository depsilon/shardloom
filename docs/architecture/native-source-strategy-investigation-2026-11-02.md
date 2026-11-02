<!-- SPDX-License-Identifier: Apache-2.0 -->

# Source and preparation strategy investigation

Status: requested November 2, scheduled immediately after the 1.1.0 version
milestone and before item 19. This is an investigation within the existing
source/workflow/planner owners, not a new implementation phase or competitive
gate. The [phase plan](phased-execution-plan.md#planned) owns the queue.

The maintainer supplied a review of direct and prepared execution and requested
exploring it after the version bump. Preserve that timing: current items 18/23
acceptance and the separately requested ingest-plus-Full43 run continue first.
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
CG-1 through CG-23 are unchanged by this queued investigation.
