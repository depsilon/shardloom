# Cold Ingestion And Preparation Research Carry-Forward

Status: implemented local cold-lane evidence inventory for the completed 1H-1L and 2J-2K bundle;
remaining benchmark refresh and claim-grade gates are tracked by the detailed unchecked follow-up
items in `docs/architecture/phased-execution-plan.md`.

## Summary

This note captures the cold ingestion/preparation research direction from the benchmark-outlier
analysis and the follow-up novel-concepts review. It does not implement runtime behavior, publish a
benchmark claim, or authorize a hidden fast mode.

The main read of the benchmark outlier is structural: the slow portion is likely in the cold lane,
not in the warm `prepared_vortex` query lane. Cold timing includes source read/parse, compatibility
to Vortex preparation, write/reopen/scan verification, optional sink/replay work, evidence
rendering, and process/harness overhead. Those are valid workflow costs, but they must not be
reported as pure query compute.

The proposed follow-through is to make the cold lane itself a first-class Vortex-native runtime
surface:

```text
UniversalIngress / InputAdapter
-> SourceState
-> vortex_ingest
-> VortexPreparedState
-> prepared_vortex
-> ExecutionPlan
-> OutputPlan
-> SinkArtifact
-> evidence + claim gate
```

## Carry-Forward Concepts

### Cold-Lane Attribution

The benchmark artifact should preserve enough timing and route constitution evidence to distinguish
these cases:

- full certified cold ingestion/preparation;
- preparation-only runs;
- warm `prepared_vortex` query runs;
- result-sink/replay overhead;
- evidence rendering and process/harness overhead.

Any row that cannot separate those costs should remain `not_claim_grade`. The goal is not to hide
cold cost; it is to make the outlier actionable.

### Vortex-Native Preparation Spine

Before inventing new ShardLoom source/sink abstractions, each cold-lane implementation must check
Vortex array, file I/O, Scan, Source, Sink, Split, layout, statistics, and writer concepts. The
preferred outcome is a small ShardLoom certificate/report wrapper around admitted Vortex-native
providers, not decode-to-Arrow or a query-engine integration.

The first runtime shape should be local and evidence-heavy:

```text
SourceState split/columnar batch
-> Vortex array/layout/write provider
-> prepared artifact digest
-> reopen/scan verification
-> Native I/O certificate
```

### Differential Preparation

Differential preparation means updating or overlaying a `VortexPreparedState` from a declared
delta, rather than rebuilding the whole prepared artifact when only a small part of the source
changed.

The scoped local runtime path now includes automatic append-only refinement for artifact-adjacent
prepared-state reuse manifests. When a local CSV/JSONL source changes by appending bytes, ShardLoom
can verify that the old source bytes are the current source prefix, write only a delta source and
delta Vortex artifact, attach a digest-backed refinement manifest, and admit the count-family
consumer over base manifest row count plus delta reopen row count. The base prepared artifact is not
rewritten.

Required evidence includes base and delta `SourceState` identifiers, base prepared-state identity,
delta manifest digest, refinement manifest path/digest, changed byte/row/segment ranges, update
mode, schema compatibility, tombstone/delete/update policy, replay/correctness digest, automatic
detection status, overlay consumer status, and deterministic invalidation reasons.

This is inspired by CDC overlays, materialized-view maintenance, content-addressed manifests, and
repair/checksum systems. It is not a broad CDC/table-transaction claim.

### Capillary I/O

Capillary I/O means splitting the cold lane into many small, typed source/sink work units that can
be split, coalesced, prefetched, retried, and scheduled under bounded memory and sink pressure.

The useful unit is not a raw thread or opaque task. It is an evidence-bearing capillary with:

- source ref and byte/row range;
- projection/filter/read mask;
- Vortex segment or prepared-artifact target ref;
- materialization/decode posture;
- retry/idempotency posture;
- sink pressure and memory pressure evidence;
- no-fallback fields.

PulseWeave concepts can then apply earlier in the route. FlowInventory can bound in-flight source
and writer tasks, ScarcityLedger can price memory/decode/sink pressure, EndoPulse can adjust only
the next local window, and ProofBound can block application when certificates are incomplete.

### Scout Ingress And Triage

Scout ingress is a small preflight pass over source metadata, schema samples, row/byte ranges,
statistics, and parse anomalies before full preparation. It is a planning and blocker surface:
detect obvious schema drift, malformed records, unexpected nullability, unsupported nested shapes,
small-file/pathology signals, and encoding/layout opportunities early.

Scout triage must not silently drop records. If rows are rejected or quarantined, the quarantine
output is an explicit sink boundary with digest, count, schema, fidelity, and no-fallback evidence.

### Predictive Layout And Write Advice

The cold lane should make layout decisions with the later workload in mind. A Vortex layout/write
advisor can use declared workload shape, SourceState statistics, pushdown requirements, output
requirements, and prior benchmark evidence to recommend chunk size, dictionary strategy, statistics
policy, segmentation, and write/reopen verification depth.

This remains advisory until correctness, Native I/O, and benchmark evidence prove that an admitted
layout strategy improves a declared workload. It must not use AI, persistent tuning state, or a
performance claim by default.

### Copy Budget And Buffer Lifecycle

Cold preparation should expose copies, allocation posture, batch handoff shape, writer buffering,
and unsafe-lifetime blockers. Buffer reuse may be admitted only when ownership, schema, dtype,
nullability, ordering, evidence parity, and correctness parity are preserved.

The immediate goal is visibility and fail-closed policy, not a memory-efficiency claim.

## Phase Mapping

The detailed execution queue is intentionally split:

- `GAR-IOREUSE-1H`: cold-lane attribution and benchmark constitution split.
- `GAR-IOREUSE-1I`: Vortex-native source/sink/split preparation spine.
- `GAR-IOREUSE-1J`: differential preparation and prepared-state delta overlays.
- `GAR-IOREUSE-1K`: capillary I/O and PulseWeave cold-lane control.
- `GAR-IOREUSE-1L`: scout ingress, anomaly quarantine, and schema-drift triage.
- `GAR-PERF-2J`: cold-lane Vortex layout/write advisor.
- `GAR-PERF-2K`: cold-lane allocation, copy-budget, and buffer lifecycle.

`GAR-IOREUSE-1H` through `GAR-IOREUSE-1L`, `GAR-PERF-2J`, and `GAR-PERF-2K` now have implemented
evidence surfaces in the local `vortex_ingest` path. Public benchmark measurement refresh remains
deferred until after this benchmark-affecting bundle merges. The scout ingress surface emits
`vortex_scout_ingress_*` fields for source scope, metadata/sample ranges, schema digest before/
after, anomaly families, malformed row refs where safe, quarantine planning, redaction status,
unsupported diagnostic codes, correctness policy, no-fallback posture, and the explicit
no-standalone-lane route. The layout/write advisor surface emits
`vortex_layout_write_advisor_*` fields for workload constitution, source statistics, pushdown/sink
requirements, strategy/provider posture, write/reopen verification depth, runtime decision applied
status, selected strategy, decision digest, provider admission, blocker, correctness refs,
benchmark refs, and no-standalone-lane route. The current runtime-applied strategy is limited to
the workspace-safe local single-artifact Vortex writer; broader layout optimization remains gated.
The copy-budget surface emits
`vortex_copy_budget_*` fields for allocation/copy scope, measured or `not_measured` copy segments,
ownership policy, buffer reuse blockers, unsafe-lifetime posture, correctness parity refs, and
no-standalone-lane route.

## Claim Boundary

These concepts are architecture and implementation direction only. They cannot support public
performance, production, Spark-displacement, object-store/lakehouse, Foundry, or broad SQL/DataFrame
claims until the matching phase item lands with workload-scoped correctness, Native I/O,
benchmark, no-fallback, and claim-gate evidence.

External engines and Vortex query-engine integrations may be used only as baselines, references, or
oracles. They must not execute unsupported ShardLoom work.
