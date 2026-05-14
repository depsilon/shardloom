# Performance Attribution And Execution Structure

Status: implemented baseline for P7.4.8/P7.4.9 with P7.5 overhaul follow-ups.

## Purpose

ShardLoom's current comparative benchmark artifacts include useful certified workflow evidence, but
some rows measure more than query compute. This document defines the timing vocabulary and execution
mode boundaries that benchmark reports, CLI envelopes, Python clients, and future REST surfaces must
preserve before any public performance interpretation.

The canonical top-level flow reference is
`docs/architecture/compute-engine-flow-reference.md`. This document is the more detailed
performance-attribution companion for that flow: it explains which costs belong to each execution
mode and which fields must keep those costs visible.

The repo-alignment review and next overhaul steps are tracked in
`docs/architecture/compute-engine-flow-overhaul-review.md`.

The immediate correction is structural: compatibility-import-certified rows must not be read as pure
operator compute. They include compatibility source parsing, compatibility-to-Vortex import, Vortex
file write/reopen/scan, temporary materialization, optional result-sink replay, evidence rendering,
and CLI/process overhead. Those costs are valid for an ingest/stage workflow, but they are not the
same as prepared/native Vortex query timing.

## Vortex-First Provider Check

- Subject area: benchmark timing structure, Vortex prepared/native execution, Scan API/source-backed
  timing, and encoded/native operator evidence.
- Upstream Vortex concept checked: arrays and deferred/compressed representations; Scan API Source,
  Sink, Split, filter/projection/limit pushdown; execution fusion/deferred execution; I/O
  coalescing, prefetch, concurrency, and memory backpressure.
- Decision: `wrap_vortex_concept` for report-only stage attribution and execution-mode vocabulary;
  `use_vortex_native_provider` only for prepared/native paths with Vortex provider evidence;
  `blocked_until_vortex_or_shardloom_evidence` for unsupported fused, encoded-native, or Scan API
  paths.
- Vortex API/provider surface: local Vortex files and current feature-gated benchmark provider
  paths; future Scan API Source/Sink/Split and pushdown surfaces when they can be admitted with
  evidence.
- ShardLoom provider/report/certificate surface: benchmark rows, typed command/result envelopes,
  Native I/O certificates, execution certificates, materialization/decode boundaries, prepared
  artifact refs/digests, and deterministic unsupported diagnostics.
- Residual handling: residuals must be executed by ShardLoom-native code or blocked; they must not
  be delegated to DataFusion, DuckDB, Spark, Polars, Dask, or another external engine as fallback.
- Materialization/decode boundary: every row must record whether native/compressed, canonical, or
  materialized representations were used and whether decode/materialization was required.
- Evidence added: planned P7.4.8 stage timing fields and planned P7.4.9 prepared/native benchmark
  lanes.
- Gates still blocked: broad SQL/DataFrame maturity, broad performance superiority, object-store
  runtime, table/catalog runtime, and production claims.
- `fallback_attempted=false`: required for every ShardLoom execution-mode row.

## Structural Paths

### One-Shot Compatibility Query

Shape:

```text
CSV/Parquet/etc -> direct transient ShardLoom-native compute -> optional result
```

This path is for small local jobs and developer quick checks. It does not persist a Vortex artifact
and does not carry a Vortex-native claim. If exposed before implementation, it must be deterministic
report-only or unsupported.

Required facts:

```text
selected_execution_mode=direct_compatibility_transient
vortex_native_claim_allowed=false
direct_transient_execution=true
compatibility_import_included=false
vortex_write_reopen_included=false
fallback_attempted=false
external_engine_invoked=false
claim_gate_status=not_vortex_native
```

### Ingest/Stage Workflow

Shape:

```text
CSV/Parquet/etc -> compatibility adapter -> Vortex import -> certify -> write/reopen -> compute
```

This is the current certified compatibility-import workflow shape. It is useful because it proves
source compatibility, Native I/O certificate evidence, artifact digests, Vortex staging, replay, and
no-fallback behavior. It is not the default lane for pure query-speed comparison.

Required facts:

```text
selected_execution_mode=compatibility_import_certified
execution_mode_family=compatibility_import
compatibility_import_included=true
vortex_prepare_included=true
vortex_write_reopen_included=true
result_sink_included=<true when result-sink proof is requested>
fallback_attempted=false
external_engine_invoked=false
```

### Prepared Vortex Query

Shape:

```text
CSV/Parquet/etc -> one-time Vortex preparation -> many scenario runs from prepared .vortex artifacts
```

This is the primary comparative benchmark lane while ShardLoom matures native Vortex operators. The
preparation step is measured and recorded, but per-scenario timing starts after prepared artifact
creation unless a caller explicitly asks to include preparation.

In the comparative harness, prepared/native rows stay attached to the requested source-format rows
such as CSV, JSONL, Parquet, Arrow IPC, Avro, or ORC. The report should not add a standalone
`.vortex` storage-format row just to show native timing; prepared artifact refs and digests record
the Vortex boundary.

Required facts:

```text
selected_execution_mode=prepared_vortex
execution_mode_family=native_vortex
preparation_millis=<measured separately>
preparation_included_in_timing=false
prepared_artifact_ref=<fact/dim refs>
prepared_artifact_digest=<digest refs>
compatibility_import_included=false for scenario timing
fallback_attempted=false
external_engine_invoked=false
```

### Native Vortex Query

Shape:

```text
existing .vortex input -> Vortex-native scan/operator path -> result/evidence
```

This is the cleanest ShardLoom performance lane once operator coverage matures. Rows in this lane
must record provider/API surface, split/pushdown evidence where available, representation
transitions, and whether compute happened on compressed/native arrays, canonical arrays, or
materialized arrays.

Required facts:

```text
selected_execution_mode=native_vortex
execution_mode_family=native_vortex
compatibility_import_included=false
vortex_prepare_included=false
direct_transient_execution=false
fallback_attempted=false
external_engine_invoked=false
```

## Execution Modes

The stable mode names are:

```text
auto
compatibility_import_certified
prepared_vortex
direct_compatibility_transient
native_vortex
```

`auto` is transparent selection only. It must always report the selected mode and reason, and it
must never silently invoke an external fallback engine.

Every relevant surface should carry:

```text
requested_execution_mode
selected_execution_mode
mode_selection_reason
execution_mode_family
vortex_native_claim_allowed
compatibility_import_included
vortex_prepare_included
vortex_write_reopen_included
direct_transient_execution
fallback_attempted
external_engine_invoked
claim_gate_status
```

## Stage Timing Fields

Benchmark JSON and Markdown should preserve these fields where available:

```text
total_runtime_millis
scenario_compute_millis
operator_compute_millis
computed_result_sink_write_millis
result_sink_write_millis
startup_warmup_millis
preparation_millis
preparation_included_in_timing
prepared_artifact_ref
prepared_artifact_digest
source_read_millis
compatibility_parse_millis
compatibility_to_vortex_import_millis
vortex_write_millis
vortex_reopen_millis
vortex_scan_millis
evidence_render_millis
build_time_excluded
compatibility_to_vortex_included
vortex_reopen_scan_included
result_sink_included
representation_transition_summary
encoded_native_execution_status
fusion_status
scan_api_status
persistent_runner_status
```

Unknown or not-yet-isolated fields should be explicit `null`, `not_measured`, or
`included_in_total_runtime` values rather than silently omitted.

## Current Interpretation

Current ShardLoom compatibility rows answer:

```text
How expensive is the certified local compatibility -> Vortex ingest/stage workflow plus current
temporary benchmark operator and evidence path?
```

They do not answer:

```text
How fast is pure ShardLoom operator compute over already-prepared Vortex data?
```

Prepared/native Vortex rows should answer the second question, with preparation timing and
artifact evidence recorded separately.

## Vortex Alignment Notes

The Vortex Scan API documentation describes Source, Sink, and Split concepts plus filter,
projection, and limit pushdown, but notes that the API is still under active development. ShardLoom
should align source-backed evidence with those concepts while emitting blockers when an upstream or
local path is not ready.

The Vortex I/O documentation describes positional reads, read coalescing, prefetching, backend
concurrency, segment caching, and memory backpressure. ShardLoom should treat those as Native I/O
evidence dimensions rather than hiding them in opaque benchmark time.

References:

- <https://docs.vortex.dev/concepts/arrays>
- <https://docs.vortex.dev/concepts/scanning>
- <https://docs.vortex.dev/developer-guide/internals/execution>
- <https://docs.vortex.dev/developer-guide/internals/io>
