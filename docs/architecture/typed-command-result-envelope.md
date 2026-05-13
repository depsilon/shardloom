# Typed Command/Result Envelope

## Purpose

This document records the first Priority 3.9 implementation slice for the typed command/result
envelope.

The corresponding code surface is:

```text
OutputEnvelope
OutputTypedPayload
OutputTypedRef
OutputTypedArtifact
shardloom-cli/src/command_family.rs
shardloom-cli/src/cli_output.rs
shardloom-cli/src/packaging_deployment.rs
shardloom-cli/src/rest_api_planning.rs
shardloom-cli/src/status_capabilities.rs
shardloom-cli/src/typed_envelope.rs
shardloom.output.v2
```

## Current shape

Every real CLI JSON envelope now emits these typed slots:

```text
result
result_refs
artifacts
artifact_refs
certificates
policy
lifecycle
capability_snapshot
```

`OutputEnvelope::with_field` mirrors current command fields into `result.fields`, so the typed
`result` payload is available without rewriting every command family in the same PR.

The CLI renderer now performs a first-pass route for common fields:

```text
policy
  fallback, side-effect, probe, network, write, external-engine, and policy fields

lifecycle
  mode, schema/protocol/version/status/phase, and transport fields

capability_snapshot
  capability, support, certification, feature-gate, readiness, and coverage fields

result
  command-specific result fields that do not match one of the above typed slots
```

The renderer also attaches typed references when command fields already carry explicit reference or
identifier keys:

```text
certificates
  execution_certificate, native_io_certificate, and other certificate refs/ids/paths/URIs

artifact_refs
  evidence artifact, materialization boundary, benchmark row, Foundry report, source report,
  sink report, and other artifact refs/ids/paths/URIs

result_refs
  result refs/ids/paths/URIs
```

Reference routing is conservative: requirement booleans such as `execution_certificate_required`
remain payload fields, while explicit `*_ref`, `*_id`, `*_path`, and `*_uri` values become typed
refs when the value is a real reference rather than `false`, `none`, `not_performed`, or similar.

The renderer now batches command fields before final envelope emission so command-family-specific
report helpers can attach inline typed artifacts while the temporary legacy `fields` mirror remains
available. The first inline report payloads are:

```text
execution_certificate_report
native_io_report
benchmark_plan_report
benchmark_claim_evidence_report
materialization_boundary_report
source_report
```

For runtime commands that already emit certificate field groups, the renderer also attaches inline
typed artifacts when the existing `*_certificate_emitted` field is true:

```text
execution_certificate
native_io_certificate
streaming_batch_runtime_report
source_report
source_pushdown_report
sink_report
adapter_fidelity_report
```

The first prefix helpers cover local CountAll Native I/O certificates and local Vortex primitive
Native I/O and execution certificates, plus emitted Vortex streaming-batch runtime reports.
The first subset helpers derive source, source-pushdown, sink, and adapter-fidelity subreports from
the same emitted local Native I/O certificate groups. Unavailable or feature-disabled certificate
reports stay as regular typed fields and do not create misleading inline certificate artifacts.

The `input-adapters` registry also enriches the typed `capability_snapshot` payload with adapter
counts, adapter family orderings, and adapter statuses so clients no longer need to infer adapter
capabilities only from the temporary flat `fields` mirror.

These are protocol payloads only. They do not execute benchmarks, evaluate certificates, read data,
write artifacts, or turn report-only surfaces into runtime support.

Shared CLI JSON/text rendering and error emission lives in `shardloom-cli/src/cli_output.rs`.
Typed-envelope field/ref routing lives in `shardloom-cli/src/typed_envelope.rs`. Command handlers
still live mostly in `main.rs`; the output module split is the next modularity step for shared
rendering and protocol behavior.

The first physical handler-family split moved the status and capabilities command handlers into
`shardloom-cli/src/status_capabilities.rs`.

`shardloom-cli/src/status_capabilities.rs` now also owns capability-discovery helper construction:
`CapabilityDiscoveryScope`, certification field/text emitters, operator discovery fields, and
world-class user-surface capability emitters. These helpers remain report-only and side-effect-free;
they do not probe datasets, run parsers, execute runtime work, invoke external engines, or weaken
no-fallback reporting.

The input planning family currently contains `input-adapters`, `input-plan`, `vortex-input-plan`,
`vortex-read-plan`, and `vortex-task-graph`, and lives in
`shardloom-cli/src/input_planning.rs`. These handlers remain metadata/planning surfaces and do not
read datasets, probe object stores, execute tasks, materialize outputs, invoke external engines, or
weaken no-fallback behavior.

The REST/API planning family currently contains `api-compat-plan` and lives in
`shardloom-cli/src/rest_api_planning.rs`. It remains report-only and does not start a REST server,
open sockets, perform remote execution, or weaken no-fallback behavior.

The CLI API protocol field group lives beside the REST/API planning handler in
`shardloom-cli/src/rest_api_planning.rs`.

The packaging/deployment family currently contains `release-plan`, `package-plan`,
`agent-contract-pack`, and `python-wrapper-plan`, and lives in
`shardloom-cli/src/packaging_deployment.rs`. These handlers remain report-only and do not publish
packages, push artifacts, invoke external engines, or weaken no-fallback behavior.

Packaging/deployment field construction is colocated with that family: release/package readiness,
Conda certification, agent contract, and Python wrapper field groups live in
`shardloom-cli/src/packaging_deployment.rs`.

The benchmark planning family currently contains `benchmark-plan` and
`benchmark-claim-evidence-plan`, and lives in `shardloom-cli/src/benchmark_planning.rs`. These
handlers remain report-only and do not run comparative benchmarks, invoke baseline engines, publish
performance claims, or weaken no-fallback behavior.

The benchmark runtime family currently contains `traditional-analytics-run`,
`traditional-analytics-vortex-run`, and `vortex-count-benchmark`, and lives in
`shardloom-cli/src/benchmark_runtime.rs`. These handlers preserve the existing local benchmark
harness behavior; external engines remain comparison-only baselines and must not become fallback
execution paths.

Benchmark field construction ownership is now colocated with these families: `benchmark_plan`,
`benchmark_claim_evidence`, and Vortex count benchmark report fields live in the benchmark planning
or runtime modules. The shared duration conversion helpers used by benchmark and Vortex output
surfaces live in `shardloom-cli/src/cli_time.rs`.

The operational hardening/security family currently contains `security-plan`,
`security-governance-evidence-gate`, `effect-budget-plan`, `agent-safety-plan`, and
`redaction-plan`, and lives in `shardloom-cli/src/operational_hardening.rs`. These handlers remain
report-only and do not resolve credentials, load secrets, execute effects, write data, or weaken
no-fallback behavior.

Operational-policy field construction is colocated with that family: effect budget and
security-governance evidence gate fields live in `shardloom-cli/src/operational_hardening.rs`.

The diagnostics family currently contains `feature-footprint`, `doctor`, `explain`, and `estimate`,
and lives in `shardloom-cli/src/diagnostics.rs`. These handlers remain report-only and do not probe
datasets, collect profiles, execute plans, invoke external engines, or weaken no-fallback behavior.

Diagnostics field construction is colocated with that family: feature footprint and observability
schema coverage field groups live in `shardloom-cli/src/diagnostics.rs`.

The evidence/certificate planning family currently contains `correctness-plan`,
`correctness-harness-plan`, `execution-certificate-plan`, `universal-harness-plan`, and
`native-io-envelope-plan`, and lives in `shardloom-cli/src/evidence_certificates.rs`. These handlers
remain report-only and do not run harnesses, read data, emit runtime certificates from execution,
invoke external engines, or weaken no-fallback behavior.

`shardloom-cli/src/evidence_certificates.rs` now also owns the field construction helpers for the
correctness plan/harness, execution-certificate surface, Native I/O envelope, universal harness,
RFC coverage follow-through, world-class sufficiency, CG-20 user capability gate, and CG-20
approximate sketch gate. The helper move preserves report-only and no-fallback output contracts and
does not add harness execution, data reads, artifact writes, runtime certificates, external engines,
or side effects.

The workflow/table planning family now contains `manifest-plan`, `layout-health-plan`,
`compaction-plan`, `table-intelligence-plan`, `schema-plan`, `table-compat-plan`,
`cg9-catalog-metadata-gate`, `incremental-plan`, `stateful-reuse-plan`, and
`cg17-stateful-reuse-gate`, and lives in
`shardloom-cli/src/workflow_planning.rs`. These handlers remain report-only and do not read
datasets, probe catalogs, execute plans, write data, materialize outputs, invoke external engines,
or weaken no-fallback behavior.

`shardloom-cli/src/workflow_planning.rs` now also owns workflow/table field construction and
fixtures for schema/table compatibility, plan import/export, layout health, compaction, table
intelligence, CG-9 catalog metadata, CDC incremental planning, and stateful reuse. The helper move
preserves stable JSON/text output and does not add catalog IO, dataset reads, plan execution,
materialization, writes, or fallback execution.

The engine/runtime planning family now contains `streaming-plan`, `streaming-batch-plan`,
`backpressure-plan`, `runtime-plan`, `task-plan`, `sizing-plan`, `sizing-feedback-plan`,
`dynamic-work-shaping-plan`, and `cg8-runtime-promotion-gate`, and lives in
`shardloom-cli/src/engine_runtime_planning.rs`. These handlers remain report-only and do not read
datasets, execute tasks, collect profiles, write data, materialize outputs, invoke external engines,
or weaken no-fallback behavior.

The object-store planning family now contains `object-store-request-plan`,
`cg10-object-store-runtime-gate`, `object-store-range-plan`, `object-store-coalesce-plan`,
`object-store-schedule-plan`, `object-store-checkpoint-retry-plan`, and
`object-store-commit-plan`, and lives in `shardloom-cli/src/object_store_planning.rs`. That module
also owns object-store request/range/coalescing/scheduling/checkpoint/retry/commit field
construction and fixtures without probing object stores, starting workers, writing checkpoints, or
weakening no-fallback behavior.

The extension/UDF planning family now contains `extension-registry`, `extension-inspect`, and
`udf-runtime-plan`, and lives in `shardloom-cli/src/extension_planning.rs`. These handlers remain
metadata-only and do not dynamically load extension code, execute UDFs, invoke external services,
write data, or weaken no-fallback behavior.

The prepared/source-backed execution family now contains the `vortex-encoded-read-api`,
`vortex-encoded-read-boundary`, `vortex-encoded-read-metadata-probe`,
`vortex-encoded-read-readiness`, `vortex-encoded-read-probe`,
`vortex-encoded-read-execute`, and `vortex-encoded-read-spike` handler entry points in
`shardloom-cli/src/prepared_source_backed_execution.rs`. The split preserves the current report-only
API/boundary/metadata-probe behavior plus readiness-only, probe-only, executor-contract, and
feature-gated spike behavior without reading, decoding, materializing, writing, or weakening
no-fallback evidence.

The Vortex primitive execution family now starts its physical split in
`shardloom-cli/src/vortex_primitive_execution.rs` with `vortex-count`, `vortex-count-where`,
`vortex-project`, `vortex-filter`, `vortex-filter-project`, `vortex-run`, `vortex-local-exec`,
`vortex-bounded-local-exec`, and `vortex-query-trace`. These handler splits preserve the existing
local primitive, local-engine, bounded-policy, work-avoidance, certificate, why-report, and
no-fallback output contracts while broader non-primitive handler extraction continues.

The Vortex runtime-readiness family now contains `vortex-adaptive-sizing`, `vortex-memory-plan`,
`vortex-schedule-plan`, and `vortex-execution-readiness`, and lives in
`shardloom-cli/src/vortex_runtime_planning.rs`. Adaptive sizing, memory bridge, and scheduler bridge
field construction is colocated with those handlers while preserving dry-run/readiness planning,
no task execution, no data reads, no writes, no external engines, and no fallback execution.

The Vortex planning family now has its metadata/report-only module in
`shardloom-cli/src/vortex_planning.rs`, covering `vortex-metadata-plan`,
`vortex-pruning-plan`, `vortex-metadata-probe`, `vortex-api-inventory`,
`vortex-encoded-path-selection-plan`, `vortex-generalized-encoded-primitive-gate`,
`vortex-metadata-execute`, `vortex-dry-run`, `vortex-plan`, `translation-plan`,
`vortex-output-plan`, `vortex-readiness`, `vortex-dtype-mapping`,
`vortex-encoding-layout-mapping`, `vortex-statistics-mapping`,
`vortex-file-metadata-open`, `vortex-metadata-summary`, and
`vortex-query-primitive-plan`, `vortex-metadata-physical-kernel-plan`,
`vortex-count-readiness-plan`, `vortex-encoded-count-approval-plan`,
`vortex-layout-driver-approval-plan`, `vortex-filtered-count-readiness-plan`, and
`vortex-projection-readiness-plan`. These handlers remain metadata-only, plan-only,
executor-contract, local-guard, or report-only surfaces and do not execute tasks beyond their
existing contract, materialize outputs, write data, invoke external engines, or weaken no-fallback
behavior.

The optimizer/kernel planning family now contains `kernel-registry`, `optimizer-plan`,
`optimizer-adaptive-memory-plan`, and `cpu-specialization-plan` in
`shardloom-cli/src/optimizer_planning.rs`. These handlers preserve the current report-only kernel
registry snapshot, unsupported optimizer skeleton, adaptive memory planning report, and CPU
specialization planning report without running optimizer execution, physical kernels,
CPU-specialized kernels, writes, materialization, external engines, or fallback execution.

Command family classification lives in `shardloom-cli/src/command_family.rs` and is emitted in the
typed lifecycle payload as `command_family`. This gives status/capabilities, Vortex primitive,
prepared/source-backed, evidence/certificate, benchmark, packaging/deployment, Foundry,
operational-hardening, diagnostic, REST/API-planning, workflow-planning, engine-runtime, and
extension-planning commands a stable family taxonomy before their handlers are physically split.

Golden typed-envelope contract snapshots now live in:

```text
shardloom-cli/tests/typed_envelope_contract_snapshots.rs
```

The current fixture coverage spans:

```text
success status envelope
invalid-input diagnostics envelope
unsupported source-backed encoded-read boundary
blocked capability promotion gate
certificate-surface report
Native I/O envelope report
evidence-incomplete benchmark row report
benchmark claim evidence report
certified runtime execution with inline certificates
missing-binary protocol parity through the Python binary-resolution layer
Foundry-adjacent optional universal harness report
```

First-class Foundry boundary-report fixtures remain planned because those surfaces are not yet
represented by a concrete CLI Foundry boundary command.

The old top-level `fields` array is still present as a temporary legacy mirror for existing tests,
the Python client, and command-family migration safety. It is no longer the intended primary
machine-readable payload model.

## Python client

The Python wrapper now expects `shardloom.output.v2` and preserves:

```text
result
result_refs
artifacts
artifact_refs
certificates
policy
lifecycle
capability_snapshot
```

It still exposes the legacy field convenience helpers while command families migrate to typed
payload-specific accessors.

## Remaining Priority 3.9 work

Remaining work is organized as outcome-oriented closeout slices rather than one-helper PRs:

```text
Runtime/optimizer/operational CLI ownership closeout.
Vortex primitive and readiness CLI ownership closeout.
Typed envelope compatibility lock across success, unsupported, blocked, certified, and error paths.
Concrete Foundry boundary fixtures once those command surfaces exist.
```

## Runtime posture

This is a protocol/refactor slice only. It does not add REST server behavior, wrapper ecosystem
implementation, benchmark execution, runtime expansion, external engine invocation, network effects,
dataset probes, writes, or fallback execution.
