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

The first physical handler-family split moves the status and capabilities command handlers into
`shardloom-cli/src/status_capabilities.rs`. The module still reuses shared capability helper
functions while broader command-family extraction continues.

The input planning family currently contains `input-adapters`, `input-plan`, `vortex-input-plan`,
`vortex-read-plan`, and `vortex-task-graph`, and lives in
`shardloom-cli/src/input_planning.rs`. These handlers remain metadata/planning surfaces and do not
read datasets, probe object stores, execute tasks, materialize outputs, invoke external engines, or
weaken no-fallback behavior.

The REST/API planning family currently contains `api-compat-plan` and lives in
`shardloom-cli/src/rest_api_planning.rs`. It remains report-only and does not start a REST server,
open sockets, perform remote execution, or weaken no-fallback behavior.

The packaging/deployment family currently contains `release-plan`, `package-plan`,
`agent-contract-pack`, and `python-wrapper-plan`, and lives in
`shardloom-cli/src/packaging_deployment.rs`. These handlers remain report-only and do not publish
packages, push artifacts, invoke external engines, or weaken no-fallback behavior.

The benchmark planning family currently contains `benchmark-plan` and
`benchmark-claim-evidence-plan`, and lives in `shardloom-cli/src/benchmark_planning.rs`. These
handlers remain report-only and do not run comparative benchmarks, invoke baseline engines, publish
performance claims, or weaken no-fallback behavior.

The benchmark runtime family currently contains `traditional-analytics-run`,
`traditional-analytics-vortex-run`, and `vortex-count-benchmark`, and lives in
`shardloom-cli/src/benchmark_runtime.rs`. These handlers preserve the existing local benchmark
harness behavior; external engines remain comparison-only baselines and must not become fallback
execution paths.

The operational hardening/security family currently contains `security-plan`,
`security-governance-evidence-gate`, `effect-budget-plan`, `agent-safety-plan`, and
`redaction-plan`, and lives in `shardloom-cli/src/operational_hardening.rs`. These handlers remain
report-only and do not resolve credentials, load secrets, execute effects, write data, or weaken
no-fallback behavior.

The diagnostics family currently contains `feature-footprint`, `doctor`, `explain`, and `estimate`,
and lives in `shardloom-cli/src/diagnostics.rs`. These handlers remain report-only and do not probe
datasets, collect profiles, execute plans, invoke external engines, or weaken no-fallback behavior.

The evidence/certificate planning family currently contains `correctness-plan`,
`correctness-harness-plan`, `execution-certificate-plan`, `universal-harness-plan`, and
`native-io-envelope-plan`, and lives in `shardloom-cli/src/evidence_certificates.rs`. These handlers
remain report-only and do not run harnesses, read data, emit runtime certificates from execution,
invoke external engines, or weaken no-fallback behavior.

The workflow/table planning family now contains `manifest-plan`, `layout-health-plan`,
`compaction-plan`, `table-intelligence-plan`, `schema-plan`, `table-compat-plan`,
`cg9-catalog-metadata-gate`, `incremental-plan`, `stateful-reuse-plan`, and
`cg17-stateful-reuse-gate`, and lives in
`shardloom-cli/src/workflow_planning.rs`. These handlers remain report-only and do not read
datasets, probe catalogs, execute plans, write data, materialize outputs, invoke external engines,
or weaken no-fallback behavior.

The engine/runtime planning family now contains `streaming-plan`, `streaming-batch-plan`,
`backpressure-plan`, `runtime-plan`, `task-plan`, `sizing-plan`, `sizing-feedback-plan`,
`dynamic-work-shaping-plan`, and `cg8-runtime-promotion-gate`, and lives in
`shardloom-cli/src/engine_runtime_planning.rs`. These handlers remain report-only and do not read
datasets, execute tasks, collect profiles, write data, materialize outputs, invoke external engines,
or weaken no-fallback behavior.

The extension/UDF planning family now contains `extension-registry`, `extension-inspect`, and
`udf-runtime-plan`, and lives in `shardloom-cli/src/extension_planning.rs`. These handlers remain
metadata-only and do not dynamically load extension code, execute UDFs, invoke external services,
write data, or weaken no-fallback behavior.

The prepared/source-backed execution family now contains the `vortex-encoded-read-probe` and
`vortex-encoded-read-spike` handler entry points in
`shardloom-cli/src/prepared_source_backed_execution.rs`. The split preserves the current probe-only
and feature-gated spike behavior; `vortex-encoded-read-execute` keeps its existing executor contract
while broader prepared/source-backed extraction continues.

The Vortex primitive execution family now starts its physical split in
`shardloom-cli/src/vortex_primitive_execution.rs` with `vortex-count`, `vortex-count-where`,
`vortex-project`, `vortex-filter`, `vortex-filter-project`, `vortex-run`, `vortex-local-exec`,
`vortex-bounded-local-exec`, and `vortex-query-trace`. These handler splits preserve the existing
local primitive, local-engine, bounded-policy, work-avoidance, certificate, why-report, and
no-fallback output contracts while broader non-primitive handler extraction continues.

The Vortex planning family now has its first metadata/report-only module in
`shardloom-cli/src/vortex_planning.rs`, covering `vortex-metadata-plan`,
`vortex-pruning-plan`, `vortex-metadata-probe`, and `vortex-api-inventory`. These handlers remain
metadata-only or plan-only surfaces and do not execute tasks, materialize outputs, write data,
invoke external engines, or weaken no-fallback behavior.

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
Foundry-adjacent optional universal harness report
```

Concrete certified runtime execution, missing-binary protocol parity, and first-class Foundry
boundary-report fixtures remain planned because those surfaces are either feature-gated, owned by
the Python binary-resolution layer, or not yet represented by a concrete CLI Foundry boundary
command.

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

Remaining work is command-family migration and CLI modularization:

```text
Continue migrating command-family-specific result fields from ad hoc field construction to typed
payload helpers beyond the first inline report payloads.
Attach inline evidence artifacts, certificate payloads, Foundry boundary reports, source/sink
reports, materialization boundary reports, and richer capability snapshots through typed slots where
a command has more than a reference.
Finish remaining golden fixtures for certified runtime execution, missing-binary protocol parity,
and concrete Foundry boundary reports.
Physically split CLI handlers by capability family and continue centralizing diagnostics, fallback,
policy, and side-effect reporting around the shared renderer.
```

## Runtime posture

This is a protocol/refactor slice only. It does not add REST server behavior, wrapper ecosystem
implementation, benchmark execution, runtime expansion, external engine invocation, network effects,
dataset probes, writes, or fallback execution.
