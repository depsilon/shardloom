# RFC 0039: Typed Command/Result Envelope and CLI Modularity

## Purpose

Replace the early flat CLI output envelope and monolithic command routing shape with a typed
command/result/evidence protocol.

ShardLoom's CLI is currently the canonical local protocol transport for the Python wrapper,
contract tests, release checks, evidence reports, and future generated clients. It must become a
typed protocol surface, not a collection of ad hoc key/value fields.

## Status

Partially implemented as protocol and CLI implementation guidance.

The first implementation slices add `shardloom.output.v2`, typed payload slots, API/Python protocol
reporting, Python typed-payload parsing, shared CLI routing for common policy/lifecycle/capability
fields, conservative typed reference attachment for explicit result, artifact, and certificate
refs/ids/paths/URIs in `shardloom-cli/src/typed_envelope.rs`, command-family lifecycle
classification in `shardloom-cli/src/command_family.rs`, shared JSON/text rendering and error
emission in `shardloom-cli/src/cli_output.rs`, the status/capabilities handler-family module in
`shardloom-cli/src/status_capabilities.rs`, the input planning handler module in
`shardloom-cli/src/input_planning.rs`, the REST/API planning handler module in
`shardloom-cli/src/rest_api_planning.rs`, the packaging/deployment handler module in
`shardloom-cli/src/packaging_deployment.rs`, the benchmark planning handler module in
`shardloom-cli/src/benchmark_planning.rs`, the executable benchmark runtime handler module in
`shardloom-cli/src/benchmark_runtime.rs`, including `vortex-count-benchmark`, the operational
hardening/security handler module in `shardloom-cli/src/operational_hardening.rs`, the diagnostics
handler module in
`shardloom-cli/src/diagnostics.rs`, the evidence/certificate planning handler module in
`shardloom-cli/src/evidence_certificates.rs`, the workflow/table planning handler module in
`shardloom-cli/src/workflow_planning.rs`, the engine/runtime planning handler module in
`shardloom-cli/src/engine_runtime_planning.rs`, the extension/UDF planning handler module in
`shardloom-cli/src/extension_planning.rs`, the prepared/source-backed execution handler module in
`shardloom-cli/src/prepared_source_backed_execution.rs`, the first Vortex primitive execution
handler module in `shardloom-cli/src/vortex_primitive_execution.rs`, including
`vortex-count-where` and `vortex-query-trace`, and typed-envelope contract snapshots for
representative success, error, unsupported, blocked, evidence-incomplete, source-backed, benchmark,
and Foundry-adjacent report surfaces. Command-family-specific result migration, richer inline
artifact/report payloads, remaining certified-runtime/missing-binary/Foundry-boundary golden
fixtures, and broader CLI handler modularization remain planned.

This RFC does not authorize REST server behavior, generated clients, DB-API/SQLAlchemy/Ibis/dbt
wrappers, benchmark execution, runtime expansion, package publication, external engine invocation,
or fallback execution.

## Envelope Replacement

Because ShardLoom is unreleased, replace the early flat `(key, value)` payload model instead of
layering a backward-compatible v2.

The typed command/result/evidence envelope should expose:

```text
schema_version
command
status
summary
human_text
diagnostics
fallback
policy
lifecycle
capability_snapshot
result
result_refs
artifacts
artifact_refs
certificates
```

Human text is rendering-only. Machine-readable typed payloads are the source of truth.

## Artifact Attachment

The envelope must attach or reference:

```text
ExecutionCertificate
NativeIoCertificate
EvidenceArtifactEnvelope
MaterializationBoundaryReport
SourcePushdownReport
SinkRequirementReport
AdapterFidelityReport
ResidualBoundaryReport
BenchmarkConstitution
Benchmark rows
Foundry boundary reports
Capability snapshots
```

Large analytical payloads must use result refs, artifact refs, Vortex artifacts, object refs,
JSONL/paged JSON, Arrow boundaries, or future Flight/ADBC tickets as explicit result policies.

## CLI Modularity

Split command handlers by capability family:

```text
status/capabilities
Vortex primitive execution
prepared/source-backed execution
evidence/certificates
benchmarks
packaging/deployment
Foundry
operational hardening
diagnostics
future REST/API planning
```

Every handler must return the shared typed envelope through one renderer. Diagnostics, fallback
fields, policy fields, and side-effect reporting must be centralized.

## Contract Tests

Golden JSON fixtures must cover:

```text
success
unsupported
blocked
certified execution
evidence-incomplete execution
source-backed execution
benchmark row
missing binary
Foundry boundary report
```

Python wrappers must parse and preserve typed payloads rather than depending on human text or ad hoc
field names.

## Non-Goals

```text
old JSON compatibility guarantee
HTTP server
wrapper implementation
database driver implementation
external engine execution
fallback execution
```

## Acceptance

```text
Flat fields are no longer the primary payload model.
Representative command families use typed handlers and shared rendering.
No command may omit fallback status.
No command may probe datasets, execute external engines, materialize data, write, or perform network
effects unless its explicit command contract allows it and emits evidence.
```
