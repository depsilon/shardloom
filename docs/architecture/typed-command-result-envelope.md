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

Shared CLI typed-envelope routing lives in `shardloom-cli/src/typed_envelope.rs`. Command handlers
still live mostly in `main.rs`; the module split is the first modularity step for shared rendering
and protocol behavior.

Command family classification lives in `shardloom-cli/src/command_family.rs` and is emitted in the
typed lifecycle payload as `command_family`. This gives status/capabilities, Vortex primitive,
prepared/source-backed, evidence/certificate, benchmark, packaging/deployment, Foundry,
operational-hardening, diagnostic, REST/API-planning, workflow-planning, and engine-runtime
commands a stable family taxonomy before their handlers are physically split.

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
evidence-incomplete benchmark row report
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
Migrate command-family-specific result fields from ad hoc field construction to typed payload helpers.
Attach inline evidence artifacts and richer report payloads through typed slots where a command has
more than a reference.
Finish remaining golden fixtures for certified runtime execution, missing-binary protocol parity,
and concrete Foundry boundary reports.
Physically split CLI handlers by capability family and centralize rendering, diagnostics, fallback,
policy, and side-effect reporting.
```

## Runtime posture

This is a protocol/refactor slice only. It does not add REST server behavior, wrapper ecosystem
implementation, benchmark execution, runtime expansion, external engine invocation, network effects,
dataset probes, writes, or fallback execution.
