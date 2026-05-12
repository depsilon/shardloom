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
Migrate command families from ad hoc field construction to typed payload helpers.
Attach real execution certificates, Native I/O certificates, evidence artifacts, benchmark rows,
source/sink reports, and future Foundry reports through typed slots.
Expand golden fixtures for success, unsupported, blocked, certified execution, evidence incomplete,
source-backed execution, benchmark rows, missing binary, and Foundry boundary reports.
Split CLI handlers by capability family and centralize rendering, diagnostics, fallback, policy,
and side-effect reporting.
```

## Runtime posture

This is a protocol/refactor slice only. It does not add REST server behavior, wrapper ecosystem
implementation, benchmark execution, runtime expansion, external engine invocation, network effects,
dataset probes, writes, or fallback execution.
