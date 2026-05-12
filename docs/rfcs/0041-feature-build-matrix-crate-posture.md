# RFC 0041: Feature/Build Matrix and Crate Posture

## Purpose

Make feature-gated build coverage and crate-level posture explicit before release-readiness work
expands.

ShardLoom now has many feature-gated and report-only paths. The build matrix, command availability,
and crate docs must state what compiles, what executes, what is report-only, and what remains
blocked.

## Status

Accepted as release-readiness and documentation-posture guidance.

This RFC does not authorize package publication, release tags, dependency expansion, runtime
expansion, object-store execution, external engine invocation, or fallback execution.

## Feature Matrix

The workspace must validate:

```text
default features
--all-features
--no-default-features where supported
upstream-vortex
vortex-file-io
vortex-local-primitives
vortex-encoded-read-spike
packaging/deployment surfaces
benchmark extras
future optional Foundry package surfaces
```

Docs/report-only commands should compile and run without requiring runtime feature gates. Feature-
disabled execution commands must return deterministic unsupported diagnostics.

## Build Evidence

Build matrix evidence should record:

```text
workspace crate
feature set
toolchain
target triple
command
status
unsupported command behavior
diagnostics
fallback_attempted=false
external_engine_invoked=false
```

Release/package claims remain blocked until the relevant matrix passes.

## Crate-Level Posture

Top-level crate docs and public export organization must clearly distinguish:

```text
executable local/prepared/source-backed paths
report-only contract surfaces
blocked/deferred runtime surfaces
future provider/adapter surfaces
prohibited external fallback
```

Stale setup-phase prose should be rewritten outright before release. Historical statements may stay
only when labeled as historical.

## Crates In Scope

```text
shardloom-core
shardloom-plan
shardloom-exec
shardloom-vortex
shardloom-cli
python wrapper docs where feature/runtime availability is described
```

## Acceptance

```text
Feature combinations are listed and verified before release claims.
Feature-disabled commands produce stable unsupported diagnostics.
Crate docs match current executable/report-only/blocked surfaces.
Public exports are grouped or documented by role.
No runtime behavior expands as part of docs/posture cleanup.
fallback_attempted=false remains visible where commands emit reports.
```
