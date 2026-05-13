<!-- SPDX-License-Identifier: Apache-2.0 -->

# Security Threat Model

Status: P8.0A initial threat-model skeleton for RFC 0043. This document is a release-readiness
input and does not authorize runtime behavior, package publication, secrets, external engines, or
fallback execution.

## Scope

This threat model covers ShardLoom's local compute-engine release path and the future surfaces that
must inherit the same policy:

- local files and workspaces
- Vortex artifacts
- compatibility inputs: CSV, JSONL, Parquet, Arrow IPC, Avro, and ORC
- generated result artifacts
- evidence artifacts, logs, and diagnostics
- benchmark fixtures and outputs
- dependency, package, and release workflows
- future REST, object-store, Foundry, and platform handles

## Assets

ShardLoom must protect:

- reviewed source code
- release artifacts and package metadata
- publishing workflow integrity
- local user files
- generated Vortex files and result sinks
- benchmark outputs
- diagnostics and evidence artifacts
- future credentials, API tokens, platform handles, and governed data references
- the no-fallback guarantee

## Trust Boundaries

Treat the following as untrusted unless a later certificate says otherwise:

- user-supplied input paths
- compatibility files
- `.vortex` files
- output workspace paths
- benchmark fixture directories
- environment variables
- registry metadata and downloaded dependencies
- CI workflow inputs
- future remote API payloads
- future Foundry and object-store handles
- external engines used only as benchmark baselines

## Threat Matrix

| Threat | Required evidence | Required tests | Release blocker |
| --- | --- | --- | --- |
| Malicious Vortex artifact | `RuntimeInputSafetyReport` | malformed Vortex fixture does not panic | yes |
| Malformed CSV/JSONL/Parquet/Arrow/Avro/ORC | `RuntimeInputSafetyReport` | deterministic unsupported/error diagnostics | yes for supported formats |
| Path traversal | `WorkspacePathSafetyReport` | output path outside workspace rejected | yes |
| Unsafe symlink or hardlink writes | `WorkspacePathSafetyReport` | deterministic blocker or safe rejection | yes |
| Oversized or deeply nested inputs | `RuntimeInputSafetyReport` | size/depth limit diagnostics | yes for supported paths |
| Invalid UTF-8 | `RuntimeInputSafetyReport` | deterministic invalid-text diagnostic | yes for text paths |
| Resource exhaustion | runtime memory/spill evidence plus security diagnostics | fail-before-OOM or bounded blocker | yes for claimed workloads |
| Credential leakage in diagnostics/evidence | `EvidenceArtifactSafetyReport` | credential-like values redacted | yes |
| Poisoned benchmark artifact | benchmark constitution plus evidence artifact safety | fixture digest and no-fallback assertions | yes for public benchmark claims |
| Compromised dependency update | `DependencyAuditReport` | cargo-deny/audit/pip-audit gate | yes |
| Compromised CI/publishing workflow | `SupplyChainReleaseEvidence` | workflow policy snapshot | yes |
| Compromised package release | `VulnerabilityResponseReport` | response plan coverage | yes |
| External engine used as security bypass | no-fallback policy evidence | `fallback_attempted=false`, `external_engine_invoked=false` | yes |

## Required Reports

Release-readiness evidence should reference these report families from RFC 0043:

- `SecurityThreatModelReport`
- `DependencyAuditReport`
- `SupplyChainReleaseEvidence`
- `RuntimeInputSafetyReport`
- `WorkspacePathSafetyReport`
- `EvidenceArtifactSafetyReport`
- `VulnerabilityResponseReport`

## Current Maturity

Current state is `SEC-1 documented policy` for the threat model itself. Some dependency and release
scaffolding is `SEC-2 checked configuration`. Runtime malicious-input and workspace-path tests
remain planned under P8.0D before any public release gate can pass.

## No-Fallback Security Boundary

External engines may be installed in benchmark/dev environments as baselines or oracles only. They
must not execute unsupported ShardLoom work, sanitize malicious input on ShardLoom's behalf, or
turn a blocked path into a supported path.
