# Sandbox Governance Runtime Readiness Gate

## Purpose

This document records the GAR-0019-B report-only sandbox and governance runtime readiness gate.

The gate makes sandbox isolation, filesystem/network/environment permissions, resource limits,
timeouts, audit logs, dependency isolation, and deny-by-default diagnostics explicit before any
plugin, UDF, connector, API, model, or effectful runtime can claim support.

## Current Status

`sandbox_governance_gate_schema_version=shardloom.sandbox_governance_readiness_gate.v1`

`sandbox_governance_gate_claim_gate_status=not_claim_grade`

`sandbox_governance_gate_all_sandbox_runtime_blocked=true`

The gate is intentionally report-only. It does not execute sandboxed code, spawn a sandbox process,
load plugins, run UDFs, resolve credentials, read environment variables, open network connections,
perform filesystem effects, enforce production resource limits, emit runtime audit logs, invoke
external engines, or attempt fallback execution.

## Surfaces

The gate is exposed through:

- `shardloom security-governance-evidence-gate --format json`
- `shardloom capabilities security-governance --format json`

## Summary Fields

- `sandbox_governance_gate_schema_version=shardloom.sandbox_governance_readiness_gate.v1`
- `sandbox_governance_gate_id=gar-0019-b.sandbox_governance_runtime_readiness`
- `sandbox_governance_gate_support_status=report_only`
- `sandbox_governance_gate_claim_gate_status=not_claim_grade`
- `sandbox_governance_gate_all_sandbox_runtime_blocked=true`
- `sandbox_governance_gate_deny_by_default=true`
- `sandbox_governance_gate_sandbox_runtime_supported=false`
- `sandbox_governance_gate_sandbox_process_spawned=false`
- `sandbox_governance_gate_extension_code_executed=false`
- `sandbox_governance_gate_udf_code_executed=false`
- `sandbox_governance_gate_filesystem_access_allowed=false`
- `sandbox_governance_gate_network_access_allowed=false`
- `sandbox_governance_gate_environment_access_allowed=false`
- `sandbox_governance_gate_secret_access_allowed=false`
- `sandbox_governance_gate_process_execution_allowed=false`
- `sandbox_governance_gate_resource_limits_enforced=false`
- `sandbox_governance_gate_timeout_enforced=false`
- `sandbox_governance_gate_audit_required=true`
- `sandbox_governance_gate_audit_log_runtime_supported=false`
- `sandbox_governance_gate_deterministic_unsupported_diagnostics=true`
- `sandbox_governance_gate_production_governance_runtime_supported=false`
- `sandbox_governance_gate_external_effect_executed=false`
- `sandbox_governance_gate_fallback_attempted=false`
- `sandbox_governance_gate_external_engine_invoked=false`

## Readiness Rows

The gate classifies these rows:

- `sandbox_profile_inventory`
- `filesystem_permission`
- `network_permission`
- `environment_access`
- `secret_access`
- `process_execution`
- `resource_limits`
- `execution_timeout`
- `audit_log`
- `dependency_isolation`
- `unsupported_diagnostics`

Every row exposes:

- readiness surface
- support status
- default policy
- blocker id
- deterministic diagnostic code
- required evidence
- user-visible surface
- sandbox enforcement status
- filesystem/network/environment/secret/process permission booleans
- resource-limit and timeout enforcement booleans
- audit-log runtime emission status
- external-effect, fallback, and external-engine booleans
- claim boundary

## Claim Boundary

ShardLoom may claim only that it exposes a deterministic report-only sandbox/governance readiness
gate.

ShardLoom may not claim:

- sandbox runtime support
- plugin or UDF execution
- sandbox process spawning
- filesystem permission enforcement
- network permission enforcement
- environment access
- secret access
- process execution
- production resource-limit enforcement
- timeout enforcement
- runtime sandbox audit emission
- dependency isolation runtime
- governed production runtime
- external effects
- external engine invocation
- fallback execution

## Verification

Expected verification:

```powershell
cargo test -p shardloom-core sandbox_governance_readiness_gate_blocks_runtime_by_default
cargo test -p shardloom-cli --test security_governance_evidence_gate
cargo test -p shardloom-cli --test capability_discovery_snapshots
cargo test -p shardloom-contract-tests --test release_readiness_metadata
git diff --check
```
