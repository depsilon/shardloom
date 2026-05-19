<!-- SPDX-License-Identifier: Apache-2.0 -->

# Credential Policy Enforcement Gate

Status: implemented report-only contract for `GAR-0019-A`.

`shardloom.credential_policy_enforcement_gate.v1` classifies the current credential and policy
runtime boundary. It is a deterministic report-only gate: it may inventory credential references,
policy blockers, redaction requirements, and unsupported diagnostics, but it does not resolve
credentials, load secrets, probe networks, enforce a production permission runtime, or authorize
external effects.

The gate is emitted by:

- `shardloom security-governance-evidence-gate --format json`
- `shardloom capabilities security-governance --format json`

## Summary Fields

```text
credential_policy_gate_schema_version=shardloom.credential_policy_enforcement_gate.v1
credential_policy_gate_id=gar-0019-a.credential_lifecycle_policy_enforcement_gate
credential_policy_gate_docs_ref=docs/architecture/credential-policy-enforcement-gate.md
credential_policy_gate_support_status=report_only
credential_policy_gate_claim_gate_status=not_claim_grade
credential_policy_gate_all_credential_runtime_blocked=true
credential_policy_gate_credential_references_only=true
credential_policy_gate_credential_resolution_performed=false
credential_policy_gate_secret_loading_performed=false
credential_policy_gate_secret_value_materialized=false
credential_policy_gate_runtime_permission_checks_enforced=false
credential_policy_gate_workspace_policy_enforced=false
credential_policy_gate_production_policy_runtime_supported=false
credential_policy_gate_redaction_required=true
credential_policy_gate_audit_required=true
credential_policy_gate_network_probe_performed=false
credential_policy_gate_external_effect_executed=false
credential_policy_gate_fallback_attempted=false
credential_policy_gate_external_engine_invoked=false
```

## Rows

| Row | Status | Meaning |
| --- | --- | --- |
| `credential_reference_inventory` | `report_only` | Credential references may be inventoried as metadata only. |
| `secret_loading` | `blocked` | Secret loading is denied until explicit policy, audit, redaction, permission, and runtime evidence exist. |
| `environment_secret_provider` | `blocked` | Environment secrets are reference-only; ShardLoom does not read secret values from the environment. |
| `file_secret_provider` | `blocked` | File secrets are reference-only; ShardLoom does not read secret files. |
| `external_secret_manager_provider` | `blocked` | External secret managers are blocked; no provider client or network probe is performed. |
| `cloud_iam_provider` | `blocked` | Cloud IAM credential exchange is blocked; no metadata-service or provider-token call is performed. |
| `workspace_policy` | `report_only` | Workspace/path policy can be reported for path-safety surfaces, but production policy runtime remains incomplete. |
| `runtime_permission_check` | `blocked` | Runtime permission checks remain blocked for production claims until enforcement evidence exists for every effectful operation. |
| `redaction_policy` | `report_only` | Strict redaction is required; this row does not claim a production redaction runtime. |
| `unsupported_diagnostics` | `report_only` | Unsupported credential paths must emit deterministic diagnostics without resolving credentials or invoking fallback. |

## Row Fields

Each row exposes:

```text
lifecycle_surface
support_status
default_policy
blocker_id
diagnostic_code
required_evidence
user_visible_surface
credential_resolution_performed=false
secret_loading_performed=false
secret_value_materialized=false
runtime_permission_check_enforced
workspace_policy_enforced
redaction_required=true
audit_required=true
network_probe_performed=false
external_effect_executed=false
fallback_attempted=false
external_engine_invoked=false
claim_boundary
```

## Claim Boundary

Allowed current claim:

```text
ShardLoom exposes a deterministic report-only credential lifecycle and policy enforcement gate.
```

Not allowed:

- no secret loading claim
- no credential resolution claim
- no network credential-provider probe claim
- no environment secret read claim
- no file secret read claim
- no external secret-manager or cloud IAM runtime claim
- no production runtime permission-enforcement claim
- no production workspace-policy runtime claim
- no external-effect claim
- no fallback or external-engine execution claim
- no governed production runtime claim

## Verification

Use:

```powershell
cargo test -p shardloom-core credential_policy_enforcement_gate_blocks_secret_runtime_by_default
cargo test -p shardloom-cli --test security_governance_evidence_gate
cargo test -p shardloom-cli --test capability_discovery_snapshots
```
