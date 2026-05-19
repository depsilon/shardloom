<!-- SPDX-License-Identifier: Apache-2.0 -->

# Comparative Rerun And Managed-Platform Posture Gate

Status: implemented fail-closed report contract for `GAR-0040-A`. This gate does not run
benchmarks, resolve credentials, probe networks, add managed-platform dependencies, invoke external
engines, or publish performance claims.

Schema:

```text
shardloom.comparative_rerun_managed_platform_gate.v1
```

Shared surface fields:

```text
comparative_rerun_managed_platform_gate_support_status=blocked
comparative_rerun_managed_platform_gate_claim_gate_status=not_claim_grade
comparative_rerun_managed_platform_gate_row_count=6
comparative_rerun_managed_platform_gate_blocking_row_count=6
comparative_rerun_managed_platform_gate_local_comparative_rerun_required=true
comparative_rerun_managed_platform_gate_local_comparative_rerun_performed=false
comparative_rerun_managed_platform_gate_external_baselines_comparison_only=true
comparative_rerun_managed_platform_gate_managed_platform_lanes_comparison_only=true
comparative_rerun_managed_platform_gate_managed_platform_credentials_required=true
comparative_rerun_managed_platform_gate_managed_platform_credentials_resolved=false
comparative_rerun_managed_platform_gate_managed_platform_dependencies_added=false
comparative_rerun_managed_platform_gate_managed_platform_execution_performed=false
comparative_rerun_managed_platform_gate_managed_platform_public_claim_allowed=false
comparative_rerun_managed_platform_gate_credential_resolution_performed=false
comparative_rerun_managed_platform_gate_network_probe_performed=false
comparative_rerun_managed_platform_gate_benchmark_artifact_required=true
comparative_rerun_managed_platform_gate_benchmark_artifact_claim_grade=false
comparative_rerun_managed_platform_gate_performance_claim_allowed=false
comparative_rerun_managed_platform_gate_superiority_claim_allowed=false
comparative_rerun_managed_platform_gate_spark_displacement_claim_allowed=false
comparative_rerun_managed_platform_gate_fallback_attempted=false
comparative_rerun_managed_platform_gate_external_engine_invoked=false
comparative_rerun_managed_platform_gate_all_claims_blocked=true
comparative_rerun_managed_platform_gate_managed_platforms_blocked_without_credentials=true
comparative_rerun_managed_platform_gate_side_effect_free=true
```

## Purpose

RFC 0040 keeps benchmark hardening local and platform-neutral by default. Managed systems such as
Photon, Fabric, Snowflake, BigQuery, Redshift, and Databricks managed services are design
references and optional external comparison targets, not ShardLoom runtime providers.

`GAR-0040-A` makes that boundary visible in the two user-facing claim surfaces that matter before
publication:

- `benchmark-claim-evidence-plan`
- `release-plan` / `package-plan`

## Gate Rows

| Row | Family | Status | Current blocker |
| --- | --- | --- | --- |
| `local_full_comparative_rerun` | local comparative rerun | blocked | A fresh full-local benchmark artifact with complete ShardLoom and local optional baseline lanes is not attached. |
| `external_baseline_oracle_rows` | external baseline | blocked | Complete comparison rows with versions, environment fingerprint, and `external_baseline_only=true` are not claim-grade. |
| `managed_platform_design_reference_rows` | managed platform | blocked | Managed-platform lanes are design references only; no credentials are resolved and no run is admitted. |
| `managed_platform_credential_policy` | credential policy | blocked | GAR-0019 credential policy remains report-only for managed-platform credentials, redaction, and network effects. |
| `claim_grade_artifact_publication` | claim gate | blocked | GAR-0041 per-claim evidence attachment and release approval are not complete. |
| `fallback_and_external_execution_boundary` | policy | blocked | External systems cannot satisfy ShardLoom execution evidence or fallback into ShardLoom claims. |

## Claim Rule

No performance, superiority, Spark-displacement, replacement, managed-platform, or public benchmark
claim may pass until a fresh local artifact exists, external lanes are explicitly baseline-only,
managed-platform credentials are explicitly admitted when used, environment evidence is attached,
GAR-0041 binds each public claim to evidence, and every ShardLoom row still reports:

```text
fallback_attempted=false
external_engine_invoked=false
```

## Non-Goals

- No benchmark rerun.
- No managed-platform benchmark run.
- No credential resolution.
- No network probe.
- No managed-platform dependency.
- No external engine invocation.
- No fallback execution.
- No performance, superiority, replacement, Spark-displacement, or managed-platform claim.
