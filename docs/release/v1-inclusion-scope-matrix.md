<!-- SPDX-License-Identifier: Apache-2.0 -->

# V1 Inclusion Scope Matrix

Status: v1 classification and unsupported-surface firewall.

Schema marker: `shardloom.v1_inclusion_scope_matrix.v1`.

This matrix classifies the active phase-plan queue by v1 scope. It is intentionally
inclusion-first: broad runtime families stay `v1_candidate_pending_feasibility` until their owning
phase either promotes a supported subset into v1, narrows the surface to deterministic unsupported
boundaries, or defers it with concrete infeasibility evidence.

## Contract Fields

```text
v1_inclusion_scope_schema_version=shardloom.v1_inclusion_scope_matrix.v1
v1_inclusion_scope_allowed_classifications=required_for_v1,v1_candidate_pending_feasibility,deferred_out_of_v1,documentation_only,unsupported_boundary
v1_inclusion_scope_required_rows_cannot_be_report_only=true
v1_inclusion_scope_deferred_rows_require_unsupported_diagnostics=true
v1_inclusion_scope_external_engine_fallback_allowed=false
v1_inclusion_scope_claim_gate_status=not_claim_grade
v1_inclusion_scope_public_release_claim_allowed=false
v1_inclusion_scope_public_package_claim_allowed=false
v1_inclusion_scope_performance_claim_allowed=false
v1_inclusion_scope_production_claim_allowed=false
v1_inclusion_scope_fallback_attempted=false
v1_inclusion_scope_external_engine_invoked=false
```

Technique review token used by every required/candidate row:

```text
dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier
```

## Classification Rows

| Phase item | Classification | Support gate posture | Feasibility status | Unsupported boundary | Technique review |
| --- | --- | --- | --- | --- | --- |
| `PY-RUNTIME-ACTIVATION-PROVIDER-PROMOTION-1` | `required_for_v1` | `implementation_gate_closed` | `closed_release_runtime_activation_and_provider_boundary_scope`; `docs/architecture/v1-front-door-runtime-scope.md`; `docs/architecture/v1-vortex-runtime-scope.md`; `docs/architecture/phased-execution-completed-ledger.md` | `not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `RELEASE-PACKAGE-0.1X-BUNDLED-CLI-1` | `v1_candidate_pending_feasibility` | `platform_wheel_clean_venv_proof_implemented` | `bundled_cli_strategy_resolver_and_local_platform_wheel_wiring_complete_patch_release_publication_pending` | `candidate_not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `PY-LOCAL-WORKFLOW-1M-PRODUCT-ROUTE-1` | `required_for_v1` | `implementation_gate_closed` | `closed_local_file_vortex_middle_policy_scope`; `docs/architecture/v1-front-door-runtime-scope.md`; `target/v1-front-door-runtime-scope-report.json` | `not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `PY-VORTEX-ROUTE-UNIFY-1` | `required_for_v1` | `implementation_gate_closed` | `closed_exact_native_vortex_provider_route_scope`; `docs/architecture/v1-vortex-runtime-scope.md`; `target/v1-vortex-runtime-scope-report.json` | `not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `PROD-V1-0B` | `required_for_v1` | `classification_gate_closed` | `closed_by_this_matrix` | `not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `PROD-V1-1A` | `required_for_v1` | `implementation_gate_closed` | `closed_front_door_scope` | `not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `PROD-V1-1B` | `required_for_v1` | `implementation_gate_closed` | `closed_vortex_runtime_scope`; `docs/architecture/v1-vortex-runtime-scope.md` | `not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `PROD-V1-1C` | `required_for_v1` | `implementation_gate_closed` | `closed_source_prepared_state_scope`; `docs/architecture/v1-source-prepared-state-scope.md` | `not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `PROD-V1-1D` | `required_for_v1` | `implementation_gate_closed` | `closed_local_output_sink_scope`; `docs/architecture/v1-local-output-sink-scope.md` | `not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `PROD-V1-2A` | `required_for_v1` | `implementation_required` | `required_api_diagnostics_scope` | `not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `PROD-V1-2B` | `required_for_v1` | `evidence_gate_closed` | `closed_correctness_scope`; `target/v1-example-replay-report.json`; `target/v1-correctness-conformance-report.json` | `not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `PROD-V1-2C` | `required_for_v1` | `evidence_gate_closed` | `closed_local_resource_safety_scope`; `docs/architecture/v1-local-resource-safety.md`; `target/v1-local-resource-safety-report.json` | `not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `PROD-V1-2D` | `required_for_v1` | `evidence_gate_closed` | `closed_observability_support_scope`; `docs/architecture/v1-observability-support.md`; `docs/release/troubleshooting-diagnostics.md`; `target/v1-observability-support-report.json` | `not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `PROD-V1-3A` | `required_for_v1` | `evidence_gate_closed` | `closed_security_ci_hardening_scope`; `docs/architecture/v1-security-ci-hardening.md`; `target/v1-security-ci-hardening-report.json` | `not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `PROD-V1-4A` | `required_for_v1` | `evidence_gate_closed` | `closed_docs_product_scope`; `docs/getting-started/v1-supported-unsupported.md`; `target/v1-docs-productization-report.json`; `target/v1-example-replay-report.json`; `target/website-readiness-report.json` | `not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `PROD-V1-5A` | `required_for_v1` | `implementation_required` | `required_package_gate_scope` | `not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `PERF-RUNTIME-7A` | `required_for_v1` | `implementation_gate_closed` | `closed_cold_route_source_elision_scope`; `docs/architecture/phased-execution-completed-ledger.md` | `not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `PERF-RUNTIME-7B` | `required_for_v1` | `implementation_required` | `required_operator_tail_perf_scope` | `not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `PERF-RUNTIME-7C` | `required_for_v1` | `implementation_required` | `required_prepared_route_attribution_scope` | `not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `PERF-RUNTIME-7D` | `required_for_v1` | `implementation_required` | `required_publication_proof_overhead_scope` | `not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `RELEASE-READY-16A` | `required_for_v1` | `implementation_required` | `required_release_boundary_scope` | `not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `PROD-READY-0A` | `required_for_v1` | `evidence_gate_closed` | `closed_common_production_certification_scope`; `docs/release/production-certification-workloads.json`; `target/production-certification-gate.json` | `not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `PROD-READY-1A` | `required_for_v1` | `evidence_gate_closed` | `closed_local_io_adapter_scope`; `docs/architecture/phased-execution-completed-ledger.md` | `not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `PROD-READY-1B` | `v1_candidate_pending_feasibility` | `feasibility_required` | `pending_object_store_local_emulator_profile_declared`; `object_store_local_emulator_runtime_v1_candidate`; `docs/release/production-certification-workloads.json` | `candidate_not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `PROD-READY-1C` | `v1_candidate_pending_feasibility` | `feasibility_required` | `pending_lakehouse_table_runtime_feasibility` | `candidate_not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `PROD-READY-1D` | `v1_candidate_pending_feasibility` | `feasibility_required` | `pending_distributed_runtime_feasibility` | `candidate_not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `PROD-READY-1E` | `v1_candidate_pending_feasibility` | `feasibility_required` | `pending_live_hybrid_runtime_feasibility` | `candidate_not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `PROD-READY-1F` | `v1_candidate_pending_feasibility` | `feasibility_required` | `pending_udf_plugin_effect_runtime_feasibility` | `candidate_not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |
| `PROD-READY-1G` | `v1_candidate_pending_feasibility` | `feasibility_required` | `pending_foundry_integration_pack_feasibility` | `candidate_not_deferred` | dynamic; capillary; PulseWeave; metadata-first; timing-surface; evidence-tier |

## Firewall Rules

- `required_for_v1` rows cannot be satisfied by `report_only`, `blocked`, `unsupported`, or
  `not_claim_grade` support-gate posture in this matrix. Their open phase-plan status means
  implementation evidence is required before v1 can close.
- `v1_candidate_pending_feasibility` rows remain in the v1 candidate pool. Their owner must promote
  a feasible subset, narrow the surface to a supported subset, or defer it with an explicit
  infeasibility reason.
- `deferred_out_of_v1` and `unsupported_boundary` rows are allowed only when deterministic
  unsupported diagnostics, `fallback_attempted=false`, and `external_engine_invoked=false` are
  named in the row boundary.
- External engines remain baselines, migration references, or test oracles only. They cannot make a
  row v1-supported.
