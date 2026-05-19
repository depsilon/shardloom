<!-- SPDX-License-Identifier: Apache-2.0 -->

# Extension Manifest Effect Capability Matrix

Status: implemented report-only contract for `GAR-0011-A`.

`shardloom.extension_manifest_effect_capability_matrix.v1` classifies extension manifest types,
required permissions, sandbox posture, effect metadata, materialization boundaries, and default
runtime blockers. It complements `shardloom.external_effect_blocker_matrix.v1`: the external-effect
matrix blocks operations such as UDFs, API calls, LLM calls, embeddings, vector search, plugin
execution, media extraction, and network egress; this matrix explains which extension families would
need those policies before any extension runtime can be admitted.

The matrix is emitted by:

- `shardloom extension-registry --format json`
- `shardloom extension-inspect <extension_id> --format json`
- `shardloom udf-runtime-plan <runtime> --format json`
- `shardloom capabilities extensions --format json`
- `shardloom capabilities udfs --format json`
- `shardloom capabilities security-governance --format json`

## Summary Fields

```text
extension_manifest_effect_matrix_schema_version=shardloom.extension_manifest_effect_capability_matrix.v1
extension_manifest_effect_matrix_id=gar-0011-a.extension_manifest_external_effect_capability_matrix
extension_manifest_effect_docs_ref=docs/architecture/extension-manifest-effect-capability-matrix.md
extension_manifest_effect_claim_gate_status=not_claim_grade
extension_manifest_effect_all_runtime_blocked=true
extension_manifest_effect_all_external_effects_blocked=true
extension_manifest_effect_runtime_execution=false
extension_manifest_effect_extension_code_executed=false
extension_manifest_effect_dynamic_loading=false
extension_manifest_effect_udf_execution=false
extension_manifest_effect_external_effect_executed=false
extension_manifest_effect_credential_resolution_performed=false
extension_manifest_effect_network_probe_performed=false
extension_manifest_effect_dependency_expansion_allowed=false
extension_manifest_effect_fallback_attempted=false
extension_manifest_effect_external_engine_invoked=false
```

## Rows

| Row | Status | Meaning |
| --- | --- | --- |
| `metadata_only_manifest` | `report_only` | Manifest metadata can be inspected without loading extension code. |
| `sql_frontend_extension` | `report_only` | SQL frontend extension metadata can be described, but no parser/binder/planner/runtime is enabled. |
| `rust_udf_extension` | `blocked` | Rust UDF execution requires ABI, registry, sandbox, determinism, and evidence contracts. |
| `wasm_udf_extension` | `blocked` | WASM UDF execution requires a WASM runtime, fuel/memory policy, sandbox, and evidence. |
| `python_udf_extension` | `blocked` | Python UDF execution requires a Python boundary, materialization policy, sandbox, redaction, and evidence. |
| `encoded_kernel_extension` | `blocked` | Encoded-kernel extensions require a kernel registry, encoding support, correctness, and decode evidence. |
| `translation_sink_extension` | `blocked` | Sink extensions require write, replay, Native I/O, and commit evidence. |
| `connector_extension` | `blocked` | Connector extensions require adapter, credential, network, source/sink certificate, and no-fallback evidence. |
| `object_store_provider_extension` | `blocked` | Object-store providers require credential, network, byte-range, commit, and Native I/O evidence. |
| `catalog_provider_extension` | `blocked` | Catalog providers require catalog, credential, table metadata, and transaction policy evidence. |
| `api_llm_effect_provider` | `blocked` | API/LLM effect providers are denied by default; no credentials, network, data egress, or model call is enabled. |
| `embedding_vector_provider` | `blocked` | Embedding/vector providers are denied by default; no model call, vector generation, or remote index query is enabled. |
| `observability_exporter_extension` | `report_only` | Exporter metadata can be described; lineage/telemetry export remains opt-in and disabled by default. |
| `benchmark_provider_extension` | `report_only` | Benchmark-provider metadata is external-baseline-only and cannot satisfy runtime evidence. |

## Row Fields

Each row exposes:

```text
extension_type
support_status
manifest_status
required_permissions
sandbox_policy
effect_metadata
materialization_boundary_required
blocker_id
diagnostic_code
required_evidence
runtime_execution=false
extension_code_executed=false
dynamic_loading=false
udf_execution=false
external_effect_executed=false
credential_resolution_performed=false
network_probe_performed=false
dependency_expansion_allowed=false
fallback_attempted=false
external_engine_invoked=false
claim_boundary
```

## Claim Boundary

Allowed current claim:

```text
ShardLoom exposes a deterministic report-only extension manifest/effect capability matrix.
```

Not allowed:

- no extension execution claim
- no plugin dynamic-loading claim
- no UDF runtime claim
- no API/LLM/embedding/vector/model-call runtime claim
- no object-store/catalog/connector runtime claim
- no credential resolution claim
- no network effect claim
- no dependency expansion claim
- no fallback or external-engine execution claim
- no production extension platform claim

## Verification

Use:

```powershell
cargo test -p shardloom-core extension_manifest_effect_capability_matrix_blocks_runtime_and_effects
cargo test -p shardloom-cli --test extension_manifest_effect_matrix_snapshots
cargo test -p shardloom-cli --test capability_discovery_snapshots
```
