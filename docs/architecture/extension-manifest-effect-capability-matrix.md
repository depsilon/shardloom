<!-- SPDX-License-Identifier: Apache-2.0 -->

# Extension Manifest Effect Capability Matrix

Status: implemented report-only contract for `GAR-0011-A`.

`shardloom.extension_manifest_effect_capability_matrix.v1` classifies extension manifest types,
required permissions, sandbox posture, effect metadata, materialization boundaries, and default
runtime blockers. It complements `shardloom.external_effect_blocker_matrix.v1`: the external-effect
matrix blocks operations such as UDFs, API calls, LLM calls, embeddings, vector search, plugin
execution, media extraction, and network egress; this matrix explains which extension families would
need those policies before any extension runtime can be admitted.

The current admitted runtime exception is not a plugin extension: it is the built-in
`udf-local-scalar-fixture-smoke` deterministic scalar fixture recorded in
`docs/architecture/effectful-operation-admission-matrix.md`. Dynamic plugin loading, arbitrary UDFs,
Python/WASM/Rust extension execution, and external-service UDFs remain blocked here.

`udf-registry --format json` exposes a typed, side-effect-free UDF registry for scalar, aggregate,
and table-function rows. The registry records encoded-native candidates and materialization-required
boundaries, but only the built-in nullable-int64 scalar fixture is admitted. Aggregate,
table-function, Python, WASM, Rust plugin, SQL-defined, and external-service UDF execution remain
blocked until separate sandbox, runtime, resource, audit, certificate, and no-fallback evidence is
available.

`extension-inspect --manifest <local-json>` now performs bounded local JSON manifest inspection for
`shardloom.extension_manifest.v1`. This is an agent-safe metadata path: it reads a caller-provided
regular local file under a fixed byte limit, parses declared capability/permission/effect/sandbox/
license/provenance/contract fields, and emits review/blocker evidence. It does not load a dynamic
library, execute extension code, run a UDF, resolve credentials, probe the network, expand
dependencies, or enable the manifest as runtime support.

`extension-registry --manifest-dir <local-dir>` performs approved local manifest-directory
discovery. It is non-recursive, admits only explicit regular local directories, reads sorted `.json`
manifest files through the same bounded parser, rejects duplicate manifest IDs, and emits a registry
snapshot. It does not scan default plugin paths or ambient environment locations.

The matrix is emitted by:

- `shardloom extension-registry --format json`
- `shardloom extension-registry --manifest-dir <local-dir> --format json`
- `shardloom extension-inspect <extension_id> --format json`
- `shardloom extension-inspect --manifest <local-json> --format json`
- `shardloom udf-registry --format json`
- `shardloom udf-runtime-plan <runtime> --format json`
- `shardloom udf-local-scalar-fixture-smoke <values> --format json`
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

`udf-registry` additionally emits:

```text
typed_udf_registry_schema_version=shardloom.typed_udf_registry.v1
typed_udf_registry_support_status=scoped_fixture_supported
typed_udf_registry_claim_gate_status=fixture_smoke_only
typed_udf_registry_row_order=<comma-separated udf ids>
typed_udf_registry_admitted_local_fixture_count=1
typed_udf_registry_blocked_count=<blocked row count>
typed_udf_registry_scalar_count=<count>
typed_udf_registry_aggregate_count=<count>
typed_udf_registry_table_function_count=<count>
typed_udf_registry_encoded_native_candidate_count=<count>
typed_udf_registry_materialization_required_count=<count>
typed_udf_registry_local_fixture_execution_bridge_available=true
typed_udf_registry_arbitrary_runtime_bridge_available=false
typed_udf_registry_sandbox_policy_declared=true
typed_udf_registry_filesystem_access_allowed=false
typed_udf_registry_network_access_allowed=false
typed_udf_registry_secret_access_allowed=false
typed_udf_registry_dynamic_loading_allowed=false
typed_udf_registry_runtime_execution_performed=false
typed_udf_registry_extension_code_executed=false
typed_udf_registry_external_effect_executed=false
typed_udf_registry_fallback_attempted=false
typed_udf_registry_external_engine_invoked=false
typed_udf_registry_row_<udf_id>_kind=scalar|aggregate|table_function
typed_udf_registry_row_<udf_id>_encoded_capability=encoded_native_candidate|late_materialized_fixture|materialization_required|unsupported
typed_udf_registry_row_<udf_id>_materialization_required=true|false
typed_udf_registry_row_<udf_id>_support_status=admitted_local_fixture|blocked_missing_runtime_bridge|blocked_sandbox_policy|blocked_materialization_boundary
```

`extension-registry --manifest-dir` additionally emits:

```text
extension_registry_snapshot_schema_version=shardloom.extension_registry_snapshot.v1
extension_registry_input_kind=approved_local_manifest_directory
extension_registry_directory_read_performed=true
extension_registry_manifest_file_read_request_count=<manifest file count>
extension_registry_manifest_bytes_read=<bounded bytes read>
extension_registry_manifest_count=<count>
extension_registry_requires_review_count=<count>
extension_registry_contract_complete_count=<count>
extension_registry_contract_incomplete_count=<count>
extension_registry_manifest_ids=<comma-separated ids>
extension_registry_runtime_execution=false
extension_registry_extension_code_executed=false
extension_registry_external_effect_executed=false
extension_registry_fallback_attempted=false
extension_registry_external_engine_invoked=false
```

`extension-inspect --manifest` additionally emits:

```text
extension_manifest_inspection_schema_version=shardloom.extension_manifest_inspection.v1
extension_manifest_input_kind=local_manifest_file
extension_manifest_schema_version=shardloom.extension_manifest.v1
extension_manifest_json_parse_status=passed_no_code_loaded
extension_manifest_file_read_request_count=1
extension_manifest_bytes_read=<bounded local manifest bytes>
extension_manifest_inspection_status=validated|requires_review
extension_manifest_id=<manifest id>
extension_manifest_category=<declared category>
extension_manifest_capability_count=<count>
extension_manifest_supported_capability_claim_count=<count>
extension_manifest_permission_names=<comma-separated permissions>
extension_manifest_effect_kinds=<comma-separated effects>
extension_manifest_effect_levels=<comma-separated effect levels>
extension_manifest_execution_contract_complete=true|false
extension_manifest_determinism=pure_deterministic|pure_nondeterministic|external_effect_bound|unknown|unsupported
extension_manifest_materialization=metadata_only|encoded_native|late_materialized|materialization_required|unsupported
extension_manifest_materialization_required=true|false
extension_manifest_null_behavior=null_propagating|null_skipping|null_aware|null_error|unknown|unsupported
extension_manifest_input_dtypes=<comma-separated dtypes>
extension_manifest_output_dtype=<dtype>
extension_manifest_dtype_contract_declared=true|false
extension_manifest_timeout_millis=<milliseconds or 0 when missing>
extension_manifest_max_memory_bytes=<bytes or 0 when missing>
extension_manifest_max_cpu_millis=<milliseconds or 0 when missing>
extension_manifest_resource_contract_declared=true|false
extension_manifest_retry_policy=none|idempotent_retry|at_most_once|manual_replay_required|unsupported
extension_manifest_idempotency_policy=not_required|required|key_required|unsupported
extension_manifest_audit_policy=manifest_only|execution_certificate_required|full_audit_required|unsupported
extension_manifest_review_required=true|false
extension_manifest_runtime_execution=false
extension_manifest_dynamic_loading_performed=false
extension_manifest_extension_code_executed=false
extension_manifest_udf_execution_performed=false
extension_manifest_external_effect_executed=false
extension_manifest_credential_resolution_performed=false
extension_manifest_network_probe_performed=false
extension_manifest_dependency_expansion_allowed=false
extension_manifest_fallback_attempted=false
extension_manifest_external_engine_invoked=false
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
- no arbitrary UDF runtime claim beyond the separately admitted built-in deterministic fixture
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
