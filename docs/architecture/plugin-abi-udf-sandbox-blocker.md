# Plugin ABI And UDF Sandbox Blocker

GAR-0023-A adds `shardloom.plugin_abi_udf_sandbox_blocker.v1` as the deterministic blocker for
plugin ABI loading, dynamic extension loading, and UDF execution.

This is a report-only contract. It does not load plugin code, execute UDFs, enforce a sandbox,
resolve credentials, perform network probes, expand dependencies, invoke external engines, or
attempt fallback execution.

## User-Visible Surfaces

- `extension-registry --format json`
- `extension-inspect <extension_id> --format json`
- `udf-runtime-plan <runtime> --format json`
- `capabilities udfs --format json`
- `capabilities extensions --format json`
- `capabilities security-governance --format json`

## Summary Fields

- `plugin_abi_udf_sandbox_blocker_schema_version=shardloom.plugin_abi_udf_sandbox_blocker.v1`
- `plugin_abi_udf_sandbox_blocker_id=gar-0023-a.plugin_abi_udf_sandbox_blocker`
- `plugin_abi_udf_sandbox_blocker_support_status=report_only`
- `plugin_abi_udf_sandbox_blocker_claim_gate_status=not_claim_grade`
- `plugin_abi_udf_sandbox_blocker_all_plugin_runtime_blocked=true`
- `plugin_abi_udf_sandbox_blocker_abi_loading_supported=false`
- `plugin_abi_udf_sandbox_blocker_dynamic_loading_performed=false`
- `plugin_abi_udf_sandbox_blocker_extension_code_executed=false`
- `plugin_abi_udf_sandbox_blocker_udf_execution_performed=false`
- `plugin_abi_udf_sandbox_blocker_sandbox_evidence_required=true`
- `plugin_abi_udf_sandbox_blocker_sandbox_enforced=false`
- `plugin_abi_udf_sandbox_blocker_permission_policy_enforced=false`
- `plugin_abi_udf_sandbox_blocker_runtime_execution=false`
- `plugin_abi_udf_sandbox_blocker_external_effect_executed=false`
- `plugin_abi_udf_sandbox_blocker_credential_resolution_performed=false`
- `plugin_abi_udf_sandbox_blocker_network_probe_performed=false`
- `plugin_abi_udf_sandbox_blocker_dependency_expansion_allowed=false`
- `plugin_abi_udf_sandbox_blocker_fallback_attempted=false`
- `plugin_abi_udf_sandbox_blocker_external_engine_invoked=false`

## Blocker Rows

| Row | Status | Boundary |
| --- | --- | --- |
| `abi_contract_inventory` | `report_only` | Plugin ABI metadata may be inventoried, but ABI runtime support is not stable and no plugin code loads. |
| `dynamic_library_loading` | `blocked` | Native/dynamic loading is blocked until ABI compatibility, signature, dependency isolation, provenance, and sandbox evidence exist. |
| `rust_native_udf` | `blocked` | Rust-native UDF execution is blocked until ABI, registry, type, determinism, sandbox, and certificate evidence exist. |
| `wasm_udf` | `blocked` | WASM UDF execution is blocked until WASM runtime, fuel, memory, timeout, sandbox, and certificate evidence exist. |
| `python_udf` | `blocked` | Python UDF execution is blocked; there is no callable bridge, row materialization path, or fallback execution. |
| `sql_defined_udf` | `blocked` | SQL-defined UDF execution is blocked until SQL parser/binder/planner and function registry evidence exist. |
| `external_service_udf` | `blocked` | External-service UDF execution is blocked; no network, credential resolution, API call, or external effect is performed. |
| `table_function_udf` | `blocked` | Table-function UDF execution is blocked until source/sink and materialization boundaries are certified. |
| `plugin_lifecycle_transition` | `blocked` | Plugin lifecycle transitions beyond metadata inspection are blocked; metadata is not loaded, enabled, or executed. |
| `sandbox_evidence_binding` | `blocked` | Plugin/UDF admission is blocked until credential and sandbox governance gates are runtime evidence-bearing. |
| `license_provenance_attestation` | `report_only` | License/provenance metadata can be inspected but does not authorize dependency expansion or runtime support. |
| `unsupported_diagnostics` | `report_only` | Unsupported requests must emit deterministic diagnostics without loading code, executing UDFs, or invoking fallback engines. |

## Evidence Requirements

Future runtime admission must attach workload-scoped evidence before any support claim can change:

- ABI schema and version policy.
- Manifest schema and lifecycle policy.
- Signature/provenance and dependency-isolation evidence.
- Credential policy gate evidence.
- Sandbox governance gate evidence.
- Function registry and type-contract evidence.
- Determinism, materialization, timeout, memory, and effect-budget evidence.
- Execution certificate, Native I/O certificate where source/sink I/O is involved, and no-fallback evidence.

## Claim Boundary

After GAR-0023-A, ShardLoom may claim only that plugin ABI, dynamic-loading, and UDF sandbox
surfaces expose deterministic report-only/blocker diagnostics.

ShardLoom must not claim:

- plugin ABI runtime support,
- dynamic extension loading,
- UDF execution,
- sandbox runtime enforcement,
- production governance enforcement,
- credential resolution,
- network or external-service UDF effects,
- dependency expansion,
- no fallback execution support, or
- external-engine execution.
