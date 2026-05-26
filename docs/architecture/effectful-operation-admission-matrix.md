# Effectful Operation Admission Matrix

Status: implemented fixture-smoke contract for `GAR-RUNTIME-IMPL-4R/5O`.

`shardloom.effectful_operation_admission_matrix.v1` is the shared policy surface for adapters,
databases, extension manifests, UDFs, and external effects. It is emitted through
`effect-budget-plan`, `extension-registry`, `extension-inspect`, `udf-runtime-plan`,
`udf-local-scalar-fixture-smoke`, `sqlite-local-import-export-smoke`, and relevant
`capabilities` scopes.

## Admitted Local Behaviors

- `local_sqlite_import_export`: `sqlite-local-import-export-smoke` table-scans a named local SQLite
  table, writes a workspace-safe JSONL export, and creates a roundtrip local SQLite artifact with
  row-count replay evidence. Optional ordering is applied by the ShardLoom fixture layer after the
  single table scan; SQLite query pushdown and BLOB schemas/values are blocked.
- `typed_extension_manifest_inspection`: extension metadata is inspectable without loading code.
- `deterministic_scalar_udf_fixture`: `udf-local-scalar-fixture-smoke` runs the built-in
  nullable-int64 `sl_fixture_double_i64` scalar fixture with pure deterministic/null-propagating
  evidence.

## Blocked Paths

Networked databases/warehouses, REST/Flight/ADBC connectors, Python UDFs, WASM/dynamic plugin UDFs,
LLM/API/embedding/vector effects, credential resolution, network probes, dynamic loading,
extension-code execution, dependency expansion, external effects, fallback, and external-engine
execution remain blocked by default.

## Contract Fields

Summary fields include:

```text
effectful_operation_admission_matrix_schema_version
effectful_operation_admission_matrix_id
effectful_operation_admission_claim_gate_status=fixture_smoke_only
effectful_operation_admission_admitted_local_fixture_count
effectful_operation_admission_metadata_only_count
effectful_operation_admission_blocked_count
effectful_operation_admission_all_external_and_sandboxed_paths_blocked=true
effectful_operation_admission_fallback_attempted=false
effectful_operation_admission_external_engine_invoked=false
```

Every row exposes support status, admission scope, permission/effect status, blocker/diagnostic ids,
required evidence, credential/network/sandbox flags, local-filesystem fixture allowance,
runtime-fixture availability, dynamic-loading/code-execution/effect/fallback/external-engine flags,
and a claim boundary.

## Claim Boundary

This admits only local fixture smokes and metadata inspection. It does not add broad connector
support, arbitrary SQL pushdown, production SQLite/database support, Vortex ingest for SQLite
sources, plugin execution, arbitrary UDF support, LLM/API/model calls, credentials, network effects,
fallback execution, performance claims, or external-engine delegation.

## Verification

Use:

```powershell
cargo +1.91.1 test -p shardloom-core effectful_operation_admission_matrix_admits_only_local_fixtures --lib
cargo +1.91.1 test -p shardloom-cli --test sqlite_local_runtime_snapshots
cargo +1.91.1 test -p shardloom-cli --test extension_manifest_effect_matrix_snapshots
cargo +1.91.1 test -p shardloom-cli --test effect_budget_plan_snapshots
```
