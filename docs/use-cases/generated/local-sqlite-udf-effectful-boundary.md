<!-- SPDX-License-Identifier: Apache-2.0 -->

# Local SQLite and deterministic UDF effect boundary

## Quick Answer

- **Audience:** user checking whether local database files, UDFs, or extensions can run without hidden effects
- **Status:** `smoke_supported`
- **Execution mode:** `local_effectful_fixture_smokes`
- **Engine mode:** `batch`
- **Claim boundary:** Local fixture-smoke boundary only: SQLite support is a named local table scan to workspace-safe JSONL plus roundtrip SQLite replay, and UDF support is the built-in deterministic nullable-int64 scalar fixture. No arbitrary SQL, SQLite query pushdown, SQLite Vortex ingest, BLOB export, network database/warehouse connector, credentials, plugin loading, Python/WASM/Rust extension execution, LLM/API/embedding/vector effect, production connector, performance, or fallback claim.

## Can ShardLoom Do This?

Local SQLite and deterministic UDF effect boundary has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

Local fixture-smoke boundary only: SQLite support is a named local table scan to workspace-safe JSONL plus roundtrip SQLite replay, and UDF support is the built-in deterministic nullable-int64 scalar fixture. No arbitrary SQL, SQLite query pushdown, SQLite Vortex ingest, BLOB export, network database/warehouse connector, credentials, plugin loading, Python/WASM/Rust extension execution, LLM/API/embedding/vector effect, production connector, performance, or fallback claim.

## How To Try It

```powershell
python -c "import pathlib, sqlite3; pathlib.Path('target').mkdir(exist_ok=True); db='target/orders.sqlite'; con=sqlite3.connect(db); con.execute('drop table if exists orders'); con.execute('create table orders(id integer primary key, label text, amount integer)'); con.executemany('insert into orders(label, amount) values (?, ?)', [('alpha', 8), ('beta', 15)]); con.commit(); con.close()"; cargo run -q -p shardloom-cli -- sqlite-local-import-export-smoke target\orders.sqlite --table orders --export-jsonl target\orders-sqlite.jsonl --roundtrip-db target\orders-roundtrip.sqlite --order-by id --allow-overwrite --format json; cargo run -q -p shardloom-cli -- udf-local-scalar-fixture-smoke 1,null,3 --format json
```

## Blocker

No current blocker is attached to this supported local smoke path beyond the claim boundary above.

## Internal Flow

`local_sqlite_file, sqlite_table_name, nullable_int64_values, extension_manifest_id -> local_effectful_fixture_smokes -> batch -> workspace_safe_jsonl_export, roundtrip_sqlite_artifact, deterministic_udf_fixture_result, extension_manifest_metadata, effectful_operation_admission_matrix -> evidence -> claim gate`

## Evidence You Should See

- `schema_version=shardloom.local_sqlite_import_export_smoke.v1`
- `source_adapter_id=sqlite_input_adapter`
- `sqlite_sql_execution_scope=single_table_scan_only`
- `sqlite_query_pushdown_allowed=false`
- `sqlite_ordering_execution_scope=shardloom_fixture_post_scan`
- `roundtrip_replay_verified=true`
- `credential_policy_status=not_required_local_file_only`
- `network_policy=disabled_no_network_probe`
- `udf_id=sl_fixture_double_i64`
- `udf_deterministic=true`
- `udf_null_behavior=null_propagating`
- `extension_code_executed=false`
- `dynamic_loading_performed=false`
- `external_effect_executed=false`
- `effectful_operation_admission_all_external_and_sandboxed_paths_blocked=true`
- `fallback_attempted=false`
- `external_engine_invoked=false`
- `claim_gate_status=fixture_smoke_only`

## Expected Output Or Evidence

SQLite smoke JSON exposes single-table-scan/no-query-pushdown fields, workspace-safe export evidence, roundtrip replay evidence, and effectful-operation admission rows; UDF smoke JSON exposes the built-in deterministic nullable-int64 fixture contract with external_effect_executed=false, fallback_attempted=false, and external_engine_invoked=false.

## Common Mistakes

- `expecting_sqlite_query_pushdown`
- `treating_sqlite_smoke_as_vortex_ingest`
- `expecting_arbitrary_udf_execution`
- `assuming_extension_manifest_inspection_loads_code`
- `treating_effect_budget_as_network_permission`

## Reference Files

- `docs/architecture/effectful-operation-admission-matrix.md` - What this proves: Effectful-operation admission rows for local SQLite, extension metadata, deterministic UDF fixture, and blocked external effects.
- `docs/architecture/universal-ingress-route-taxonomy.md` - What this proves: UniversalIngress, Vortex ingest, prepared-state, and route-timing contract boundaries.
- `docs/architecture/universal-compatibility-coverage-scoreboard.md` - What this proves: Compatibility scoreboard status and source/sink support boundaries.
- `docs/architecture/extension-manifest-effect-capability-matrix.md` - What this proves: Extension manifest inspection posture and blockers for dynamic loading, plugin execution, and arbitrary UDF execution.
- `python/README.md` - What this proves: Python wrapper scope, local smoke usage, and Python API claim boundaries.

## Related Use Cases

- `vortex-ingest-prepare-once-local`
- `evidence-audit-claim-gates`
- `sql-dataframe-capability-posture`

## Related Field Guide Terms

- `website/field-guide/local-sqlite-fixture.html` - Local SQLite fixture (`UniversalIngress` / `smoke_supported`)
- `website/field-guide/effectful-operation-admission.html` - Effectful operation admission (`Evidence + Certificates` / `smoke_supported`)
