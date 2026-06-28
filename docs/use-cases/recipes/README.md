<!-- SPDX-License-Identifier: Apache-2.0 -->

# Use Case Recipes

These recipes are practical entry points for the Use Case Atlas. They are scoped local technical
preview recipes, not production, performance, SQL/DataFrame, object-store/lakehouse, Foundry
production, package-publication, or Spark-replacement claims.

The machine-readable index is `docs/use-cases/recipes/recipe-index.json` with schema
`shardloom.workflow_recipe_library.v1`. Validate it with:

```powershell
python scripts\check_workflow_recipes.py
```

Every indexed recipe maps back to a Use Case Atlas id, declares a claim boundary, and includes
`fallback_attempted=false` plus `external_engine_invoked=false` evidence fields.

| Recipe | Status | Use case |
| --- | --- | --- |
| No-Dataset Smoke | `ready_local` | `first-10-minutes-local-smoke` |
| Local CSV Certified Result | `smoke_supported` | `compatibility-import-certified-local` |
| Local Parquet Certified Result | `smoke_supported` | `compatibility-import-certified-local` |
| Prepared Vortex Batch Run | `smoke_supported` | `prepared-native-vortex-runtime-direction` |
| Native Vortex Input | `report_only` | `prepared-native-vortex-runtime-direction` |
| Source-Free Generated Reference Table | `smoke_supported` | `source-free-generated-output-boundary` |
| Dirty CSV Cleanup | `smoke_supported` | `messy-data-local-fixtures` |
| Nested JSON Scan | `smoke_supported` | `messy-data-local-fixtures` |
| CDC Overlay | `smoke_supported` | `messy-data-local-fixtures` |
| Output Fanout | `report_only` | `output-result-sink-and-fanout-boundary` |
| Object-Store Blocked Diagnostic | `blocked` | `object-store-boundary-report` |
| Foundry Dev-Stack Smoke | `smoke_supported` | `foundry-local-proof-boundary` |
| Benchmark Evidence Interpretation | `smoke_supported` | `benchmark-interpretation-evidence-not-leaderboard` |

## No-Dataset Smoke

- **User goal:** confirm the local CLI and Python wrapper can report status without reading data.
- **Command:**
  ```powershell
  python examples\local-python-smoke\run.py --repo-root .
  ```
- **Expected output:** status, smoke, and capabilities JSON.
- **Evidence fields:** `fallback_attempted=false`, `external_engine_invoked=false`,
  `protocol_version`, `resolved_cli_path`.
- **Claim boundary:** no dataset execution and no generated-output claim.
- **References:** `docs/getting-started/first-10-minutes.md`, `examples/local-python-smoke/README.md`.

## Local CSV Certified Result

- **User goal:** run a small local CSV compatibility-import-certified workload.
- **Command:**
  ```powershell
  python benchmarks\traditional_analytics\run.py --engines shardloom --formats csv --scenario "selective filter" --dataset-profile tiny_smoke --rows 256 --iterations 3 --shardloom-result-sink --skip-shardloom-native --no-markdown --output target\shardloom-csv-certified-smoke.json --regenerate
  ```
- **Expected output:** benchmark JSON with timing, coverage, result-sink, and certificate fields.
- **Evidence fields:** `execution_mode`, `compatibility_parse_millis`,
  `compatibility_to_vortex_import_millis`, `vortex_scan_millis`, `claim_gate_status`,
  `fallback_attempted=false`.
- **Claim boundary:** certification lane, not pure query speed.
- **References:** `docs/getting-started/certified-local-workload.md`,
  `docs/benchmarks/local-taxonomy-benchmark.md`.

## Local Parquet Certified Result

- **User goal:** run the same certification posture over local Parquet fixture input.
- **Command:**
  ```powershell
  python benchmarks\traditional_analytics\run.py --engines shardloom --formats parquet --scenario "selective filter" --dataset-profile tiny_smoke --rows 256 --iterations 3 --shardloom-result-sink --skip-shardloom-native --no-markdown --output target\shardloom-parquet-certified-smoke.json --regenerate
  ```
- **Expected output:** local benchmark artifact with separated compatibility-import timing.
- **Evidence fields:** `source_read_millis`, `vortex_write_millis`, `result_sink_write_millis`,
  `native_io_certificate_status`, `external_engine_invoked=false`.
- **Claim boundary:** scoped local proof only; no broad file-format or lakehouse claim.
- **References:** `docs/benchmarks/local-taxonomy-benchmark.md`.

## Prepared Vortex Batch Run

- **User goal:** inspect the current prepared/native runtime-development lane.
- **Command:**
  ```powershell
  python benchmarks\traditional_analytics\run.py --engines shardloom-prepared-vortex,shardloom-prepare-batch --formats csv,jsonl,parquet,arrow-ipc,avro,orc --scenario "filter + projection + limit" --dataset-profile tiny_smoke --rows 1000 --iterations 1 --output target\shardloom-prepared-vortex-smoke.json --regenerate
  ```
- **Expected output:** warm prepared Vortex rows separate from the single-process prepare/batch
  rows that carry `prepare_batch_*` adapter evidence.
- **Evidence fields:** `source_backed_scan_*`, `source_state_*`,
  `encoded_predicate_provider_*`, `prepare_batch_*`, `claim_gate_status`.
- **Claim boundary:** prepared/native smoke, not performance or encoded-native proof.
- **References:** `benchmarks/traditional_analytics/README.md`,
  `docs/architecture/compute-engine-flow-reference.md`.

## Native Vortex Input

- **User goal:** understand where existing Vortex input fits.
- **Command:** use prepared/native benchmark rows that explicitly label `native_vortex` when
  admitted by the scenario.
- **Expected output:** source-backed scan and Native I/O evidence fields.
- **Evidence fields:** `execution_mode=native_vortex`, `native_io_certificate_status`,
  `data_decoded`, `data_materialized`, `fallback_attempted=false`.
- **Claim boundary:** native Vortex input is scoped to admitted rows, not broad runtime support.
- **References:** `docs/architecture/compute-engine-flow-reference.md`,
  `docs/architecture/benchmark-suite-catalog.md`.

## Source-Free Generated Reference Table

- **User goal:** create a reference table without an input dataset.
- **Status:** scoped local user-row, literal-table, calendar/date-dimension, range, sequence, SQL
  VALUES/literal SELECT, SQL generate_series/range, and scoped SQL range-projection JSONL/CSV smokes supported.
- **Command:**
  ```powershell
  $env:PYTHONPATH = "python\src"
  python -c "from shardloom import context; r=context(repo_root='.').from_rows([{'id': 1, 'label': 'alpha'}, {'id': 2, 'label': 'beta'}]).write('target/generated-reference.jsonl', allow_overwrite=True); print(r.claim_gate_status)"
  ```
- **Equivalent CLI:**
  ```powershell
  shardloom generated-source-user-rows target\generated-reference.jsonl id:int64,label:utf8 "id=1,label=alpha;id=2,label=beta" --allow-overwrite --format json
  ```
- **Range example:**
  ```powershell
  $env:PYTHONPATH = "python\src"
  python -c "from shardloom import context; r=context(repo_root='.').range(0, 5, column='id').write('target/generated-range.jsonl', allow_overwrite=True); print(r.generated_source_kind, r.generated_source_row_count, r.claim_gate_status)"
  ```
- **Range CLI:**
  ```powershell
  shardloom generated-source-range target\generated-range.jsonl 0 5 --column id --allow-overwrite --format json
  ```
- **Sequence example:**
  ```powershell
  $env:PYTHONPATH = "python\src"
  python -c "from shardloom import context; r=context(repo_root='.').sequence(0, 5, column='id').write('target/generated-sequence.jsonl', allow_overwrite=True); print(r.generated_source_kind, r.generated_source_row_count, r.claim_gate_status)"
  ```
- **Sequence CLI:**
  ```powershell
  shardloom generated-source-sequence target\generated-sequence.jsonl 0 5 --column id --allow-overwrite --format json
  ```
- **Literal-table example:**
  ```powershell
  $env:PYTHONPATH = "python\src"
  python -c "from shardloom import context; r=context(repo_root='.').literal_table([{'code':'A','weight':1.5},{'code':'B','weight':2.0}]).write('target/generated-literal.jsonl', allow_overwrite=True); print(r.generated_source_kind, r.generated_source_row_count, r.claim_gate_status)"
  ```
- **Calendar example:**
  ```powershell
  $env:PYTHONPATH = "python\src"
  python -c "from shardloom import context; r=context(repo_root='.').calendar('2026-05-18','2026-05-21', column='dt').write('target/generated-calendar.jsonl', allow_overwrite=True); print(r.generated_source_kind, r.generated_source_row_count, r.claim_gate_status)"
  ```
- **Source-free SQL examples:**
  ```powershell
  $env:PYTHONPATH = "python\src"
  python -c "from shardloom import context; r=context(repo_root='.').sql_values(\"VALUES (1, 'alpha'), (2, 'beta')\").write('target/generated-sql-values.jsonl', allow_overwrite=True); print(r.generated_source_kind, r.generated_source_row_count, r.claim_gate_status)"
  python -c "from shardloom import context; r=context(repo_root='.').sql_literal_select(\"SELECT 1 AS id, 'alpha' AS label, true AS active\").write('target/generated-sql-select.jsonl', allow_overwrite=True); print(r.generated_source_kind, r.generated_source_row_count, r.claim_gate_status)"
  python -c "from shardloom import context; ctx=context(repo_root='.'); r=ctx.sql(\"SELECT * FROM generate_series(0, 4)\").write('target/generated-sql-series.jsonl', allow_overwrite=True); p=ctx.sql(\"SELECT value AS id, value + 1 AS next FROM range(0, 4)\").write('target/generated-sql-range-projection.jsonl', allow_overwrite=True); print(r.generated_source_kind, r.generated_source_row_count, r.generated_source_range_end_inclusive, p.sql_source_free_projection_columns, r.claim_gate_status)"
  ```
- **Expected output:** local JSONL/CSV output plus a generated-source/output evidence envelope.
- **Evidence fields:** `input_dataset_count=0`, `source_io_performed=false`,
  `generated_source_created=true`, `generated_source_certificate_status`,
  `output_native_io_certificate_status`, and for range/sequence smokes,
  `generated_source_range_start/end/step/column`; source-free SQL smokes also expose
  `sql_statement_kind`, SQL parser/binder/planner fields, and `generated_source_kind=sql_values`
  or `sql_literal_select` or `sql_generate_series_range`. SQL generator rows also expose
  `generated_source_sql_generator_function` and `generated_source_range_end_inclusive`.
  Capability discovery also exposes
  `generated_source_api_admission_schema_version` plus per-form `support_status`, `blocker_id`,
  and no-fallback/no-external-engine fields.
- **Claim boundary:** scoped local user-row, literal-table, calendar/date-dimension, range,
  sequence, SQL literal `SELECT`, SQL `VALUES`, SQL `generate_series`/`range`, scoped SQL range
  projection, scoped DataFrame literal projection, and scoped generated DataFrame `with_column`
  JSONL/CSV fixture smokes only; broad SQL runtime, SQL source-free projection over expressions
  beyond the admitted literals, broad expression-backed DataFrame generation, object-store writes,
  and Foundry generated-output runtime remain report-only/planned/blocked.
- **References:** `docs/foundry/proof-of-use-certification.md`,
  `docs/architecture/compute-engine-flow-reference.md`.

## Dirty CSV Cleanup

- **User goal:** inspect local malformed timestamp / dirty CSV fixture handling.
- **Command:**
  ```powershell
  python benchmarks\traditional_analytics\run.py --engines shardloom --formats csv --scenario "malformed timestamp / dirty CSV" --dataset-profile dirty_csv --rows 1000 --iterations 1 --output target\shardloom-dirty-csv-smoke.json --regenerate
  ```
- **Expected output:** local fixture evidence and deterministic no-fallback row.
- **Evidence fields:** `dataset_profile`, `scenario`, `materialization_boundary`,
  `claim_gate_status`.
- **Claim boundary:** fixture smoke, not production data-quality runtime.
- **References:** `benchmarks/traditional_analytics/README.md`.

## Nested JSON Scan

- **User goal:** inspect a local nested JSON field scan fixture.
- **Command:**
  ```powershell
  python benchmarks\traditional_analytics\run.py --engines shardloom --formats jsonl --scenario "nested JSON field scan" --dataset-profile nested_json --rows 1000 --iterations 1 --output target\shardloom-nested-json-smoke.json --regenerate
  ```
- **Expected output:** nested fixture evidence with no-fallback fields.
- **Evidence fields:** `scenario`, `dataset_profile`, `source_metadata_snapshot_status`,
  `claim_gate_status`.
- **Claim boundary:** fixture smoke, not broad JSON query runtime.
- **References:** `benchmarks/traditional_analytics/README.md`.

## CDC Overlay

- **User goal:** inspect deterministic local CDC overlay fixture coverage.
- **Command:**
  ```powershell
  python benchmarks\traditional_analytics\run.py --engines shardloom --formats csv --scenario "small change over large base" --dataset-profile cdc_delta_overlay --rows 1000 --iterations 1 --output target\shardloom-cdc-overlay-smoke.json --regenerate
  ```
- **Expected output:** local base-plus-delta fixture evidence.
- **Evidence fields:** `cdc_delta_overlay`, `source_state_reuse_hit`, `claim_gate_status`.
- **Claim boundary:** fixture smoke, not table transaction, lakehouse, or streaming CDC support.
- **References:** `docs/architecture/benchmark-suite-catalog.md`.

## Output Fanout

- **User goal:** understand local output and planned cross-format fanout.
- **Status:** result-sink smoke exists; cross-format fanout is planned.
- **Command:**
  ```powershell
  python examples\local-vortex-benchmark\run.py --repo-root . --rows 64 --iterations 1
  ```
- **Expected output:** local result-sink proof artifact.
- **Evidence fields:** `result_sink_write_millis`, `result_replay_verified`,
  `output_native_io_certificate_status`, `claim_gate_status`.
- **Claim boundary:** no S3/object-store write or table commit claim.
- **References:** `docs/architecture/io-reuse-and-fanout-architecture.md`.

## Object-Store Blocked Diagnostic

- **User goal:** ask whether S3/GCS/ADLS runtime I/O is available.
- **Command:**
  ```powershell
  target\debug\shardloom object-store-request-plan --format json
  ```
- **Expected output:** report-only or blocked object-store plan.
- **Evidence fields:** `credential_policy_status`, `network_probe_allowed=false`,
  `object_store_io=false`, `fallback_attempted=false`.
- **Claim boundary:** no object-store runtime or lakehouse claim.
- **References:** `docs/architecture/object-store-request-planner.md`.

## Foundry Dev-Stack Smoke

- **User goal:** inspect the local Foundry-style proof boundary.
- **Command:**
  ```powershell
  python scripts\foundry_proof_of_use.py --rows 64 --iterations 1
  ```
- **Expected output:** local proof report with Foundry/external compute fields set false.
- **Evidence fields:** `foundry_runtime_invoked=false`, `foundry_compute_invoked=false`,
  `foundry_spark_invoked=false`, `external_engine_invoked=false`.
- **Claim boundary:** local Foundry-style proof only; no Foundry production or package claim.
- **References:** `docs/foundry/proof-of-use-certification.md`.

## Benchmark Evidence Interpretation

- **User goal:** read benchmark output without overclaiming speed.
- **Command:**
  ```powershell
  python examples\local-vortex-benchmark\run.py --repo-root . --rows 64 --iterations 1
  ```
- **Expected output:** local timing and coverage rows.
- **Evidence fields:** `execution_mode`, `source_read_millis`, `vortex_prepare_millis`,
  `operator_compute_millis`, `claim_gate_status`.
- **Claim boundary:** benchmark evidence, not a leaderboard, performance claim, or superiority
  claim.
- **References:** `docs/benchmarks/local-taxonomy-benchmark.md`,
  `docs/benchmarks/baseline-comparison-boundary.md`.
