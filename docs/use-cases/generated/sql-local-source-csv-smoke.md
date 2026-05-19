<!-- SPDX-License-Identifier: Apache-2.0 -->

# SQL local CSV projection/filter/limit smoke

## Quick Answer

- **Audience:** user who wants to try one tiny SQL query over a local CSV without fallback
- **Status:** `smoke_supported`
- **Execution mode:** `direct_compatibility_transient`
- **Engine mode:** `batch`
- **Claim boundary:** One scoped local CSV SELECT projection/filter/limit smoke with optional local JSONL output only; no broad SQL/DataFrame runtime, production SQL support, object-store/table source, external fallback, or performance claim.

## Can ShardLoom Do This?

SQL local CSV projection/filter/limit smoke has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

One scoped local CSV SELECT projection/filter/limit smoke with optional local JSONL output only; no broad SQL/DataFrame runtime, production SQL support, object-store/table source, external fallback, or performance claim.

## How To Try It

```powershell
New-Item -ItemType Directory -Force target | Out-Null; "id,label,amount`n1,alpha,8`n2,beta,15`n3,gamma,`n" | Set-Content -Encoding utf8 target\sql-local-source-smoke.csv; cargo run -q -p shardloom-cli -- sql-local-source-smoke "SELECT id,label FROM 'target/sql-local-source-smoke.csv' WHERE amount >= 10 LIMIT 1" --output target\sql-local-source-result.jsonl --allow-overwrite --format json
```

## Blocker

Parquet/Vortex SQL sources, joins, aggregates, functions, subqueries, catalogs, object stores, table/lakehouse sources, output sinks, and production SQL/DataFrame support require later runtime slices.

## Internal Flow

`local_csv -> direct_compatibility_transient -> batch -> inline_jsonl_result, optional_local_jsonl_output, sql_local_source_evidence -> evidence -> claim gate`

## Evidence You Should See

- `schema_version=shardloom.sql_local_source_smoke.v1`
- `sql_parser_executed=true`
- `sql_binder_executed=true`
- `sql_planner_executed=true`
- `source_io_performed=true`
- `source_format=csv`
- `output_io_performed`
- `output_native_io_certificate_status`
- `materialization_boundary`
- `fallback_attempted=false`
- `external_engine_invoked=false`
- `claim_gate_status=fixture_smoke_only`

## Expected Output Or Evidence

A JSON envelope with inline JSONL result, optional local JSONL output path/digest/certificate fields, parser/binder/planner/runtime flags, local CSV source evidence, materialization/decode evidence, fallback_attempted=false, external_engine_invoked=false, and claim_gate_status=fixture_smoke_only.

## Common Mistakes

- `treating_smoke_as_sql_compatibility`
- `expecting_parquet_or_s3_sql_sources`
- `expecting_join_or_aggregate_support`

## Reference Files

- `README.md` - What this proves: Public technical-preview posture, Vortex-first/no-fallback positioning, and primary repo entrypoints.
- `docs/getting-started/examples.md` - What this proves: Current example catalog and local workflow entrypoints.
- `docs/architecture/compute-engine-flow-reference.md` - What this proves: Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.
- `docs/architecture/phased-execution-plan.md` - What this proves: Active planned work, claim boundaries, non-goals, and ledger move rules.

## Related Use Cases

- `python-local-csv-query-builder-smoke`
- `sql-dataframe-capability-posture`
- `source-free-generated-output-boundary`
- `local-file-etl-cleanup-smoke`

## Related Field Guide Terms

- `website/field-guide/direct-compatibility-transient.html` - Direct Compatibility Transient (`Execution Modes` / `scoped-local-smoke`)
