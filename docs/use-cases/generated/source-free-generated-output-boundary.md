<!-- SPDX-License-Identifier: Apache-2.0 -->

# Source-free generated output boundary

## Quick Answer

- **Audience:** user who wants range, sequence, values, calendar, or literal-table output without input data
- **Status:** `smoke_supported`
- **Execution mode:** `source_free_generated_output`
- **Engine mode:** `batch`
- **Claim boundary:** Scoped local user-row, generated-row projection/literal with_column, literal-table, calendar/date-dimension, range/sequence limit/head/take bound adjustment, SQL VALUES, SQL literal SELECT, SQL generate_series/range, scoped SQL value-column/int64 arithmetic range projection, scoped DataFrame literal projection, and scoped generated DataFrame with_column JSONL/CSV fixture smokes through explicit helpers or ctx.sql(...).write(...) only; feature-gated flat scalar Parquet/Arrow IPC/Avro/ORC local sinks are admitted when shardloom-cli is built with --features universal-format-io, and feature-gated flat scalar local Vortex output is admitted when built with --features vortex-write. No broad Vortex writer claim, broad SQL/DataFrame runtime, expression-backed DataFrame generation, arbitrary SQL function runtime, S3/object-store write, Foundry production, performance, production, or package-publication claim.

## Can ShardLoom Do This?

Source-free generated output boundary has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

Scoped local user-row, generated-row projection/literal with_column, literal-table, calendar/date-dimension, range/sequence limit/head/take bound adjustment, SQL VALUES, SQL literal SELECT, SQL generate_series/range, scoped SQL value-column/int64 arithmetic range projection, scoped DataFrame literal projection, and scoped generated DataFrame with_column JSONL/CSV fixture smokes through explicit helpers or ctx.sql(...).write(...) only; feature-gated flat scalar Parquet/Arrow IPC/Avro/ORC local sinks are admitted when shardloom-cli is built with --features universal-format-io, and feature-gated flat scalar local Vortex output is admitted when built with --features vortex-write. No broad Vortex writer claim, broad SQL/DataFrame runtime, expression-backed DataFrame generation, arbitrary SQL function runtime, S3/object-store write, Foundry production, performance, production, or package-publication claim.

## How To Try It

```powershell
$env:PYTHONPATH = "python\src"; python -c "from shardloom import context; ctx=context(repo_root='.'); a=ctx.from_rows([{'id': 1, 'label': 'alpha'}]).write('target/generated-reference.jsonl', allow_overwrite=True); t=ctx.from_rows([{'id': 1, 'label': 'alpha'}, {'id': 2, 'label': 'beta'}]).with_column('batch_id', 1).select('id', 'batch_id').write('target/generated-reference-transformed.jsonl', allow_overwrite=True); b=ctx.literal_table([{'code':'A','weight':1.5}]).write('target/generated-literal.jsonl', allow_overwrite=True); c=ctx.calendar('2026-05-18','2026-05-20', column='dt').write('target/generated-calendar.jsonl', allow_overwrite=True); d=ctx.range(0, 50, column='id').limit(5).write('target/generated-range.jsonl', allow_overwrite=True); s=ctx.sequence(0, 50, column='id').take(5).write('target/generated-sequence.jsonl', allow_overwrite=True); e=ctx.sql_values("VALUES (1, 'alpha'), (2, 'beta')").write('target/generated-sql-values.jsonl', allow_overwrite=True); f=ctx.sql_literal_select("SELECT 1 AS id, 'alpha' AS label").write('target/generated-sql-select.jsonl', allow_overwrite=True); g=ctx.sql("SELECT 2 AS id, 'beta' AS label").write('target/generated-sql-from-context.jsonl', allow_overwrite=True); h=ctx.sql("SELECT * FROM generate_series(0, 4)").write('target/generated-sql-series.jsonl', allow_overwrite=True); p=ctx.sql("SELECT value AS id, value + 1 AS next FROM range(0, 4)").write('target/generated-sql-range-projection.jsonl', allow_overwrite=True); q=ctx.dataframe_source_free_projection("lit(1).alias('value')").write('target/generated-dataframe-projection.jsonl', allow_overwrite=True); r=ctx.dataframe_generated_with_column('value', 'lit(1)').write('target/generated-dataframe-column.jsonl', allow_overwrite=True); print(a.claim_gate_status, t.generated_source_row_count, b.generated_source_kind, c.generated_source_row_count, d.generated_source_kind, d.generated_source_row_count, s.generated_source_kind, s.generated_source_row_count, e.generated_source_kind, f.generated_source_kind, g.generated_source_kind, h.generated_source_kind, h.generated_source_range_end_inclusive, p.sql_source_free_projection_columns, q.generated_source_kind, r.generated_source_kind)"
```

## Blocker

Default binaries support JSONL/CSV generated-output smokes and return deterministic blockers for Parquet, Arrow IPC, Avro, and ORC until built with --features universal-format-io and for Vortex until built with --features vortex-write. The source-free API admission matrix and deterministic unsupported helpers keep SQL source-free projection beyond the admitted literal SELECT/VALUES/generate_series/range and scoped range-projection subset, broad expression-backed DataFrame generation, engine-native values/synthetic profiles, broad Vortex writer behavior, object-store writes, and Foundry generated-output runtime blocked/report-only until separate evidence lands.

## Internal Flow

`none, generated_rows, generated_rows_projection, generated_rows_literal_with_column, literal_table_rows, calendar_dimension, range, sequence, sql_values, sql_literal_select, sql_generate_series_range, sql_generate_series_range_projection, dataframe_literal_projection, dataframe_generated_with_column, ctx_sql_source_free -> source_free_generated_output -> batch -> local_jsonl_output_artifact, local_csv_output_artifact, feature_gated_local_parquet_output, feature_gated_local_arrow_ipc_output, feature_gated_local_avro_output, feature_gated_local_orc_output, feature_gated_local_vortex_output, generated_source_certificate, output_native_io_certificate -> evidence -> claim gate`

## Evidence You Should See

- `input_dataset_count=0`
- `source_io_performed=false`
- `generated_source_created=true`
- `generated_source_kind`
- `generated_source_schema_digest`
- `generated_source_row_count`
- `output_io_performed`
- `generated_source_certificate_status`
- `output_native_io_certificate_status`
- `generated_source_api_admission_schema_version`
- `support_status`
- `blocker_id`
- `vortex_output_runtime_execution`
- `vortex_output_reopen_verified`
- `vortex_artifact_digest`
- `upstream_vortex_write_called`
- `upstream_vortex_scan_called`
- `fallback_attempted=false`
- `external_engine_invoked=false`
- `claim_gate_status`

## Expected Output Or Evidence

A local JSONL/CSV output, feature-gated flat scalar Parquet/Arrow IPC/Avro/ORC output, or feature-gated local .vortex output. Evidence includes generated_source_kind, generated_source_row_count, generated_source_range_* and generated_source_sql_generator_function for SQL generator rows, sql_source_free_projection_* fields for scoped range projections, generated_source_certificate_status=present, output_native_io_certificate_status, sink_artifact_* fields, Vortex artifact/reopen fields when write_vortex is admitted, fallback_attempted=false, and external_engine_invoked=false.

## Common Mistakes

- `confusing_no_dataset_smoke_with_generated_output`
- `expecting_structured_outputs_in_default_build`
- `claiming_source_native_io_without_source_read`
- `writing_to_s3`

## Reference Files

- `docs/architecture/compute-engine-flow-reference.md` - What this proves: Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.
- `docs/foundry/proof-of-use-certification.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `python/README.md` - What this proves: Python wrapper scope, local smoke usage, and Python API claim boundaries.
- `docs/architecture/phased-execution-completed-ledger.md` - What this proves: Completed runtime provenance and historical phase evidence for this use case.

## Related Use Cases

- `first-10-minutes-local-smoke`
- `foundry-local-proof-boundary`
- `output-result-sink-and-fanout-boundary`

## Related Field Guide Terms

- `website/field-guide/generated-source-route.html` - Generated source route (`Execution Routes` / `smoke_supported`)
- `website/field-guide/output-plan.html` - OutputPlan (`I/O + Outputs` / `smoke_supported`)
- `website/field-guide/sink-artifact.html` - SinkArtifact (`I/O + Outputs` / `smoke_supported`)
- `website/field-guide/output-fanout.html` - Output fanout (`I/O + Outputs` / `planned`)
- `website/field-guide/foundry-boundary.html` - Foundry boundary (`Platform Boundaries` / `smoke_supported`)
