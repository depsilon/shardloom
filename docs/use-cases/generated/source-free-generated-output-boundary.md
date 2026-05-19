<!-- SPDX-License-Identifier: Apache-2.0 -->

# Source-free generated output boundary

## Quick Answer

- **Audience:** user who wants range, sequence, values, calendar, or literal-table output without input data
- **Status:** `smoke_supported`
- **Execution mode:** `source_free_generated_output`
- **Engine mode:** `batch`
- **Claim boundary:** Scoped local user-row, generated-row projection/literal with_column, literal-table, calendar/date-dimension, range, sequence, SQL VALUES, SQL literal SELECT, and SQL generate_series/range JSONL fixture smokes through explicit helpers or ctx.sql(...).write(...) only; no broad SQL/DataFrame runtime, expression-backed DataFrame generation, arbitrary SQL function runtime, S3/object-store write, Foundry production, performance, production, or package-publication claim.

## Can ShardLoom Do This?

Source-free generated output boundary has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

Scoped local user-row, generated-row projection/literal with_column, literal-table, calendar/date-dimension, range, sequence, SQL VALUES, SQL literal SELECT, and SQL generate_series/range JSONL fixture smokes through explicit helpers or ctx.sql(...).write(...) only; no broad SQL/DataFrame runtime, expression-backed DataFrame generation, arbitrary SQL function runtime, S3/object-store write, Foundry production, performance, production, or package-publication claim.

## How To Try It

```powershell
$env:PYTHONPATH = "python\src"; python -c "from shardloom import context; ctx=context(repo_root='.'); a=ctx.from_rows([{'id': 1, 'label': 'alpha'}]).write('target/generated-reference.jsonl', allow_overwrite=True); t=ctx.from_rows([{'id': 1, 'label': 'alpha'}, {'id': 2, 'label': 'beta'}]).with_column('batch_id', 1).select('id', 'batch_id').write('target/generated-reference-transformed.jsonl', allow_overwrite=True); b=ctx.literal_table([{'code':'A','weight':1.5}]).write('target/generated-literal.jsonl', allow_overwrite=True); c=ctx.calendar('2026-05-18','2026-05-20', column='dt').write('target/generated-calendar.jsonl', allow_overwrite=True); d=ctx.range(0, 5, column='id').write('target/generated-range.jsonl', allow_overwrite=True); s=ctx.sequence(0, 5, column='id').write('target/generated-sequence.jsonl', allow_overwrite=True); e=ctx.sql_values("VALUES (1, 'alpha'), (2, 'beta')").write('target/generated-sql-values.jsonl', allow_overwrite=True); f=ctx.sql_literal_select("SELECT 1 AS id, 'alpha' AS label").write('target/generated-sql-select.jsonl', allow_overwrite=True); g=ctx.sql("SELECT 2 AS id, 'beta' AS label").write('target/generated-sql-from-context.jsonl', allow_overwrite=True); h=ctx.sql("SELECT * FROM generate_series(0, 4)").write('target/generated-sql-series.jsonl', allow_overwrite=True); print(a.claim_gate_status, t.generated_source_row_count, b.generated_source_kind, c.generated_source_row_count, d.generated_source_kind, s.generated_source_kind, e.generated_source_kind, f.generated_source_kind, g.generated_source_kind, h.generated_source_kind, h.generated_source_range_end_inclusive)"
```

## Blocker

The source-free API admission matrix and Python context unsupported helpers keep SQL source-free projection beyond the admitted literal SELECT/VALUES/generate_series/range smoke subset, broad DataFrame source-free projection, expression-backed generated with_column, engine-native values/synthetic profiles, object-store writes, and Foundry generated-output runtime blocked/report-only until separate evidence lands.

## Internal Flow

`none, generated_rows, generated_rows_projection, generated_rows_literal_with_column, literal_table_rows, calendar_dimension, range, sequence, sql_values, sql_literal_select, sql_generate_series_range, ctx_sql_source_free -> source_free_generated_output -> batch -> local_jsonl_output_artifact, generated_source_certificate, output_native_io_certificate -> evidence -> claim gate`

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
- `fallback_attempted=false`
- `external_engine_invoked=false`
- `claim_gate_status`

## Expected Output Or Evidence

A local JSONL output plus fields including generated_source_kind=user_rows, literal_table, calendar, range, sequence, sql_values, sql_literal_select, or sql_generate_series_range; generated_source_row_count for base and transformed generated rows; generated_source_range_* and generated_source_sql_generator_function for SQL generator rows; generated_source_certificate_status=present; output_native_io_certificate_status=certified_local_file_sink; fallback_attempted=false; and external_engine_invoked=false.

## Common Mistakes

- `confusing_no_dataset_smoke_with_generated_output`
- `claiming_source_native_io_without_source_read`
- `writing_to_s3`

## Reference Files

- `docs/architecture/compute-engine-flow-reference.md` - What this proves: Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.
- `docs/foundry/proof-of-use-certification.md` - What this proves: Foundry-style local proof boundary and no-production-Foundry claim posture.
- `python/README.md` - What this proves: Python wrapper posture, local smoke usage, and Python API claim boundaries.
- `docs/architecture/phased-execution-plan.md` - What this proves: Active planned work, claim boundaries, non-goals, and ledger move rules.

## Related Use Cases

- `first-10-minutes-local-smoke`
- `foundry-local-proof-boundary`
- `output-result-sink-and-fanout-boundary`

## Related Field Guide Terms

- `website/field-guide/fixture-smoke.html` - Fixture Smoke (`Engine Modes` / `scoped-evidence`)
- `website/field-guide/python-wrapper-client.html` - Python Wrapper Client (`User Workflows` / `current-local-client`)
- `website/field-guide/source-free-generated-output.html` - Source-Free Generated Output (`User Workflows` / `scoped-local-smoke`)
- `website/field-guide/output-fanout.html` - Output Fanout (`User Workflows` / `report-only-to-planned`)
- `website/field-guide/output-plan-reuse.html` - OutputPlan Reuse (`I/O And Output` / `planned-contract`)
- `website/field-guide/foundry-dev-stack-smoke.html` - Foundry Dev-Stack Smoke (`Platform Boundaries` / `local-style-proof`)
