<!-- SPDX-License-Identifier: Apache-2.0 -->

# Source-free generated output boundary

## Quick Answer

- **Audience:** user who wants range, values, calendar, or literal-table output without input data
- **Status:** `smoke_supported`
- **Execution mode:** `source_free_generated_output`
- **Engine mode:** `batch`
- **Claim boundary:** Scoped local user-row JSONL and range JSONL fixture smokes only; no sequence/values/calendar/literal-table runtime, SQL/DataFrame runtime, S3/object-store write, Foundry production, performance, production, or package-publication claim.

## Can ShardLoom Do This?

Source-free generated output boundary has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## How To Try It

```powershell
$env:PYTHONPATH = "python\src"; python -c "from shardloom import context; ctx=context(repo_root='.'); a=ctx.from_rows([{'id': 1, 'label': 'alpha'}]).write('target/generated-reference.jsonl', allow_overwrite=True); b=ctx.range(0, 5, column='id').write('target/generated-range.jsonl', allow_overwrite=True); print(a.claim_gate_status, b.generated_source_kind, b.generated_source_row_count)"
```

## Blocker

The source-free API admission matrix now exposes report-only rows for ctx.literal_table, ctx.calendar, SQL literal SELECT, SQL VALUES, SQL source-free projection, SQL generate_series/range vocabulary, DataFrame source-free projection, and generated with_column. Sequence, VALUES/literal-table, calendar/date dimension, synthetic profile, SQL execution, broad DataFrame execution, object-store writes, and Foundry generated-output runtime remain blocked/report-only until separate evidence lands.

## Internal Flow

`none, generated_rows, range, values, calendar_dimension -> source_free_generated_output -> batch -> local_jsonl_output_artifact, generated_source_certificate, output_native_io_certificate -> evidence -> claim gate`

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

A local JSONL output plus fields including generated_source_kind=user_rows or range, generated_source_certificate_status=present, output_native_io_certificate_status=certified_local_file_sink, fallback_attempted=false, and external_engine_invoked=false.

## Common Mistakes

- `confusing_no_dataset_smoke_with_generated_output`
- `claiming_source_native_io_without_source_read`
- `writing_to_s3`

## Reference Files

- `docs/architecture/compute-engine-flow-reference.md`
- `docs/foundry/proof-of-use-certification.md`
- `python/README.md`
- `docs/architecture/phased-execution-plan.md`

## Related Use Cases

- `first-10-minutes-local-smoke`
- `foundry-local-proof-boundary`
- `output-result-sink-and-fanout-boundary`
