<!-- SPDX-License-Identifier: Apache-2.0 -->

# Source-free generated output boundary

## Quick Answer

- **Audience:** user who wants range, values, calendar, or literal-table output without input data
- **Status:** `planned`
- **Execution mode:** `planned_generated_source`
- **Engine mode:** `batch`
- **Claim boundary:** Planned generated-output contract only; no current source-free runtime, SQL/DataFrame runtime, S3/object-store write, Foundry production, or package-publication claim.

## Can ShardLoom Do This?

Source-free generated output boundary is planned. The blocker and evidence requirements are part of the current public posture.

## Blocker

No-input smoke exists, but generated-output execution needs a GeneratedSourceCertificate, deterministic generation evidence, and output sink proof before it can be supported.

## Internal Flow

`none, generated_rows, range, values, calendar_dimension -> planned_generated_source -> batch -> planned_local_output_artifact, generated_source_certificate -> evidence -> claim gate`

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
- `fallback_attempted=false`
- `external_engine_invoked=false`
- `claim_gate_status`

## Expected Output Or Evidence

Future rows should distinguish no_dataset_smoke from user_generated_source and engine_native_generated_source.

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
