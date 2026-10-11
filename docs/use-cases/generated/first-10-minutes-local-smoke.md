<!-- SPDX-License-Identifier: Apache-2.0 -->

# First 10 minutes local smoke

## Quick Answer

- **Audience:** new local user or reviewer
- **Status:** `ready_local`
- **Execution mode:** `local_csv_and_generated_output_smoke`
- **Engine mode:** `batch`
- **Claim boundary:** Local source-checkout smoke executes a bounded CSV query and writes generated JSONL output using illustrative caller-declared limits of 16 GiB and 8 lanes; these values are not defaults, recommendations, or measurements. No production, broad SQL/DataFrame, object-store, Foundry, performance, or Spark-replacement claim.

## Can ShardLoom Do This?

First 10 minutes local smoke has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

Local source-checkout smoke executes a bounded CSV query and writes generated JSONL output using illustrative caller-declared limits of 16 GiB and 8 lanes; these values are not defaults, recommendations, or measurements. No production, broad SQL/DataFrame, object-store, Foundry, performance, or Spark-replacement claim.

## How To Try It

```text
python examples\local-python-smoke\run.py --repo-root . --memory-gb 16 --max-parallelism 8
```

## Blocker

No current blocker is attached to this supported local smoke path beyond the claim boundary above.

## Internal Flow

`local_csv_fixture -> local_csv_and_generated_output_smoke -> batch -> status_report, capabilities_report, smoke_report, bounded_csv_result, generated_jsonl_output -> evidence -> claim gate`

## Evidence You Should See

- `fallback_attempted=false`
- `external_engine_invoked=false`
- `protocol_version`
- `resolved_cli_path`
- `claim_gate_status`

## Expected Output Or Evidence

Status, smoke, and capabilities JSON, bounded CSV result, generated JSONL output, and fallback_attempted=false / external_engine_invoked=false.

## Common Mistakes

- `treating_local_smoke_as_no_dataset_only`
- `assuming_package_publication`

## Reference Files

- `README.md` - What this proves: Published local engine, operational maturity, Vortex-first positioning, and no-fallback boundaries.
- `docs/getting-started/first-10-minutes.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `docs/getting-started/examples.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `examples/local-python-smoke/README.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `python/README.md` - What this proves: Python wrapper scope, local smoke usage, and Python API claim boundaries.

## Related Use Cases

- `python-wrapper-client-smoke`
- `evidence-audit-claim-gates`

## Related Field Guide Terms

- [What is ShardLoom?](https://shardloom.io/field-guide/what-is-shardloom) (`Start Here` / `runtime_supported`)
