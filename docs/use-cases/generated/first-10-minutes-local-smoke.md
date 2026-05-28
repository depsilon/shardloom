<!-- SPDX-License-Identifier: Apache-2.0 -->

# First 10 minutes local smoke

## Quick Answer

- **Audience:** new local user or reviewer
- **Status:** `ready_local`
- **Execution mode:** `no_dataset_smoke`
- **Engine mode:** `batch_status`
- **Claim boundary:** Local source-checkout smoke only; no dataset execution, package-publication, production, SQL/DataFrame, object-store, Foundry, performance, or Spark-replacement claim.

## Can ShardLoom Do This?

First 10 minutes local smoke has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

Local source-checkout smoke only; no dataset execution, package-publication, production, SQL/DataFrame, object-store, Foundry, performance, or Spark-replacement claim.

## How To Try It

```text
python examples\local-python-smoke\run.py --repo-root .
```

## Blocker

No current blocker is attached to this supported local smoke path beyond the claim boundary above.

## Internal Flow

`none -> no_dataset_smoke -> batch_status -> status_report, capabilities_report, smoke_report -> evidence -> claim gate`

## Evidence You Should See

- `fallback_attempted=false`
- `external_engine_invoked=false`
- `protocol_version`
- `resolved_cli_path`
- `claim_gate_status`

## Expected Output Or Evidence

Status, smoke, and capabilities JSON with fallback_attempted=false and external_engine_invoked=false.

## Common Mistakes

- `expecting_dataset_output`
- `treating_no_dataset_smoke_as_generated_output`
- `assuming_package_publication`

## Reference Files

- `README.md` - What this proves: Public technical-preview posture, Vortex-first positioning, and no-fallback boundaries.
- `docs/getting-started/first-10-minutes.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `docs/getting-started/examples.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `examples/local-python-smoke/README.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `python/README.md` - What this proves: Python wrapper scope, local smoke usage, and Python API claim boundaries.

## Related Use Cases

- `python-wrapper-client-smoke`
- `evidence-audit-claim-gates`

## Related Field Guide Terms

- `website/field-guide/what-is-shardloom.html` - What is ShardLoom? (`Start Here` / `runtime_supported`)
