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

## How To Try It

```powershell
python examples\local-python-smoke\run.py --repo-root .
```

## Internal Flow

`none -> no_dataset_smoke -> batch_status -> status_report, capabilities_report, smoke_report -> evidence -> claim gate`

## Evidence You Should See

- `fallback_attempted=false`
- `external_engine_invoked=false`
- `protocol_version`
- `resolved_cli_path`

## Expected Output Or Evidence

Status, smoke, and capabilities JSON with fallback_attempted=false and external_engine_invoked=false.

## Common Mistakes

- `expecting_dataset_output`
- `treating_no_dataset_smoke_as_generated_output`
- `assuming_package_publication`

## Reference Files

- `README.md` - What this proves: Public technical-preview posture, Vortex-first/no-fallback positioning, and primary repo entrypoints.
- `docs/getting-started/first-10-minutes.md` - What this proves: Shortest local orientation path for smoke checks and evidence inspection.
- `docs/getting-started/examples.md` - What this proves: Current example catalog and local workflow entrypoints.
- `examples/local-python-smoke/README.md` - What this proves: Runnable or blocked example posture, expected local command path, and claim boundary.
- `python/README.md` - What this proves: Python wrapper posture, local smoke usage, and Python API claim boundaries.

## Related Use Cases

- `python-wrapper-client-smoke`
- `evidence-audit-claim-gates`

## Related Field Guide Terms

- `website/field-guide/what-is-shardloom.html` - What Is ShardLoom? (`Start Here` / `technical-preview`)
- `website/field-guide/batch-engine.html` - Batch Engine (`Engine Modes` / `scoped-local`)
- `website/field-guide/fixture-smoke.html` - Fixture Smoke (`Engine Modes` / `scoped-evidence`)
- `website/field-guide/execution-certificate.html` - Execution Certificate (`Evidence And Claims` / `current-evidence`)
- `website/field-guide/first-ten-minutes.html` - First 10 Minutes (`User Workflows` / `ready-local-docs`)
- `website/field-guide/source-free-generated-output.html` - Source-Free Generated Output (`User Workflows` / `scoped-local-smoke`)
- `website/field-guide/package-channel-boundary.html` - Package Channel Boundary (`Platform Boundaries` / `blocked-until-proof`)
- `website/field-guide/package-publication-boundary.html` - Package Publication Boundary (`Release And Trust` / `blocked-until-proof`)
