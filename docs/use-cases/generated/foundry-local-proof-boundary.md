<!-- SPDX-License-Identifier: Apache-2.0 -->

# Foundry-style local proof boundary

## Quick Answer

- **Audience:** Foundry-adjacent user who wants to see the local transform shape
- **Status:** `smoke_supported`
- **Execution mode:** `no_dataset_smoke`
- **Engine mode:** `batch_status`
- **Claim boundary:** Local Foundry-style proof only; no Foundry production, package publication, Marketplace, virtual table, direct object-store, Spark, or external compute claim.

## Can ShardLoom Do This?

Foundry-style local proof boundary has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## How To Try It

```powershell
python scripts\foundry_proof_of_use.py --rows 64 --iterations 1
```

## Internal Flow

`none, local_style_fixture -> no_dataset_smoke -> batch_status -> local_foundry_style_report, certificate_metrics_dataset_output -> evidence -> claim gate`

## Evidence You Should See

- `no_dataset_smoke_performed`
- `transform_import_proven`
- `cli_binary_resolved`
- `foundry_runtime_invoked=false`
- `foundry_compute_invoked=false`
- `foundry_spark_invoked=false`
- `fallback_attempted=false`
- `external_engine_invoked=false`
- `public_foundry_claim_allowed=false`

## Expected Output Or Evidence

A local proof report showing Foundry and external compute were not invoked.

## Common Mistakes

- `treating_local_style_as_real_foundry_runtime`
- `expecting_foundry_package`
- `writing_directly_to_s3`

## Reference Files

- `docs/foundry/proof-of-use-certification.md`
- `docs/foundry/integration-pack-readiness.md`
- `examples/foundry-lightweight-transform/README.md`
- `docs/architecture/compute-engine-flow-reference.md`

## Related Use Cases

- `source-free-generated-output-boundary`
- `package-channel-readiness-boundary`

## Related Field Guide Terms

- `website/field-guide/foundry-dev-stack-smoke.html` - Foundry Dev-Stack Smoke (`Platform Boundaries` / `local-style-proof`)
