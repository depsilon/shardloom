<!-- SPDX-License-Identifier: Apache-2.0 -->

# Foundry-style local proof boundary

## Quick Answer

- **Audience:** Foundry-adjacent user who wants to see the local transform shape
- **Status:** `smoke_supported`
- **Execution mode:** `local_foundry_style_generated_and_staged_transform_smoke`
- **Engine mode:** `batch_status`
- **Claim boundary:** Local Foundry-style generated-output and staged-transform proof only; no real Foundry runtime/output API, production, package publication, Marketplace, virtual table, direct object-store, Spark, or external compute claim.

## Can ShardLoom Do This?

Foundry-style local proof boundary has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

Local Foundry-style generated-output and staged-transform proof only; no real Foundry runtime/output API, production, package publication, Marketplace, virtual table, direct object-store, Spark, or external compute claim.

## How To Try It

```text
python scripts\foundry_proof_of_use.py --rows 64 --iterations 1
```

## Blocker

No current blocker is attached to this supported local smoke path beyond the claim boundary above.

## Internal Flow

`none, local_style_fixture, staged_local_csv_fixture -> local_foundry_style_generated_and_staged_transform_smoke -> batch_status -> local_foundry_style_report, generated_output_artifact, staged_transform_output, local_foundry_style_result_dataset, local_foundry_style_evidence_dataset, certificate_metrics_dataset_output -> evidence -> claim gate`

## Evidence You Should See

- `no_dataset_smoke_performed`
- `transform_import_proven`
- `cli_binary_resolved`
- `generated_output_execution_performed`
- `generated_source_created`
- `generated_source_certificate_status`
- `staged_input_transform_execution_performed`
- `foundry_style_output_api_invoked=true`
- `foundry_style_result_dataset_written=true`
- `foundry_style_evidence_dataset_written=true`
- `output_evidence_dataset_written=true`
- `foundry_output_api_invoked=false`
- `foundry_runtime_invoked=false`
- `foundry_compute_invoked=false`
- `foundry_spark_invoked=false`
- `fallback_attempted=false`
- `external_engine_invoked=false`
- `public_foundry_claim_allowed=false`
- `claim_gate_status`

## Expected Output Or Evidence

A local proof report plus local result/evidence dataset-shaped artifacts showing ShardLoom generated/staged execution and no Foundry or external compute invocation.

## Common Mistakes

- `treating_local_style_as_real_foundry_runtime`
- `expecting_foundry_package`
- `writing_directly_to_s3`

## Reference Files

- `docs/foundry/proof-of-use-certification.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `docs/foundry/integration-pack-readiness.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `examples/foundry-lightweight-transform/README.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `docs/architecture/compute-engine-flow-reference.md` - What this proves: Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.

## Related Use Cases

- `source-free-generated-output-boundary`
- `package-channel-readiness-boundary`

## Related Field Guide Terms

- [Generated source route](https://shardloom.io/field-guide/generated-source-route) (`Execution Routes` / `smoke_supported`)
- [Foundry boundary](https://shardloom.io/field-guide/foundry-boundary) (`Platform Boundaries` / `smoke_supported`)
