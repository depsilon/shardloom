<!-- SPDX-License-Identifier: Apache-2.0 -->

# Foundry Dev-Stack Starter Kit

Status: local Foundry-style starter for `GAR-COMMERCIAL-1E` and
`GAR-RUNTIME-IMPL-5P`. This is not a real Foundry runtime proof, Foundry package proof, Foundry
Marketplace proof, or Foundry production support claim.

The machine-readable source of truth is
[`docs/foundry/dev-stack-starter-kit.json`](dev-stack-starter-kit.json) with schema
`shardloom.foundry_dev_stack_starter_kit.v1`. Validate it with:

```powershell
python scripts\check_foundry_dev_stack_starter.py
```

## What This Starter Does

The starter gives a local developer a single path to inspect the shape of a future Foundry code
repository transform without invoking Foundry services:

1. Build the local ShardLoom CLI.
2. Import the local Python package/client from the checkout.
3. Resolve the ShardLoom CLI.
4. Run no-dataset smoke and capability checks.
5. Execute a scoped source-free generated-output workflow through ShardLoom.
6. Execute a scoped staged local CSV transform through ShardLoom.
7. Write local result and evidence dataset-shaped artifacts through the dev-stack Foundry-style
   output API.
8. Run the existing local proof script that also runs a tiny local Vortex smoke.

## Commands

Run from a source checkout:

```powershell
cargo build -p shardloom-cli --bin shardloom
python examples\foundry-lightweight-transform\run.py --repo-root .
python scripts\foundry_proof_of_use.py --rows 64 --iterations 1
```

Expected local outputs:

```text
target/foundry-lightweight-transform/certificate-output.json
target/foundry-lightweight-transform/generated-output.jsonl
target/foundry-lightweight-transform/generated-output.csv
target/foundry-lightweight-transform/staged-transform-output.jsonl
target/foundry-lightweight-transform/result-dataset
target/foundry-lightweight-transform/evidence-dataset
target/foundry-proof-of-use/report.json
target/foundry-proof-of-use/generated-output.jsonl
target/foundry-proof-of-use/generated-output.csv
target/foundry-proof-of-use/staged-transform-output.jsonl
target/foundry-proof-of-use/result-dataset
target/foundry-proof-of-use/evidence-dataset
target/foundry-proof-of-use/local-vortex-benchmark-smoke.json
```

These are local files/directories only. The `result-dataset` and `evidence-dataset` directories use
a local Foundry-style output API shape; they are not real Foundry output datasets.

## Source-Free Generated-Output Posture

No-dataset smoke remains separate from generated-output execution:

```text
no_dataset_smoke_separate_from_generated_output=true
generated_output_execution_performed=true
generated_source_created=true
generated_source_certificate_status=present
output_native_io_certificate_status=certified_local_file_sink
foundry_output_api_required=true
foundry_style_output_api_invoked=true
claim_gate_status=fixture_smoke_only
```

The starter now creates rows through ShardLoom's scoped generated-source local-output route and
writes those rows into local Foundry-style result/evidence dataset artifacts. Real Foundry generated
output still remains blocked until a real Foundry transform writes result and evidence datasets
through Foundry output APIs, not direct S3/object-store paths.

## Staged Input Example

The starter declares:

```text
examples/foundry-lightweight-transform/fixtures/staged_input.csv
```

That staged input is a local fixture for the scoped staged-transform proof. It is not a Foundry
input dataset, not a production dataset, and not an object-store read.

```text
staged_input_transform_execution_performed=true
```

## Evidence Dataset Boundary

The local script writes local certificate-style JSON. A real Foundry proof would need a Foundry
evidence dataset output. Current posture:

```text
local_certificate_json_written=true
foundry_evidence_dataset_written=false
foundry_result_dataset_written=false
foundry_style_evidence_dataset_written=true
foundry_style_result_dataset_written=true
deterministic_blocker=blocked_until_real_foundry_output_api_evidence
output_evidence_dataset_written=true
```

## Required Safety Fields

The starter and proof reports must preserve:

```text
foundry_runtime_invoked=false
foundry_compute_invoked=false
foundry_spark_invoked=false
foundry_output_api_invoked=false
foundry_result_dataset_written=false
foundry_evidence_dataset_written=false
foundry_style_output_api_invoked=true
foundry_style_result_dataset_written=true
foundry_style_evidence_dataset_written=true
direct_s3_read_invoked=false
direct_s3_write_invoked=false
object_store_read_invoked=false
object_store_write_invoked=false
object_store_commit_invoked=false
credential_resolution_performed=false
network_probe_performed=false
external_compute_invoked=false
external_engine_invoked=false
fallback_attempted=false
public_foundry_claim_allowed=false
foundry_marketplace_claim_allowed=false
```

## What This Proves

- Local Python import/client wiring can resolve ShardLoom from a source checkout.
- The local CLI can be resolved.
- A Foundry-style transform shape can execute source-free generated output through ShardLoom.
- A staged local CSV fixture can execute through ShardLoom and write a local output.
- The dev-stack output API can write local result/evidence dataset-shaped artifacts.
- The proof can run a tiny local Vortex smoke through existing ShardLoom paths.
- Foundry support remains explicit and claim-gated.

## What This Does Not Prove

- No real Foundry runtime invocation.
- No Foundry compute invocation.
- No Foundry Spark invocation.
- No real Foundry output API write.
- No real Foundry evidence dataset output.
- No Foundry package or Marketplace availability.
- No direct S3/object-store runtime.
- No production SQL/DataFrame, object-store/lakehouse, or Foundry claim.
- No Spark-displacement or performance claim.
- No external engine fallback.

## Claim Boundary

The only allowed claim is local-style evaluation:

```text
local_foundry_style_generated_output_and_staged_transform_smoke_only
```

This starter reduces evaluation friction. It does not certify ShardLoom for Foundry production use.
