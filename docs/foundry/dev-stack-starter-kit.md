<!-- SPDX-License-Identifier: Apache-2.0 -->

# Foundry Dev-Stack Starter Kit

Status: local Foundry-style starter for `GAR-COMMERCIAL-1E`. This is not a real Foundry runtime
proof, Foundry package proof, Foundry Marketplace proof, or Foundry production support claim.

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
5. Declare a staged local input fixture.
6. Write local certificate-style evidence.
7. Run the existing local proof script that also runs a tiny local Vortex smoke.

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
target/foundry-proof-of-use/report.json
target/foundry-proof-of-use/local-vortex-benchmark-smoke.json
```

These are local files only. They are not Foundry output datasets.

## Source-Free Generated-Output Posture

No-dataset smoke remains separate from generated-output execution:

```text
no_dataset_smoke_separate_from_generated_output=true
generated_output_execution_performed=false
generated_source_created=false
generated_source_certificate_status=not_emitted_report_only
output_native_io_certificate_status=not_emitted_report_only
foundry_output_api_required=true
claim_gate_status=not_claim_grade
```

The current starter does not create rows with a Foundry transform and does not write a Foundry
result dataset. Future generated-output proof must write result and evidence datasets through
Foundry output APIs, not direct S3/object-store paths.

## Staged Input Example

The starter declares:

```text
examples/foundry-lightweight-transform/fixtures/staged_input.csv
```

That staged input is a local fixture for path and boundary evidence. It is not a Foundry input
dataset, not a production dataset, and not an object-store read.

## Evidence Dataset Boundary

The local script writes local certificate-style JSON. A real Foundry proof would need a Foundry
evidence dataset output. Current posture:

```text
local_certificate_json_written=true
foundry_evidence_dataset_written=false
foundry_result_dataset_written=false
deterministic_blocker=blocked_until_real_foundry_output_api_evidence
output_evidence_dataset_written=false
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
- A Foundry-style transform shape can emit local certificate-style evidence.
- The proof can run a tiny local Vortex smoke through existing ShardLoom paths.
- Foundry support remains explicit and claim-gated.

## What This Does Not Prove

- No real Foundry runtime invocation.
- No Foundry compute invocation.
- No Foundry Spark invocation.
- No Foundry output API write.
- No Foundry evidence dataset output.
- No Foundry package or Marketplace availability.
- No direct S3/object-store runtime.
- No production SQL/DataFrame, object-store/lakehouse, or Foundry claim.
- No Spark-displacement or performance claim.
- No external engine fallback.

## Claim Boundary

The only allowed claim is local-style evaluation:

```text
local_foundry_style_transform_and_local_vortex_execution_smoke_only
```

This starter reduces evaluation friction. It does not certify ShardLoom for Foundry production use.
