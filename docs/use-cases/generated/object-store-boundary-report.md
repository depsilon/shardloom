<!-- SPDX-License-Identifier: Apache-2.0 -->

# Object-store and cloud storage boundary

## Quick Answer

- **Audience:** user asking whether S3, GCS, or ADLS runtime I/O works
- **Status:** `blocked`
- **Execution mode:** `report_only_blocked`
- **Engine mode:** `none`
- **Claim boundary:** Object-store read/write runtime is blocked/report-only; no S3/GCS/ADLS, lakehouse, distributed, credential, network, or production claim.

## Can ShardLoom Do This?

Object-store and cloud storage boundary is blocked or unsupported until the listed evidence exists.

## How To Try It

```powershell
target\debug\shardloom object-store-request-plan --format json
```

## Blocker

Runtime object-store I/O needs provider, credential, byte-range, retry, idempotency, commit, certificate, and no-fallback evidence before support can be claimed.

## Internal Flow

`s3_uri, gcs_uri, adls_uri -> report_only_blocked -> none -> object_store_plan, deterministic_blocker -> evidence -> claim gate`

## Evidence You Should See

- `credential_policy_status`
- `network_probe_allowed=false`
- `byte_range_read_allowed`
- `object_store_io=false`
- `write_io=false`
- `native_io_certificate_status=blocked`
- `fallback_attempted=false`
- `external_engine_invoked=false`

## Expected Output Or Evidence

A blocked or report-only object-store plan with no provider probe and no external engine invocation.

## Common Mistakes

- `expecting_public_s3_read`
- `assuming_signed_url_support`
- `treating_planner_as_runtime_io`

## Reference Files

- `docs/architecture/compute-engine-flow-reference.md`
- `docs/architecture/universal-input-contract.md`
- `docs/architecture/object-store-request-planner.md`
- `docs/architecture/universal-compatibility-coverage-scoreboard.md`

## Related Use Cases

- `table-lakehouse-boundary-report`
- `output-result-sink-and-fanout-boundary`
