<!-- SPDX-License-Identifier: Apache-2.0 -->

# Object-store and cloud storage boundary

## Quick Answer

- **Audience:** user asking whether S3, GCS, or ADLS runtime I/O works
- **Status:** `blocked`
- **Execution mode:** `report_only_blocked`
- **Engine mode:** `none`
- **Claim boundary:** Cloud object-store read/write runtime is blocked/report-only; local-emulator smokes do not create S3/GCS/ADLS, lakehouse, distributed, credential, network, or production claims.

## Can ShardLoom Do This?

Object-store and cloud storage boundary is blocked or unsupported until the listed evidence exists.

## Claim Boundary

Cloud object-store read/write runtime is blocked/report-only; local-emulator smokes do not create S3/GCS/ADLS, lakehouse, distributed, credential, network, or production claims.

## How To Try It

```powershell
target\debug\shardloom object-store-request-plan --format json
```

## Blocker

Cloud object-store I/O needs provider, credential, byte-range, retry, idempotency, commit, certificate, and no-fallback evidence before support can be claimed beyond local-emulator smokes.

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

- `docs/architecture/compute-engine-flow-reference.md` - What this proves: Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.
- `docs/architecture/universal-input-contract.md` - What this proves: Universal input contract posture and unsupported input-family diagnostics.
- `docs/architecture/object-store-request-planner.md` - What this proves: Object-store request planning posture and blocked/runtime admission boundaries.
- `docs/architecture/universal-compatibility-coverage-scoreboard.md` - What this proves: Compatibility scoreboard status and source/sink support boundaries.

## Related Use Cases

- `object-store-local-emulator-read-smoke`
- `object-store-local-emulator-write-smoke`
- `table-lakehouse-boundary-report`
- `output-result-sink-and-fanout-boundary`

## Related Field Guide Terms

- `website/field-guide/no-fallback.html` - No fallback (`Start Here` / `runtime_supported`)
- `website/field-guide/universal-ingress.html` - UniversalIngress (`UniversalIngress` / `report_only`)
- `website/field-guide/scale-classes.html` - Scale classes (`Scale + Resource Envelope` / `planned`)
- `website/field-guide/object-store-boundary.html` - Object-store boundary (`Platform Boundaries` / `blocked`)
- `website/field-guide/table-lakehouse-boundary.html` - Table/lakehouse boundary (`Platform Boundaries` / `blocked`)
- `website/field-guide/deterministic-blockers.html` - Deterministic blockers (`Unsupported Diagnostics` / `runtime_supported`)
