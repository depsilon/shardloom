<!-- SPDX-License-Identifier: Apache-2.0 -->

# Object-store local-emulator write/commit smoke

## Quick Answer

- **Audience:** user validating ShardLoom's first provider/profile-scoped object-store write proof without cloud credentials
- **Status:** `smoke_supported`
- **Execution mode:** `object_store_write_smoke`
- **Engine mode:** `batch`
- **Claim boundary:** Local-emulator staged object write/commit smoke only; real S3/GCS/ADLS providers, credentials, network probes, provider listing, public/authenticated cloud writes, table/lakehouse commits, catalogs, distributed runtime, production use, and performance claims remain blocked.

## Can ShardLoom Do This?

ShardLoom can run an explicit local-emulator object-store write smoke from a local source file to a local object path. This is runtime write/commit proof for the declared emulator profile only.

## Claim Boundary

Local-emulator staged object write/commit smoke only; real S3/GCS/ADLS providers, credentials, network probes, provider listing, public/authenticated cloud writes, table/lakehouse commits, catalogs, distributed runtime, production use, and performance claims remain blocked.

## How To Try It

```powershell
target\debug\shardloom object-store-write-smoke target\source.bin target\object-store-fixture.bin --profile local-emulator --idempotency-key orders-batch-001 --rollback-after-commit --format json
```

## Internal Flow

`local_source_file -> object_store_write_smoke -> batch -> committed_local_object, sidecar_commit_manifest, rollback_cleanup_evidence, native_io_certificate -> evidence -> claim gate`

## Evidence You Should See

- `provider_profile=local-emulator`
- `object_store_write_status`
- `write_staging_status`
- `commit_protocol_status`
- `commit_status`
- `rollback_status`
- `cleanup_deleted_count`
- `idempotency_key`
- `idempotency_status`
- `payload_digest`
- `target_content_digest`
- `commit_manifest_digest`
- `credential_resolution_performed=false`
- `network_probe_performed=false`
- `native_io_certificate_status`
- `claim_gate_status`
- `fallback_attempted=false`
- `external_engine_invoked=false`

## Expected Output Or Evidence

A fixture-smoke report with staged write, sidecar commit-manifest, idempotency, payload/target/manifest digest, optional rollback cleanup, Native I/O, credential/network-disabled, fallback_attempted=false, and external_engine_invoked=false evidence.

## Common Mistakes

- `using_real_s3_uri`
- `treating_sidecar_manifest_as_lakehouse_commit`
- `expecting_credentials_to_resolve`
- `treating_local_emulator_smoke_as_production_object_store_support`

## Reference Files

- `docs/architecture/object-store-request-planner.md` - What this proves: Object-store request planning posture and the local-emulator write/commit runtime boundary.
- `docs/architecture/compute-engine-flow-reference.md` - What this proves: Compute-flow support boundaries and no-fallback claim context for object-store smokes.
- `docs/architecture/phased-execution-completed-ledger.md` - What this proves: Completed-session provenance for the local-emulator object write proof and its claim boundary.
- `python/README.md` - What this proves: Python-facing command wrapper posture and local technical-preview scope.

## Related Use Cases

- `object-store-public-no-credential-fixture-read-smoke`
- `object-store-local-emulator-read-smoke`
- `object-store-boundary-report`
- `local-table-append-commit-rehearsal-smoke`
- `table-lakehouse-boundary-report`
- `output-result-sink-and-fanout-boundary`

## Related Field Guide Terms

- `website/field-guide/native-io-certificate.html` - Native I/O certificate (`Evidence + Certificates` / `smoke_supported`)
- `website/field-guide/object-store-boundary.html` - Object-store boundary (`Platform Boundaries` / `smoke_supported`)
