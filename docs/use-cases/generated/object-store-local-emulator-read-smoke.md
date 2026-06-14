<!-- SPDX-License-Identifier: Apache-2.0 -->

# Object-store local-emulator read smoke

## Quick Answer

- **Audience:** user validating ShardLoom's first provider/profile-scoped object-store read proof without cloud credentials
- **Status:** `smoke_supported`
- **Execution mode:** `object_store_read_smoke`
- **Engine mode:** `batch`
- **Claim boundary:** Local-emulator object-store read smoke only; public no-credential fixture reads use a separate profile. Live real S3/GCS/ADLS providers, credentials, network probes, cloud writes, table/lakehouse commits, distributed runtime, production use, and performance claims remain blocked.

## Can ShardLoom Do This?

Object-store local-emulator read smoke has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

Local-emulator object-store read smoke only; public no-credential fixture reads use a separate profile. Live real S3/GCS/ADLS providers, credentials, network probes, cloud writes, table/lakehouse commits, distributed runtime, production use, and performance claims remain blocked.

## How To Try It

```text
target\debug\shardloom object-store-read-smoke target\object-store-fixture.bin --profile local-emulator --range 0:16 --format json
```

## Blocker

No current blocker is attached to this supported local smoke path beyond the claim boundary above.

## Internal Flow

`local_emulator_object_file -> object_store_read_smoke -> batch -> source_state_evidence, native_io_certificate, read_digest -> evidence -> claim gate`

## Evidence You Should See

- `provider_profile=local-emulator`
- `object_store_read_status`
- `byte_range_read_status`
- `full_file_read_status`
- `object_etag_status`
- `object_version_status`
- `object_store_checksum_validation_status`
- `object_store_checksum_algorithm`
- `object_store_checksum_scope`
- `object_store_request_count`
- `object_store_bytes_requested`
- `object_store_bytes_read`
- `object_store_bounded_read_status`
- `object_store_bounded_read_budget_bytes`
- `object_store_request_coalescing_status`
- `object_store_coalesced_request_count`
- `object_store_prefetch_status`
- `object_store_retry_policy_status`
- `object_store_retry_attempt_count`
- `object_store_rate_limit_policy_status`
- `object_store_cache_hit_count`
- `object_store_cache_miss_count`
- `source_state_id`
- `source_state_digest`
- `source_fingerprint_kind`
- `source_content_digest`
- `credential_resolution_performed=false`
- `network_probe_performed=false`
- `native_io_certificate_status`
- `claim_gate_status`
- `fallback_attempted=false`
- `external_engine_invoked=false`

## Expected Output Or Evidence

A fixture-smoke report with SourceState digest fields, selected byte-range evidence, requested-byte digest evidence, bounded-read budget evidence, request/byte/retry/cache counters, Native I/O certificate status, credential/network probes disabled, fallback_attempted=false, and external_engine_invoked=false.

## Common Mistakes

- `using_real_s3_uri_with_local_emulator_profile`
- `expecting_credentials_to_resolve`
- `treating_local_emulator_smoke_as_production_object_store_support`

## Reference Files

- `docs/architecture/object-store-request-planner.md` - What this proves: Object-store route admission, local-emulator evidence, and remote-provider blockers.
- `docs/architecture/universal-input-contract.md` - What this proves: Universal input contract posture and unsupported input-family diagnostics.
- `docs/architecture/vortex-public-api-inventory.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `python/README.md` - What this proves: Python wrapper scope, local smoke usage, and Python API claim boundaries.

## Related Use Cases

- `object-store-public-no-credential-fixture-read-smoke`
- `object-store-local-emulator-write-smoke`
- `object-store-boundary-report`
- `table-lakehouse-boundary-report`
- `output-result-sink-and-fanout-boundary`

## Related Field Guide Terms

- [Native I/O certificate](https://shardloom.io/field-guide/native-io-certificate) (`Evidence + Certificates` / `smoke_supported`)
- [Object-store boundary](https://shardloom.io/field-guide/object-store-boundary) (`Platform Boundaries` / `smoke_supported`)
