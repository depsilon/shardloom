<!-- SPDX-License-Identifier: Apache-2.0 -->

# Object-store public no-credential fixture read smoke

## Quick Answer

- **Audience:** user validating provider/profile-scoped S3/GCS/ADLS URI admission without credentials or network effects
- **Status:** `smoke_supported`
- **Execution mode:** `object_store_read_smoke`
- **Engine mode:** `batch`
- **Claim boundary:** Public no-credential fixture read smoke only. ShardLoom parses a supported S3/GCS/ADLS URI and reads explicit local fixture bytes supplied by the caller; it does not contact a provider, resolve credentials, probe a network, write cache entries, perform cloud writes, commit tables, enable distributed runtime, claim production use, or claim performance.

## Can ShardLoom Do This?

Object-store public no-credential fixture read smoke has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

Public no-credential fixture read smoke only. ShardLoom parses a supported S3/GCS/ADLS URI and reads explicit local fixture bytes supplied by the caller; it does not contact a provider, resolve credentials, probe a network, write cache entries, perform cloud writes, commit tables, enable distributed runtime, claim production use, or claim performance.

## How To Try It

```text
target\debug\shardloom object-store-read-smoke s3://shardloom-public-fixtures/orders.vortex --profile public-no-credential-fixture --public-fixture-path target\object-store-public-fixture.vortex --fixture-listing --range 0:16 --format json
```

## Blocker

No current blocker is attached to this supported local smoke path beyond the claim boundary above.

## Internal Flow

`s3_uri, gcs_uri, adls_uri, public_fixture_local_file -> object_store_read_smoke -> batch -> provider_uri_parse_evidence, source_state_evidence, native_io_certificate, read_digest -> evidence -> claim gate`

## Evidence You Should See

- `provider_profile=public-no-credential-fixture`
- `object_store_provider`
- `object_store_bucket`
- `object_store_key`
- `object_store_uri_parse_status`
- `requested_uri_redaction_status`
- `public_fixture_path`
- `public_no_credential_fixture_profile=true`
- `byte_range_read_status`
- `full_file_read_status`
- `listing_status`
- `object_etag`
- `object_version`
- `source_state_id`
- `source_state_digest`
- `source_fingerprint_kind`
- `source_content_digest`
- `credential_policy_status=public_no_credential_fixture_admitted`
- `credential_resolution_performed=false`
- `network_probe_performed=false`
- `provider_probe_performed=false`
- `local_cache_status`
- `native_io_certificate_status=public_fixture_smoke_only`
- `public_no_credential_fixture_claim_allowed=true`
- `claim_gate_status=public_fixture_smoke_only`
- `fallback_attempted=false`
- `external_engine_invoked=false`

## Expected Output Or Evidence

A public fixture report with parsed provider/bucket/key fields, no credential/network/provider probe fields, optional single-object fixture listing evidence, SourceState digest fields, ETag/version fixture evidence, Native I/O status, fallback_attempted=false, and external_engine_invoked=false.

## Common Mistakes

- `omitting_public_fixture_path`
- `expecting_network_fetch`
- `expecting_credentials_to_resolve`
- `treating_public_fixture_smoke_as_live_cloud_provider_support`
- `treating_fixture_listing_as_provider_listing`

## Reference Files

- `docs/architecture/object-store-request-planner.md` - What this proves: Object-store route admission, local-emulator evidence, and remote-provider blockers.
- `docs/architecture/universal-input-contract.md` - What this proves: Universal input contract posture and unsupported input-family diagnostics.
- `docs/architecture/vortex-public-api-inventory.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `python/README.md` - What this proves: Python wrapper scope, local smoke usage, and Python API claim boundaries.

## Related Use Cases

- `object-store-local-emulator-read-smoke`
- `object-store-boundary-report`
- `object-store-local-emulator-write-smoke`
- `table-lakehouse-boundary-report`

## Related Field Guide Terms

- `website/field-guide/no-fallback.html` - No fallback (`Start Here` / `runtime_supported`)
- `website/field-guide/universal-ingress.html` - UniversalIngress (`UniversalIngress` / `report_only`)
- `website/field-guide/native-io-certificate.html` - Native I/O certificate (`Evidence + Certificates` / `smoke_supported`)
- `website/field-guide/scale-classes.html` - Scale classes (`Scale + Resource Envelope` / `planned`)
- `website/field-guide/object-store-boundary.html` - Object-store boundary (`Platform Boundaries` / `smoke_supported`)
- `website/field-guide/deterministic-blockers.html` - Deterministic blockers (`Unsupported Diagnostics` / `runtime_supported`)
