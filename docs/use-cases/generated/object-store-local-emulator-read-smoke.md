<!-- SPDX-License-Identifier: Apache-2.0 -->

# Object-store local-emulator read smoke

## Quick Answer

- **Audience:** user validating ShardLoom's first provider/profile-scoped object-store read proof without cloud credentials
- **Status:** `smoke_supported`
- **Execution mode:** `object_store_read_smoke`
- **Engine mode:** `batch`
- **Claim boundary:** Local-emulator object-store read smoke only; real S3/GCS/ADLS providers, credentials, network probes, writes, commits, table/lakehouse runtime, distributed runtime, production use, and performance claims remain blocked.

## Can ShardLoom Do This?

ShardLoom can run an explicit local-emulator object-store read smoke over a local fixture file. This is runtime read proof for the declared emulator profile only.

## Claim Boundary

Local-emulator object-store read smoke only; real S3/GCS/ADLS providers, credentials, network probes, writes, commits, table/lakehouse runtime, distributed runtime, production use, and performance claims remain blocked.

## How To Try It

```powershell
target\debug\shardloom object-store-read-smoke target\object-store-fixture.bin --profile local-emulator --range 0:16 --format json
```

## Internal Flow

`local_emulator_object_file -> object_store_read_smoke -> batch -> source_state_evidence, native_io_certificate, read_digest -> evidence -> claim gate`

## Evidence You Should See

- `provider_profile=local-emulator`
- `object_store_read_status`
- `byte_range_read_status`
- `full_file_read_status`
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

A fixture-smoke report with SourceState digest fields, selected byte-range evidence, Native I/O certificate status, credential/network probes disabled, fallback_attempted=false, and external_engine_invoked=false.

## Common Mistakes

- `using_real_s3_uri`
- `expecting_credentials_to_resolve`
- `treating_local_emulator_smoke_as_production_object_store_support`

## Reference Files

- `docs/architecture/object-store-request-planner.md` - What this proves: Object-store request planning posture and blocked/runtime admission boundaries.
- `docs/architecture/universal-input-contract.md` - What this proves: Universal input adapter boundaries and no-fallback input contract for object-store-like sources.
- `docs/architecture/vortex-public-api-inventory.md` - What this proves: Vortex 0.71 I/O hooks are candidate inputs but not broad object-store admission by themselves.
- `python/README.md` - What this proves: Python-facing command wrapper posture and local technical-preview scope.

## Related Use Cases

- `object-store-boundary-report`
- `table-lakehouse-boundary-report`
- `output-result-sink-and-fanout-boundary`

## Related Field Guide Terms

- `website/field-guide/object-store-boundary.html` - Object-Store Boundary (`I/O And Output` / `blocked-report-only`)
- `website/field-guide/source-state.html` - SourceState (`Vortex Runtime` / `planned`)
- `website/field-guide/native-io-certificate.html` - Native I/O Certificate (`Evidence And Claims` / `current-evidence`)
- `website/field-guide/no-fallback.html` - No Fallback (`Start Here` / `core-contract`)
