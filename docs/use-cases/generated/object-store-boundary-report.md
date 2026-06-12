<!-- SPDX-License-Identifier: Apache-2.0 -->

# Object-store and cloud storage boundary

## Quick Answer

- **Audience:** user asking whether S3, GCS, or ADLS runtime I/O works
- **Status:** `smoke_supported`
- **Execution mode:** `object_store_read_smoke`
- **Engine mode:** `batch`
- **Claim boundary:** S3/GCS/ADLS URI parsing plus an explicit public no-credential fixture read profile is smoke-supported when the caller supplies local fixture bytes with --public-fixture-path. Live provider network reads, credential resolution, authenticated reads, cache writes, cloud writes, table/lakehouse commits, distributed runtime, production use, and performance claims remain blocked.

## Can ShardLoom Do This?

Object-store and cloud storage boundary has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

S3/GCS/ADLS URI parsing plus an explicit public no-credential fixture read profile is smoke-supported when the caller supplies local fixture bytes with --public-fixture-path. Live provider network reads, credential resolution, authenticated reads, cache writes, cloud writes, table/lakehouse commits, distributed runtime, production use, and performance claims remain blocked.

## How To Try It

```text
target\debug\shardloom object-store-read-smoke s3://shardloom-public-fixtures/orders.vortex --profile public-no-credential-fixture --public-fixture-path target\object-store-public-fixture.vortex --range 0:16 --format json
```

## Blocker

Live cloud object-store I/O still needs provider, credential, byte-range, retry, idempotency, commit, certificate, and no-fallback evidence before support can be claimed beyond explicit local fixture bytes.

## Internal Flow

`s3_uri, gcs_uri, adls_uri -> object_store_read_smoke -> batch -> object_store_plan, deterministic_blocker, public_fixture_read_evidence -> evidence -> claim gate`

## Evidence You Should See

- `provider_profile=public-no-credential-fixture`
- `object_store_uri_parse_status`
- `credential_policy_status`
- `public_no_credential_fixture_claim_allowed`
- `network_probe_allowed=false`
- `network_probe_performed=false`
- `provider_probe_performed=false`
- `object_store_io`
- `write_io=false`
- `native_io_certificate_status`
- `claim_gate_status`
- `fallback_attempted=false`
- `external_engine_invoked=false`

## Expected Output Or Evidence

A public no-credential fixture smoke report with parsed provider/bucket/key fields, SourceState digest fields, selected byte-range/full-file evidence, Native I/O certificate status, credential/network/provider probes disabled, fallback_attempted=false, and external_engine_invoked=false.

## Common Mistakes

- `expecting_live_public_s3_network_read`
- `assuming_signed_url_support`
- `omitting_public_fixture_path`
- `treating_planner_as_runtime_io`

## Reference Files

- `docs/architecture/compute-engine-flow-reference.md` - What this proves: Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.
- `docs/architecture/universal-input-contract.md` - What this proves: Universal input contract posture and unsupported input-family diagnostics.
- `docs/architecture/object-store-request-planner.md` - What this proves: Object-store route admission, local-emulator evidence, and remote-provider blockers.
- `docs/architecture/universal-compatibility-coverage-scoreboard.md` - What this proves: Compatibility scoreboard status and source/sink support boundaries.

## Related Use Cases

- `object-store-public-no-credential-fixture-read-smoke`
- `object-store-local-emulator-read-smoke`
- `object-store-local-emulator-write-smoke`
- `table-lakehouse-boundary-report`
- `output-result-sink-and-fanout-boundary`

## Related Field Guide Terms

- [No fallback](https://shardloom.io/field-guide/no-fallback) (`Start Here` / `runtime_supported`)
- [UniversalIngress](https://shardloom.io/field-guide/universal-ingress) (`UniversalIngress` / `report_only`)
- [Scale classes](https://shardloom.io/field-guide/scale-classes) (`Scale + Resource Envelope` / `planned`)
- [Object-store boundary](https://shardloom.io/field-guide/object-store-boundary) (`Platform Boundaries` / `smoke_supported`)
- [Table/lakehouse boundary](https://shardloom.io/field-guide/table-lakehouse-boundary) (`Platform Boundaries` / `blocked`)
- [Deterministic blockers](https://shardloom.io/field-guide/deterministic-blockers) (`Unsupported Diagnostics` / `runtime_supported`)
