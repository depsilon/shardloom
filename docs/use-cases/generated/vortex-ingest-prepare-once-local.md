<!-- SPDX-License-Identifier: Apache-2.0 -->

# Vortex ingest prepare-once local smoke

## Quick Answer

- **Audience:** user who wants to create a local VortexPreparedState from an admitted local source
- **Status:** `smoke_supported`
- **Execution mode:** `vortex_ingest_to_prepared_vortex`
- **Engine mode:** `batch`
- **Claim boundary:** Feature-gated local prepare-once fixture smoke over flat non-null int/uint/float/UTF-8/date32/timestamp rows only; default builds block deterministically. No broad Vortex writer, object-store/table sink, production SQL/DataFrame, performance, superiority, Foundry, package-publication, or Spark-replacement claim.

## Can ShardLoom Do This?

Vortex ingest prepare-once local smoke has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

Feature-gated local prepare-once fixture smoke over flat non-null int/uint/float/UTF-8/date32/timestamp rows only; default builds block deterministically. No broad Vortex writer, object-store/table sink, production SQL/DataFrame, performance, superiority, Foundry, package-publication, or Spark-replacement claim.

## How To Try It

```powershell
New-Item -ItemType Directory -Force target | Out-Null; "id,label,amount`n1,alpha,8`n2,beta,15`n" | Set-Content -Encoding utf8 target\vortex-ingest-source.csv; cargo run -q -p shardloom-cli --features vortex-write -- vortex-ingest-smoke target\vortex-ingest-source.csv target\vortex-ingest-source.vortex --allow-overwrite --format json; $env:PYTHONPATH = "python\src"; python -c "from shardloom import context; ctx=context(repo_root='.', profile_order=('debug','release')); r=ctx.prepare_vortex('target/vortex-ingest-source.csv','target/vortex-ingest-source.vortex', allow_overwrite=True); print(r.vortex_ingest_status, r.prepared_state_created, r.input_row_count, r.fallback_attempted, r.external_engine_invoked)"
```

## Internal Flow

`local_csv_flat_non_null_scalars, local_json_flat_non_null_scalars, local_jsonl_flat_non_null_scalars, local_parquet_when_features_enabled -> vortex_ingest_to_prepared_vortex -> batch -> local_vortex_artifact, vortex_prepared_state_evidence -> evidence -> claim gate`

## Evidence You Should See

- `source_adapter_id`
- `source_state_id`
- `source_state_digest`
- `ingress_route=vortex_ingest`
- `vortex_ingest_status`
- `prepared_state_id`
- `prepared_state_digest`
- `vortex_artifact_digest`
- `writer_row_count`
- `reopen_row_count`
- `timing_scope=ingest_only`
- `fallback_attempted=false`
- `external_engine_invoked=false`
- `claim_gate_status`

## Expected Output Or Evidence

A local .vortex artifact plus VortexPreparedState id/digest, writer/reopen row-count proof, source-state fields, and no-fallback/no-external-engine evidence.

## Common Mistakes

- `assuming_prepared_vortex_reads_csv_directly`
- `treating_vortex_ingest_as_query_runtime`
- `expecting_default_build_to_write_vortex`
- `treating_smoke_as_performance_claim`

## Reference Files

- `README.md` - What this proves: Public technical-preview posture, Vortex-first/no-fallback positioning, and primary repo entrypoints.
- `python/README.md` - What this proves: Python wrapper posture, local smoke usage, and Python API claim boundaries.
- `docs/getting-started/examples.md` - What this proves: Current example catalog and local workflow entrypoints.
- `docs/architecture/compute-engine-flow-reference.md` - What this proves: Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.
- `docs/architecture/universal-ingress-route-taxonomy.md` - What this proves: UniversalIngress, vortex_ingest, VortexPreparedState, and route-timing contract boundaries.

## Related Use Cases

- `prepared-native-vortex-runtime-direction`
- `compatibility-import-certified-local`
- `output-result-sink-and-fanout-boundary`

## Related Field Guide Terms

- `website/field-guide/universal-ingress.html` - UniversalIngress (`UniversalIngress` / `report_only`)
- `website/field-guide/vortex-ingest.html` - vortex_ingest (`Vortex Ingest` / `smoke_supported`)
- `website/field-guide/vortex-prepared-state.html` - VortexPreparedState (`Vortex Ingest` / `smoke_supported`)
- `website/field-guide/prepared-vortex.html` - prepared_vortex (`Prepared/Native Vortex` / `smoke_supported`)
