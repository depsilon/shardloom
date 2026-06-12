<!-- SPDX-License-Identifier: Apache-2.0 -->

# Vortex ingest prepare-once local smoke

## Quick Answer

- **Audience:** user who wants to create a local VortexPreparedState from an admitted local source
- **Status:** `smoke_supported`
- **Execution mode:** `vortex_ingest_to_prepared_vortex`
- **Engine mode:** `batch`
- **Claim boundary:** Feature-gated local prepare-once fixture smoke over flat scalar rows. Nullable/all-null boolean, int64, uint64, float64, utf8, binary, decimal128, date32, and timestamp_micros columns are admitted with dtype/family evidence. ingest_minimal records artifact/digest/writer evidence; ingest_certified reopens/scans row-count proof; full_replay waits for downstream replay. Scout ingress, layout/write advisor, copy-budget, append-only refinement, and capillary evidence stay inside vortex_ingest. Append-only refinement is local CSV/JSONL prefix-verified, writes a delta artifact, leaves the base unchanged, and admits count-family consumer evidence only. Unknown/unsupported NULL-bearing Vortex batches block before writer conversion; malformed sources, unsupported strategies, unsafe lifetime shortcuts, non-append drift, missing manifests, format/compression drift, and update/delete/upsert/schema mismatch block before claims. Python session reuse is local/fingerprint-gated. No broad Vortex writer, object-store/table sink, CDC/table transaction, production SQL/DataFrame, persistent/distributed cache, performance, superiority, Foundry, package-publication, or Spark-replacement claim.

## Can ShardLoom Do This?

Vortex ingest prepare-once local smoke has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

Feature-gated local prepare-once fixture smoke over flat scalar rows. Nullable/all-null boolean, int64, uint64, float64, utf8, binary, decimal128, date32, and timestamp_micros columns are admitted with dtype/family evidence. ingest_minimal records artifact/digest/writer evidence; ingest_certified reopens/scans row-count proof; full_replay waits for downstream replay. Scout ingress, layout/write advisor, copy-budget, append-only refinement, and capillary evidence stay inside vortex_ingest. Append-only refinement is local CSV/JSONL prefix-verified, writes a delta artifact, leaves the base unchanged, and admits count-family consumer evidence only. Unknown/unsupported NULL-bearing Vortex batches block before writer conversion; malformed sources, unsupported strategies, unsafe lifetime shortcuts, non-append drift, missing manifests, format/compression drift, and update/delete/upsert/schema mismatch block before claims. Python session reuse is local/fingerprint-gated. No broad Vortex writer, object-store/table sink, CDC/table transaction, production SQL/DataFrame, persistent/distributed cache, performance, superiority, Foundry, package-publication, or Spark-replacement claim.

## How To Try It

```powershell
New-Item -ItemType Directory -Force target | Out-Null; "id,label,amount`n1,alpha,8`n2,beta,15`n" | Set-Content -Encoding utf8 target\vortex-ingest-source.csv; cargo run -q -p shardloom-cli --features vortex-write -- vortex-ingest-smoke target\vortex-ingest-source.csv target\vortex-ingest-source.vortex --allow-overwrite --format json; $env:PYTHONPATH = "python\src"; python -c "from shardloom import context; ctx=context(repo_root='.', profile_order=('debug','release')); r=ctx.prepare_vortex('target/vortex-ingest-source.csv','target/vortex-ingest-source.vortex', allow_overwrite=True); print(r.vortex_ingest_status, r.prepared_state_created, r.input_row_count, r.fallback_attempted, r.external_engine_invoked)"
```

## Blocker

No current blocker is attached to this supported local smoke path beyond the claim boundary above.

## Internal Flow

`local_csv_flat_scalars, local_json_flat_scalars, local_jsonl_flat_scalars, local_parquet_when_features_enabled -> vortex_ingest_to_prepared_vortex -> batch -> local_vortex_artifact, vortex_prepared_state_evidence -> evidence -> claim gate`

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
- `reopen_verification_status`
- `vortex_scout_ingress_status`
- `vortex_scout_ingress_anomaly_count`
- `vortex_scout_ingress_quarantine_required`
- `vortex_scout_ingress_no_standalone_lane_status`
- `vortex_layout_write_advisor_status`
- `vortex_layout_write_advisor_strategy_admitted`
- `vortex_layout_write_advisor_no_standalone_lane_status`
- `vortex_preparation_spine_status`
- `vortex_preparation_spine_vortex_first_decision`
- `vortex_preparation_spine_source_split_refs`
- `vortex_preparation_spine_source_byte_range_refs`
- `vortex_preparation_spine_source_row_range_refs`
- `vortex_preparation_spine_native_io_certificate_status`
- `vortex_preparation_spine_no_standalone_lane_status`
- `vortex_copy_budget_status`
- `vortex_copy_budget_measurement_status`
- `vortex_copy_budget_buffer_reuse_status`
- `vortex_copy_budget_unsafe_lifetime_shortcut_status`
- `vortex_copy_budget_no_standalone_lane_status`
- `vortex_differential_preparation_status`
- `vortex_differential_preparation_update_mode`
- `vortex_differential_preparation_refinement_status`
- `vortex_differential_preparation_refinement_mode`
- `vortex_differential_preparation_automatic_detection_status`
- `vortex_differential_preparation_blocker_id`
- `vortex_differential_preparation_delta_manifest_digest`
- `vortex_differential_preparation_refinement_manifest_path`
- `vortex_differential_preparation_refinement_manifest_digest`
- `vortex_differential_preparation_refinement_manifest_written`
- `vortex_differential_preparation_refined_prepared_state_id`
- `vortex_differential_preparation_overlay_consumer_family`
- `vortex_differential_preparation_overlay_consumer_status`
- `vortex_differential_preparation_overlay_consumer_correctness_digest`
- `vortex_differential_preparation_changed_row_range_refs`
- `vortex_differential_preparation_overlay_applied`
- `vortex_differential_preparation_no_standalone_lane_status`
- `vortex_capillary_preparation_status`
- `vortex_capillary_preparation_activation_policy`
- `vortex_capillary_preparation_activation_result`
- `vortex_capillary_preparation_activation_reason`
- `vortex_capillary_preparation_activation_threshold_bytes`
- `vortex_capillary_preparation_activation_threshold_rows`
- `vortex_capillary_preparation_activation_observed_bytes`
- `vortex_capillary_preparation_activation_observed_rows`
- `vortex_capillary_preparation_activation_observed_split_count`
- `vortex_capillary_preparation_task_roles`
- `vortex_capillary_preparation_execution_window_count`
- `vortex_capillary_preparation_execution_window_ids`
- `vortex_capillary_preparation_scheduler_applied`
- `vortex_capillary_preparation_prewrite_status`
- `vortex_capillary_preparation_prewrite_scheduler_applied`
- `vortex_capillary_preparation_prewrite_execution_window_count`
- `vortex_capillary_preparation_prewrite_array_build_gate_status`
- `vortex_capillary_preparation_prewrite_write_gate_status`
- `vortex_capillary_preparation_prewrite_reopen_gate_status`
- `vortex_capillary_preparation_prewrite_sink_evidence_gate_status`
- `vortex_capillary_preparation_read_chunk_byte_range_refs`
- `vortex_capillary_preparation_row_range_refs`
- `vortex_capillary_preparation_vortex_segment_refs`
- `vortex_capillary_preparation_pulseweave_status`
- `vortex_capillary_preparation_pulseweave_runtime_decision_applied`
- `vortex_capillary_preparation_no_standalone_lane_status`
- `certification_level`
- `certification_status`
- `timing_scope=ingest_only`
- `session_id`
- `session_state_scope`
- `source_state_reuse_hit`
- `prepared_state_reuse_hit`
- `session_cache_smoke_status`
- `session_cache_hit_count`
- `session_cache_invalidation_count`
- `session_cache_buffer_reuse_count`
- `reuse_reason`
- `fallback_attempted=false`
- `external_engine_invoked=false`
- `claim_gate_status`

## Expected Output Or Evidence

A local .vortex artifact plus VortexPreparedState id/digest, certification-level/status fields, writer evidence, optional reopen row-count proof for ingest_certified, source-state fields, in-route scout-ingress/layout-write/copy-budget/differential/capillary/PulseWeave cold-preparation evidence, optional Python session reuse evidence, optional session-cache-smoke lifecycle evidence, and no-fallback/no-external-engine evidence.

## Common Mistakes

- `assuming_prepared_vortex_reads_csv_directly`
- `treating_vortex_ingest_as_query_runtime`
- `expecting_default_build_to_write_vortex`
- `treating_session_reuse_as_persistent_cache`
- `treating_smoke_as_performance_claim`

## Reference Files

- `README.md` - What this proves: Public technical-preview posture, Vortex-first positioning, and no-fallback boundaries.
- `python/README.md` - What this proves: Python wrapper scope, local smoke usage, and Python API claim boundaries.
- `docs/getting-started/examples.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `docs/architecture/compute-engine-flow-reference.md` - What this proves: Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.
- `docs/architecture/universal-ingress-route-taxonomy.md` - What this proves: UniversalIngress, Vortex ingest, prepared-state, and route-timing contract boundaries.

## Related Use Cases

- `prepared-native-vortex-runtime-direction`
- `compatibility-import-certified-local`
- `output-result-sink-and-fanout-boundary`

## Related Field Guide Terms

- [UniversalIngress](https://shardloom.io/field-guide/universal-ingress) (`UniversalIngress` / `report_only`)
- [vortex_ingest](https://shardloom.io/field-guide/vortex-ingest) (`Vortex Ingest` / `smoke_supported`)
- [VortexPreparedState](https://shardloom.io/field-guide/vortex-prepared-state) (`Vortex Ingest` / `smoke_supported`)
- [prepared_vortex](https://shardloom.io/field-guide/prepared-vortex) (`Prepared/Native Vortex` / `smoke_supported`)
