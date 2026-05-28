<!-- SPDX-License-Identifier: Apache-2.0 -->

# Prepared/native Vortex runtime direction

## Quick Answer

- **Audience:** user evaluating the current runtime-development lane
- **Status:** `smoke_supported`
- **Execution mode:** `prepared_vortex/native_vortex`
- **Engine mode:** `batch`
- **Claim boundary:** Prepared/native smoke and structural evidence only; prepared_vortex starts from VortexPreparedState, while shardloom-prepare-batch prepares local compatibility inputs in the same CLI process before child query timing and emits in-route prepared_native_vortex_lifecycle, prepared_vortex_scale, and PulseWeave evidence over real Vortex fixture bytes. Selective-filter rows carry scoped stateless split-operator proof, and admitted aggregate/distinct/sort/window/join/CDC rows carry local stateful or shuffle split-operator replay/certificate proof with source-replay, bounded-memory, backpressure, spill-policy posture, and certificate-gated PulseWeave FlowInventory/ScarcityLedger/EndoPulse/ProofBound decisions where admitted. No standalone lifecycle, scale, or runtime-control lane; no larger-than-memory runtime, actual data-spill I/O, broad encoded-native, performance, superiority, SQL/DataFrame, object-store, lakehouse, Foundry, or Spark-replacement claim.

## Can ShardLoom Do This?

Prepared/native Vortex runtime direction has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

Prepared/native smoke and structural evidence only; prepared_vortex starts from VortexPreparedState, while shardloom-prepare-batch prepares local compatibility inputs in the same CLI process before child query timing and emits in-route prepared_native_vortex_lifecycle, prepared_vortex_scale, and PulseWeave evidence over real Vortex fixture bytes. Selective-filter rows carry scoped stateless split-operator proof, and admitted aggregate/distinct/sort/window/join/CDC rows carry local stateful or shuffle split-operator replay/certificate proof with source-replay, bounded-memory, backpressure, spill-policy posture, and certificate-gated PulseWeave FlowInventory/ScarcityLedger/EndoPulse/ProofBound decisions where admitted. No standalone lifecycle, scale, or runtime-control lane; no larger-than-memory runtime, actual data-spill I/O, broad encoded-native, performance, superiority, SQL/DataFrame, object-store, lakehouse, Foundry, or Spark-replacement claim.

## How To Try It

```text
python benchmarks\traditional_analytics\run.py --engines shardloom-prepared-vortex,shardloom-prepare-batch --formats csv,jsonl,parquet,arrow-ipc,avro,orc --scenario "filter + projection + limit" --dataset-profile tiny_smoke --rows 1000 --iterations 1 --output target\shardloom-prepared-vortex-smoke.json --regenerate
```

## Blocker

No current blocker is attached to this supported local smoke path beyond the claim boundary above.

## Internal Flow

`vortex_prepared_state, local_prepared_vortex_artifact, benchmark_fixture -> prepared_vortex/native_vortex -> batch -> prepared_native_timing_rows, source_backed_scan_evidence, prepared_native_lifecycle_evidence, prepared_vortex_scale_split_operator_evidence, pulseweave_runtime_control_evidence -> evidence -> claim gate`

## Evidence You Should See

- `execution_mode`
- `prepared_state_id`
- `prepared_state_digest`
- `timing_scope=warm_prepared_query`
- `source_backed_scan_used`
- `source_state_reuse_hit`
- `encoded_predicate_provider_status`
- `prepare_batch_route`
- `prepare_batch_preparation_millis`
- `prepare_batch_source_to_columnar_millis`
- `prepare_batch_lifecycle_status`
- `prepare_batch_lifecycle_scan_status`
- `prepare_batch_lifecycle_output_status`
- `prepare_batch_lifecycle_no_standalone_lane`
- `prepared_native_vortex_lifecycle_status`
- `prepared_native_vortex_lifecycle_scan_status`
- `prepared_native_vortex_lifecycle_scan_pushdown_status`
- `prepared_native_vortex_lifecycle_output_status`
- `prepared_native_vortex_lifecycle_no_standalone_lane`
- `prepare_batch_scale_no_standalone_lane`
- `prepare_batch_scale_real_bytes`
- `prepare_batch_scale_split_runtime_status`
- `prepare_batch_scale_pulseweave_status`
- `prepare_batch_scale_pulseweave_applied_count`
- `prepare_batch_scale_flow_inventory_min_wip_limit`
- `prepare_batch_scale_scarcity_ledger_selected_actions`
- `prepare_batch_scale_endopulse_adjustment_applied_count`
- `prepare_batch_scale_proofbound_claim_allowed_count`
- `prepared_vortex_scale_split_manifest_digest`
- `prepared_vortex_scale_split_runtime_status`
- `prepared_vortex_scale_split_execution_certificate_status`
- `prepared_vortex_scale_split_operator_runtime_status`
- `prepared_vortex_scale_split_operator_family`
- `prepared_vortex_scale_split_operator_stateful`
- `prepared_vortex_scale_split_operator_shuffle_required`
- `prepared_vortex_scale_split_operator_local_combine_used`
- `prepared_vortex_scale_split_operator_global_merge_used`
- `prepared_vortex_scale_split_operator_execution_certificate_status`
- `prepared_vortex_scale_split_operator_execution_certificate_id`
- `prepared_vortex_scale_split_operator_claim_gate_status`
- `prepared_vortex_scale_split_operator_retry_replay_status`
- `prepared_vortex_scale_split_operator_source_replay_status`
- `prepared_vortex_scale_split_operator_memory_envelope_status`
- `prepared_vortex_scale_split_operator_backpressure_status`
- `prepared_vortex_scale_split_operator_spill_policy_status`
- `prepared_vortex_scale_split_operator_output_commit_proof_status`
- `prepared_vortex_scale_split_operator_fallback_attempted=false`
- `prepared_vortex_scale_split_operator_external_engine_invoked=false`
- `pulseweave_status`
- `pulseweave_runtime_decision_applied`
- `pulseweave_policy_mutated`
- `pulseweave_decision_digest`
- `pulseweave_claim_gate_status`
- `flow_inventory_wip_limit`
- `scarcity_ledger_selected_action`
- `endopulse_next_wip_limit`
- `endopulse_persistent_state_used=false`
- `proofbound_certificate_status`
- `proofbound_claim_allowed`
- `prepared_vortex_scale_split_reader_digest`
- `prepared_vortex_scale_idempotency_key`
- `data_decoded`
- `data_materialized`
- `claim_gate_status`
- `fallback_attempted=false`
- `external_engine_invoked=false`

## Expected Output Or Evidence

Warm prepared Vortex rows separate from single-process prepare/batch rows, with source-backed scan, prepare_batch_lifecycle, prepared_native_vortex_lifecycle, prepared_vortex_scale, PulseWeave runtime-control evidence, scoped stateless/stateful/shuffle split-operator proof where admitted, and no-fallback fields where available.

## Common Mistakes

- `calling_prepared_vortex_production_ready`
- `treating_encoded_predicate_fields_as_encoded_native_claims`
- `expecting_sql_runtime`

## Reference Files

- `docs/architecture/compute-engine-flow-reference.md` - What this proves: Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.
- `docs/benchmarks/local-taxonomy-benchmark.md` - What this proves: Local benchmark taxonomy, evidence rows, and workload-scoped interpretation boundaries.
- `benchmarks/traditional_analytics/README.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `docs/architecture/benchmark-suite-catalog.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.

## Related Use Cases

- `compatibility-import-certified-local`
- `benchmark-interpretation-evidence-not-leaderboard`

## Related Field Guide Terms

- `website/field-guide/source-state.html` - SourceState (`UniversalIngress` / `smoke_supported`)
- `website/field-guide/vortex-ingest.html` - vortex_ingest (`Vortex Ingest` / `smoke_supported`)
- `website/field-guide/vortex-prepared-state.html` - VortexPreparedState (`Vortex Ingest` / `smoke_supported`)
- `website/field-guide/prepared-state-reuse.html` - Prepared state reuse (`Vortex Ingest` / `smoke_supported`)
- `website/field-guide/prepared-vortex.html` - prepared_vortex (`Prepared/Native Vortex` / `smoke_supported`)
- `website/field-guide/native-vortex.html` - native_vortex (`Prepared/Native Vortex` / `smoke_supported`)
- `website/field-guide/source-backed-scan.html` - Source-backed scan (`Prepared/Native Vortex` / `smoke_supported`)
- `website/field-guide/materialization-boundary.html` - Materialization boundary (`Evidence + Certificates` / `smoke_supported`)
- `website/field-guide/prepared-warm-route.html` - Prepared warm route (`Benchmarks` / `smoke_supported`)
