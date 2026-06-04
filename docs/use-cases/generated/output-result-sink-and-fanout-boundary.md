<!-- SPDX-License-Identifier: Apache-2.0 -->

# Output and fanout boundary

## Quick Answer

- **Audience:** user asking what ShardLoom can write today and what fanout means
- **Status:** `smoke_supported`
- **Execution mode:** `compatibility_import_certified`
- **Engine mode:** `batch`
- **Claim boundary:** Result-sink and fanout smoke is local/scoped; local Vortex output/fanout is feature-gated and flat-scalar only. Replay/fidelity verifies sink artifacts; fanout DAG verifies schema/row normalization; output capillary records bounded conversion/write/replay admission; layout/write advisor records Vortex writer admission or compatibility-target posture with metadata preservation/loss. Parquet/Arrow IPC/Avro and non-null local Vortex typed decimal sinks preserve precision/scale; ORC and nullable/all-null local Vortex typed decimals block deterministically. ShardLoomSession reuses local query/output reports only when statement, source fingerprints, and output fingerprints still match; session-cache-smoke proves scoped OutputPlan lifecycle only. Not broad writer fidelity, S3/object-store write, table commit, persistent OutputPlan cache, arbitrary layout optimization, broad Vortex writer behavior, performance evidence, or production sink support.

## Can ShardLoom Do This?

Output and fanout boundary has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

Result-sink and fanout smoke is local/scoped; local Vortex output/fanout is feature-gated and flat-scalar only. Replay/fidelity verifies sink artifacts; fanout DAG verifies schema/row normalization; output capillary records bounded conversion/write/replay admission; layout/write advisor records Vortex writer admission or compatibility-target posture with metadata preservation/loss. Parquet/Arrow IPC/Avro and non-null local Vortex typed decimal sinks preserve precision/scale; ORC and nullable/all-null local Vortex typed decimals block deterministically. ShardLoomSession reuses local query/output reports only when statement, source fingerprints, and output fingerprints still match; session-cache-smoke proves scoped OutputPlan lifecycle only. Not broad writer fidelity, S3/object-store write, table commit, persistent OutputPlan cache, arbitrary layout optimization, broad Vortex writer behavior, performance evidence, or production sink support.

## How To Try It

```text
with ctx.session() as session: result = session.fanout(ctx.read_csv("target/input.csv").select("id").limit(10), {"jsonl": "target/out.jsonl", "csv": "target/out.csv"}, allow_overwrite=True); replay = session.fanout(ctx.read_csv("target/input.csv").select("id").limit(10), {"jsonl": "target/out.jsonl", "csv": "target/out.csv"}); print(replay.output_plan_reuse_hit, replay.result_replay_reuse_hit); # with --features vortex-write: ctx.read_csv("target/input.csv").select("id").limit(10).write_vortex("target/out.vortex", allow_overwrite=True)
```

## Blocker

No current blocker is attached to this supported local smoke path beyond the claim boundary above.

## Internal Flow

`local_benchmark_fixture, prepared_vortex_artifact -> compatibility_import_certified -> batch -> ResultBatchState -> OutputPlan -> shared fanout conversion DAG -> output capillary admission -> output layout/write advisor -> local_result_sink_artifact, local_jsonl_csv_fanout, feature_gated_structured_fanout, feature_gated_local_vortex_output, output_certificate -> evidence -> claim gate`

## Evidence You Should See

- `result_sink_write_millis`
- `result_batch_state_status`
- `result_batch_state_digest`
- `result_batch_state_layout`
- `result_batch_state_row_count`
- `result_batch_state_column_count`
- `result_batch_state_materialization_required`
- `result_batch_state_decode_required`
- `result_batch_state_build_millis`
- `output_conversion_millis`
- `sink_artifact_conversion_millis`
- `fanout_output_conversion_millis`
- `output_capillary_status`
- `output_capillary_task_roles`
- `output_capillary_window_count`
- `output_sink_pressure_status`
- `output_memory_pressure_status`
- `pulseweave_output_policy_applied`
- `output_layout_write_advisor_status`
- `output_layout_write_advisor_selected_strategy`
- `output_layout_write_advisor_runtime_decision_applied`
- `output_metadata_preservation_map`
- `output_metadata_loss`
- `output_native_io_certificate_status`
- `output_format`
- `output_plan_id`
- `output_plan_digest`
- `output_plan_materialization_required`
- `output_plan_required_columns`
- `output_plan_ordering_required`
- `output_plan_statistics_required`
- `output_plan_text_materialization_boundary`
- `output_plan_conversion_blocker`
- `output_fanout_performed`
- `fanout_output_count`
- `fanout_output_formats`
- `fanout_output_digests`
- `fanout_result_reuse_hit`
- `result_replay_verified`
- `output_replay_status`
- `output_replay_millis`
- `output_fidelity_report_status`
- `output_fidelity_loss`
- `fanout_output_replay_statuses`
- `fanout_output_fidelity_statuses`
- `fanout_output_fidelity_loss`
- `vortex_output_runtime_execution`
- `vortex_output_reopen_verified`
- `vortex_artifact_digest`
- `upstream_vortex_write_called`
- `upstream_vortex_scan_called`
- `session_id`
- `session_state_scope`
- `output_plan_reuse_hit`
- `result_replay_reuse_hit`
- `session_cache_smoke_status`
- `session_cache_output_plan_reuse_count`
- `session_cache_cleanup_entries_removed`
- `reuse_reason`
- `claim_gate_status`
- `fallback_attempted=false`
- `external_engine_invoked=false`

## Expected Output Or Evidence

A local result-sink or fanout proof artifact with shared ResultBatchState, sink-driven OutputPlan, fanout conversion DAG, thresholded output capillary, layout/write advisor, metadata preservation/loss, conversion timing, per-output digest/certificate, replay, and fidelity fields. Parquet/Arrow IPC/Avro and non-null local Vortex typed decimal rows preserve decimal128 precision/scale, while ORC typed decimal and nullable/all-null Vortex typed decimal requests report deterministic conversion blockers. Vortex rows include artifact digest and upstream writer/reopen proof when built with --features vortex-write. Session reuse exposes session_id, output_plan_reuse_hit, result_replay_reuse_hit, and reuse_reason; session-cache-smoke exposes scoped cache lifecycle evidence. Claim-grade/broad replay remains gated.

## Common Mistakes

- `coupling_input_format_to_output_format`
- `treating_local_sink_as_s3_write`
- `treating_session_output_reuse_as_persistent_cache`
- `assuming_lakehouse_commit`
- `expecting_vortex_default_build_support`

## Reference Files

- `docs/architecture/io-reuse-and-fanout-architecture.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `docs/architecture/compute-engine-flow-reference.md` - What this proves: Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.
- `docs/benchmarks/local-taxonomy-benchmark.md` - What this proves: Local benchmark taxonomy, evidence rows, and workload-scoped interpretation boundaries.
- `examples/local-vortex-benchmark/README.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.

## Related Use Cases

- `compatibility-import-certified-local`
- `source-free-generated-output-boundary`
- `object-store-boundary-report`

## Related Field Guide Terms

- `website/field-guide/prepared-state-reuse.html` - Prepared state reuse (`Vortex Ingest` / `smoke_supported`)
- `website/field-guide/native-io-certificate.html` - Native I/O certificate (`Evidence + Certificates` / `smoke_supported`)
- `website/field-guide/result-sink-replay.html` - Result-sink replay (`Evidence + Certificates` / `smoke_supported`)
- `website/field-guide/output-plan.html` - OutputPlan (`I/O + Outputs` / `smoke_supported`)
- `website/field-guide/sink-artifact.html` - SinkArtifact (`I/O + Outputs` / `smoke_supported`)
- `website/field-guide/output-fanout.html` - Output fanout (`I/O + Outputs` / `planned`)
