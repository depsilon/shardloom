<!-- SPDX-License-Identifier: Apache-2.0 -->

# Output and fanout boundary

## Quick Answer

- **Audience:** user asking what ShardLoom can write today and what fanout means
- **Status:** `smoke_supported`
- **Execution mode:** `compatibility_import_certified`
- **Engine mode:** `batch`
- **Claim boundary:** Result-sink and fanout smoke is local and scoped; local Vortex output/fanout is feature-gated and flat-scalar only. Local replay/fidelity evidence verifies sink artifacts, and Python ShardLoomSession can reuse local query/output reports only when statement, source fingerprints, and output artifact fingerprints still match. CLI session-cache-smoke proves scoped OutputPlan cache lifecycle, invalidation, close, and cleanup evidence only. This is not broad writer fidelity, S3/object-store write, table commit, persistent OutputPlan cache, broad Vortex writer behavior, or production sink support.

## Can ShardLoom Do This?

Output and fanout boundary has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

Result-sink and fanout smoke is local and scoped; local Vortex output/fanout is feature-gated and flat-scalar only. Local replay/fidelity evidence verifies sink artifacts, and Python ShardLoomSession can reuse local query/output reports only when statement, source fingerprints, and output artifact fingerprints still match. CLI session-cache-smoke proves scoped OutputPlan cache lifecycle, invalidation, close, and cleanup evidence only. This is not broad writer fidelity, S3/object-store write, table commit, persistent OutputPlan cache, broad Vortex writer behavior, or production sink support.

## How To Try It

```text
with ctx.session() as session: result = session.fanout(ctx.read_csv("target/input.csv").select("id").limit(10), {"jsonl": "target/out.jsonl", "csv": "target/out.csv"}, allow_overwrite=True); replay = session.fanout(ctx.read_csv("target/input.csv").select("id").limit(10), {"jsonl": "target/out.jsonl", "csv": "target/out.csv"}); print(replay.output_plan_reuse_hit, replay.result_replay_reuse_hit); # with --features vortex-write: ctx.read_csv("target/input.csv").select("id").limit(10).write_vortex("target/out.vortex", allow_overwrite=True)
```

## Blocker

No current blocker is attached to this supported local smoke path beyond the claim boundary above.

## Internal Flow

`local_benchmark_fixture, prepared_vortex_artifact -> compatibility_import_certified -> batch -> local_result_sink_artifact, local_jsonl_csv_fanout, feature_gated_structured_fanout, feature_gated_local_vortex_output, output_certificate -> evidence -> claim gate`

## Evidence You Should See

- `result_sink_write_millis`
- `output_native_io_certificate_status`
- `output_format`
- `output_plan_id`
- `output_plan_digest`
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

A local result-sink or fanout proof artifact with per-output digest/certificate fields plus result_replay_verified, output_replay_status, output_fidelity_report_status, output_fidelity_loss, and fanout replay/fidelity status lists for admitted local sinks; Vortex rows include artifact digest and upstream writer/reopen proof when built with --features vortex-write. Python session reuse adds session_id, output_plan_reuse_hit, result_replay_reuse_hit, and reuse_reason; session-cache-smoke adds scoped OutputPlan reuse, invalidation, close, and cleanup evidence. Claim-grade/broad replay remains gated to later OutputPlan slices.

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
