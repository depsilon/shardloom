<!-- SPDX-License-Identifier: Apache-2.0 -->

# Output and fanout boundary

## Quick Answer

- **Audience:** user asking what ShardLoom can write today and what fanout means
- **Status:** `smoke_supported`
- **Execution mode:** `compatibility_import_certified`
- **Engine mode:** `batch`
- **Claim boundary:** Result-sink smoke is local and scoped; cross-format fanout, S3/object-store write, table commits, and production sink support remain planned or blocked.

## Can ShardLoom Do This?

Output and fanout boundary has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

Result-sink smoke is local and scoped; cross-format fanout, S3/object-store write, table commits, and production sink support remain planned or blocked.

## How To Try It

```powershell
python examples\local-vortex-benchmark\run.py --repo-root . --rows 64 --iterations 1
```

## Internal Flow

`local_benchmark_fixture, prepared_vortex_artifact -> compatibility_import_certified -> batch -> local_result_sink_artifact, output_certificate, planned_multi_format_fanout -> evidence -> claim gate`

## Evidence You Should See

- `result_sink_write_millis`
- `result_replay_verified`
- `output_native_io_certificate_status`
- `output_format`
- `session_state_scope`
- `output_plan_reuse_hit`
- `claim_gate_status`
- `fallback_attempted=false`
- `external_engine_invoked=false`

## Expected Output Or Evidence

A local result-sink proof artifact and future fanout fields only when admitted by GAR-IOREUSE slices.

## Common Mistakes

- `coupling_input_format_to_output_format`
- `treating_local_sink_as_s3_write`
- `assuming_lakehouse_commit`

## Reference Files

- `docs/architecture/io-reuse-and-fanout-architecture.md` - What this proves: SourceState, VortexPreparedState, OutputPlan, fanout, and reuse evidence contracts.
- `docs/architecture/compute-engine-flow-reference.md` - What this proves: Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.
- `docs/benchmarks/local-taxonomy-benchmark.md` - What this proves: Local benchmark taxonomy, evidence rows, and workload-scoped interpretation boundaries.
- `examples/local-vortex-benchmark/README.md` - What this proves: Runnable or blocked example posture, expected local command path, and claim boundary.

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
