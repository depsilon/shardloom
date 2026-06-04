<!-- SPDX-License-Identifier: Apache-2.0 -->

# Benchmark evidence, not leaderboard

## Quick Answer

- **Audience:** user comparing local evidence rows without overclaiming performance
- **Status:** `smoke_supported`
- **Execution mode:** `mixed_by_row`
- **Engine mode:** `batch`
- **Claim boundary:** Benchmarks are local evidence and attribution, not a speed leaderboard, performance claim, superiority claim, Spark-displacement claim, or production proof. The next promoted artifact must show route-shape/source-array guard fields plus the new text-adapter RecordBatch and writer-byte digest evidence before any updated speed interpretation.

## Can ShardLoom Do This?

Benchmark evidence, not leaderboard has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

Benchmarks are local evidence and attribution, not a speed leaderboard, performance claim, superiority claim, Spark-displacement claim, or production proof. The next promoted artifact must show route-shape/source-array guard fields plus the new text-adapter RecordBatch and writer-byte digest evidence before any updated speed interpretation.

## How To Try It

```text
python examples\local-vortex-benchmark\run.py --repo-root . --rows 64 --iterations 1
```

## Blocker

No current blocker is attached to this supported local smoke path beyond the claim boundary above.

## Internal Flow

`local_benchmark_fixture, optional_external_baselines -> mixed_by_row -> batch -> timing_rows, coverage_rows, claim_boundary_notes -> evidence -> claim gate`

## Evidence You Should See

- `engine`
- `execution_mode`
- `source_read_millis`
- `compatibility_parse_millis`
- `source_state_materialization_layout`
- `source_state_record_batch_count`
- `vortex_prepare_millis`
- `route_shape_route_lane_id`
- `route_shape_stage_attribution_scope`
- `source_to_vortex_array_guard_status`
- `source_to_vortex_array_guard_exclusive_stage_field`
- `source_to_vortex_array_guard_inclusive_parent_field`
- `vortex_array_build_strategy`
- `vortex_array_build_input_layout`
- `vortex_write_millis`
- `vortex_digest_millis`
- `vortex_scan_millis`
- `operator_compute_millis`
- `result_sink_write_millis`
- `claim_gate_status`
- `fallback_attempted=false`
- `external_engine_invoked=false`

## Expected Output Or Evidence

Timing rows and coverage rows that keep ShardLoom runtime lanes separate from optional external baselines, with route-shape stratification, source-to-Vortex-array guard status, and import layout/digest attribution visible for cold certified rows.

## Common Mistakes

- `ranking_external_engines_as_public_claims`
- `comparing_import_costs_to_pure_query_speed`
- `hiding_external_baseline_only_status`

## Reference Files

- `docs/benchmarks/local-taxonomy-benchmark.md` - What this proves: Local benchmark taxonomy, evidence rows, and workload-scoped interpretation boundaries.
- `docs/benchmarks/baseline-comparison-boundary.md` - What this proves: Benchmark comparison boundaries and external-baseline-only policy.
- `benchmarks/traditional_analytics/README.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `docs/architecture/benchmark-suite-catalog.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.

## Related Use Cases

- `prepared-native-vortex-runtime-direction`
- `compatibility-import-certified-local`

## Related Field Guide Terms

- `website/field-guide/evidence-gated-compute.html` - Evidence-gated compute (`Start Here` / `smoke_supported`)
- `website/field-guide/native-vortex.html` - native_vortex (`Prepared/Native Vortex` / `smoke_supported`)
- `website/field-guide/claim-gate-status.html` - claim_gate_status (`Evidence + Certificates` / `runtime_supported`)
- `website/field-guide/benchmark-evidence.html` - Benchmark evidence (`Benchmarks` / `smoke_supported`)
- `website/field-guide/certified-cold-route.html` - Certified cold route (`Benchmarks` / `smoke_supported`)
- `website/field-guide/prepared-warm-route.html` - Prepared warm route (`Benchmarks` / `smoke_supported`)
- `website/field-guide/external-baseline-only.html` - external_baseline_only (`Benchmarks` / `runtime_supported`)
- `website/field-guide/scale-classes.html` - Scale classes (`Scale + Resource Envelope` / `planned`)
