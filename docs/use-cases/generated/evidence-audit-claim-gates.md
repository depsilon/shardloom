<!-- SPDX-License-Identifier: Apache-2.0 -->

# Evidence, audit, and claim gates

## Quick Answer

- **Audience:** reviewer who needs to know what proof a run produced
- **Status:** `ready_local`
- **Execution mode:** `compatibility_import_certified`
- **Engine mode:** `batch`
- **Claim boundary:** Audit fields explain scoped local evidence only; missing evidence blocks claims and never permits hidden fallback.

## Can ShardLoom Do This?

Evidence, audit, and claim gates has a scoped local path. Treat it as technical-preview evidence with the listed claim boundary.

## Claim Boundary

Audit fields explain scoped local evidence only; missing evidence blocks claims and never permits hidden fallback.

## How To Try It

```powershell
python examples\local-vortex-benchmark\run.py --repo-root . --rows 64 --iterations 1
```

## Internal Flow

`local_certified_workload -> compatibility_import_certified -> batch -> execution_certificate, native_io_certificate, claim_gate_status, result_replay_evidence -> evidence -> claim gate`

## Evidence You Should See

- `execution_mode`
- `engine_mode`
- `native_io_certificate_status`
- `materialization_boundary`
- `result_replay_verified`
- `claim_gate_status`
- `fallback_attempted=false`
- `external_engine_invoked=false`

## Expected Output Or Evidence

A local benchmark smoke artifact with certificate, result-sink, materialization, claim gate, and no-fallback fields.

## Common Mistakes

- `omitting_claim_gate_status`
- `assuming_missing_evidence_means_supported`
- `ignoring_materialization_boundary`

## Reference Files

- `docs/getting-started/certified-local-workload.md` - What this proves: Scoped certified local workload path and expected evidence fields.
- `docs/architecture/compute-engine-flow-reference.md` - What this proves: Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.
- `docs/benchmarks/local-taxonomy-benchmark.md` - What this proves: Local benchmark taxonomy, evidence rows, and workload-scoped interpretation boundaries.
- `docs/architecture/operational-evidence-policy-hardening.md` - What this proves: Evidence policy rules that keep unsupported paths explicit and claim gates closed.

## Related Use Cases

- `compatibility-import-certified-local`
- `benchmark-interpretation-evidence-not-leaderboard`

## Related Field Guide Terms

- `website/field-guide/what-is-shardloom.html` - What is ShardLoom? (`Start Here` / `runtime_supported`)
- `website/field-guide/evidence-gated-compute.html` - Evidence-gated compute (`Start Here` / `smoke_supported`)
- `website/field-guide/claim-gate-status.html` - claim_gate_status (`Evidence + Certificates` / `runtime_supported`)
- `website/field-guide/materialization-boundary.html` - Materialization boundary (`Evidence + Certificates` / `smoke_supported`)
- `website/field-guide/result-sink-replay.html` - Result-sink replay (`Evidence + Certificates` / `smoke_supported`)
- `website/field-guide/external-baseline-only.html` - external_baseline_only (`Benchmarks` / `runtime_supported`)
