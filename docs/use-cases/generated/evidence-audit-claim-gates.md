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

```text
python examples\local-vortex-benchmark\run.py --repo-root . --rows 64 --iterations 1
```

## Blocker

No current blocker is attached to this supported local smoke path beyond the claim boundary above.

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

- `docs/getting-started/certified-local-workload.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.
- `docs/architecture/compute-engine-flow-reference.md` - What this proves: Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.
- `docs/benchmarks/local-taxonomy-benchmark.md` - What this proves: Local benchmark taxonomy, evidence rows, and workload-scoped interpretation boundaries.
- `docs/architecture/operational-evidence-policy-hardening.md` - What this proves: This source anchors the page claim boundary, evidence fields, and support posture.

## Related Use Cases

- `compatibility-import-certified-local`
- `benchmark-interpretation-evidence-not-leaderboard`

## Related Field Guide Terms

- [What is ShardLoom?](https://shardloom.io/field-guide/what-is-shardloom) (`Start Here` / `runtime_supported`)
- [Evidence-gated compute](https://shardloom.io/field-guide/evidence-gated-compute) (`Start Here` / `smoke_supported`)
- [claim_gate_status](https://shardloom.io/field-guide/claim-gate-status) (`Evidence + Certificates` / `runtime_supported`)
- [Materialization boundary](https://shardloom.io/field-guide/materialization-boundary) (`Evidence + Certificates` / `smoke_supported`)
- [Result-sink replay](https://shardloom.io/field-guide/result-sink-replay) (`Evidence + Certificates` / `smoke_supported`)
- [external_baseline_only](https://shardloom.io/field-guide/external-baseline-only) (`Benchmarks` / `runtime_supported`)
