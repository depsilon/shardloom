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

- `docs/getting-started/certified-local-workload.md`
- `docs/architecture/compute-engine-flow-reference.md`
- `docs/benchmarks/local-taxonomy-benchmark.md`
- `docs/architecture/operational-evidence-policy-hardening.md`

## Related Use Cases

- `compatibility-import-certified-local`
- `benchmark-interpretation-evidence-not-leaderboard`
