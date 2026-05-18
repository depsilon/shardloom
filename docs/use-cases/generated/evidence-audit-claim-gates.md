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

- `docs/getting-started/certified-local-workload.md` - What this proves: Scoped certified local workload path and expected evidence fields.
- `docs/architecture/compute-engine-flow-reference.md` - What this proves: Canonical execution-mode, engine-mode, evidence, and claim-gate flow definitions.
- `docs/benchmarks/local-taxonomy-benchmark.md` - What this proves: Local benchmark taxonomy, evidence rows, and workload-scoped interpretation boundaries.
- `docs/architecture/operational-evidence-policy-hardening.md` - What this proves: Evidence policy rules that keep unsupported paths explicit and claim gates closed.

## Related Use Cases

- `compatibility-import-certified-local`
- `benchmark-interpretation-evidence-not-leaderboard`

## Related Field Guide Terms

- `website/field-guide/what-is-shardloom.html` - What Is ShardLoom? (`Start Here` / `technical-preview`)
- `website/field-guide/no-fallback.html` - No Fallback (`Start Here` / `core-contract`)
- `website/field-guide/evidence-certified-compute.html` - Evidence-Certified Compute (`Start Here` / `current-differentiator`)
- `website/field-guide/auto-execution-mode.html` - Auto Execution Mode (`Execution Modes` / `transparent-selection`)
- `website/field-guide/engine-modes.html` - Engine Modes (`Engine Modes` / `current-vocabulary`)
- `website/field-guide/live-engine.html` - Live Engine (`Engine Modes` / `report-only`)
- `website/field-guide/hybrid-engine.html` - Hybrid Engine (`Engine Modes` / `report-only`)
- `website/field-guide/native-io-certificate.html` - Native I/O Certificate (`Evidence And Claims` / `current-evidence`)
- `website/field-guide/execution-certificate.html` - Execution Certificate (`Evidence And Claims` / `current-evidence`)
- `website/field-guide/result-sink-replay.html` - Result-Sink Replay (`Evidence And Claims` / `current-evidence-level`)
- `website/field-guide/claim-gates.html` - Claim Gates (`Evidence And Claims` / `core-contract`)
- `website/field-guide/no-fallback-evidence.html` - No-Fallback Evidence (`Evidence And Claims` / `core-contract`)
- `website/field-guide/external-baseline-only.html` - External Baseline Only (`Evidence And Claims` / `core-boundary`)
- `website/field-guide/result-sink-proof.html` - Result-Sink Proof (`User Workflows` / `current-evidence`)
- `website/field-guide/rest-control-plane.html` - REST Control Plane (`Platform Boundaries` / `report-only`)
- `website/field-guide/mcp-agent-api.html` - MCP Agent API (`Platform Boundaries` / `planned-report-only`)
- `website/field-guide/evidence-level.html` - Evidence Level (`Performance Architecture` / `current-vocabulary`)
- `website/field-guide/certified-evidence-level.html` - Certified Evidence Level (`Performance Architecture` / `current-evidence`)
- `website/field-guide/full-replay-evidence-level.html` - Full Replay Evidence Level (`Performance Architecture` / `current-evidence`)
- `website/field-guide/release-readiness-gate.html` - Release Readiness Gate (`Release And Trust` / `current-gate`)
- `website/field-guide/security-governance-report.html` - Security/Governance Report (`Release And Trust` / `current-reporting`)
- `website/field-guide/license-provenance.html` - License And Provenance (`Release And Trust` / `current-gate`)
