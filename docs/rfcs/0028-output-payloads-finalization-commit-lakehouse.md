# RFC 0028 — Output Payloads, Manifest Finalization, Commit Execution, and Lakehouse Semantics

## Scope

This RFC defines implementation contracts for:
- CG-3 output payload write path.
- CG-4 commit protocol execution.
- CG-9 lakehouse/table intelligence.
- CG-10 object-store/distributed execution progression.

## Output payload and manifest contracts

- Real Vortex output payload path is required for CG-3.
- Output payload readiness and fidelity must be explicit and machine-reportable.
- Manifest finalization must produce finalized-candidate state distinct from committed state.
- Committed manifests remain separate from finalized candidates.
- Existing local placeholder output payload artifacts are not real Vortex payloads.
- Real executable Vortex output payload writes are a separate milestone from placeholder artifact
  support.
- Manifest finalization/commit must not treat placeholder payload artifacts as production output.
- CG-3 acceptance requires real payload fidelity over at least one supported workload path.

## Commit execution progression

- Local-first commit execution.
- Idempotency and recovery are mandatory.
- No object-store commit initially.
- Object-store commit protocol is deferred to later gates.

## Upstream Vortex write posture

- Upstream Vortex write APIs remain feature-gated and deferred until explicit approval.
- Real payload execution must preserve no-fallback and deterministic diagnostics.

## Lakehouse semantics and planning

- Schema evolution.
- Partition evolution.
- Delete/tombstone semantics.
- CDC/incremental planning.
- Layout health.
- Compaction planning.

## Object-store/distributed progression

- Object-store read planning.
- Byte-range coalescing.
- Object-store commit protocol later.
- Distributed scheduling later.

## Non-goals

- No external fallback execution.
- No immediate object-store commit execution.
