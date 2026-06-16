# Repo Readiness and User Surface Audit

Date: 2026-05-31

Status: active audit baseline for `GAR-RUNTIME-IMPL-6B`.

This audit checks whether the repository is ready to claim that the ShardLoom compute engine has no
remaining unsupported or blocked gaps, and whether the coding surface feels robust enough for users
and agents to build with ShardLoom without guessing.

## Evidence Snapshot

- Tracked source inventory: 939 files from `rg --files`.
- Completion gate after PR #990: `status=blocked`, `phase_plan_unchecked_count=1`,
  `global_review_unchecked_count=38`.
- Completion gate after this audit split: `phase_plan_unchecked_count=3` because the prior broad
  phase blocker is now joined by explicit user-surface graduation and true runtime gap burn-down
  follow-through items.
- Benchmark completion evidence: 480 ShardLoom rows, zero top-level blockers, zero residual
  substatus blockers, zero external invocation blockers, and zero external-baseline
  classification blockers for the current published `full_local` artifact.
- CLI command registry: 195 registered commands.
- CLI support-state inventory:
  - `executable`: 40
  - `feature_gated`: 12
  - `diagnostic_only`: 8
  - `report_only`: 134
  - `blocked`: 0
  - `future`: 0
- Python user surface inventory:
  - `ShardLoomClient`: 99 public methods.
  - `ShardLoomContext`: 73 public methods.
- Stale-ledger inventory found two completed session entries that still said `pending`; both map to
  already-merged PRs #983 and #984.
- User-surface smoke found `shardloom --help` and `shardloom <command> --help` were rejected even
  though `shardloom help [command]` existed.

## Findings

The repo is not ready for a "no gaps / no blockers / no unsupported anywhere" claim. The blocker
surface has improved materially because benchmark blockers are now zero, but the global architecture
review still records 38 broad incomplete areas and the phase plan still carries the active
completion burn-down.

The command registry is internally disciplined: no command is currently classified as `blocked` or
`future`, and every public command has support-state, side-effect, input/output contract, owning
phase, no-fallback, and external-engine metadata. The large `report_only` count is intentional
evidence of remaining engine maturity work, not stale command metadata by itself.

The coding surface is usable for the current admitted local workflows, but it is not yet mature
enough to feel complete. High-level Python context helpers now cover the scoped local runtime
families that have been promoted recently, including generated sources, local object-store smokes,
local table metadata/append smokes, SQLite fixture import/export, REST planning, live/hybrid
fixtures, extension inspection, and the built-in deterministic scalar UDF fixture. The remaining
gap is a formal graduation matrix that says which CLI/report-only families deserve a high-level
context helper, which should remain low-level client-only, and which must stay diagnostic/report
only until runtime evidence exists.

The first concrete ergonomic defect was standard help flag handling. Users reasonably expect
`shardloom --help`, `shardloom -h`, and `shardloom <command> --help` to work. Those aliases are
part of the user-surface baseline because a command registry is not enough if the first discovery
command feels non-standard.

## True Runtime Gap Families

These are not stale docs and should not be checkbox-closed without implementation or stronger
claim-boundary reclassification:

- Broad SQL/DataFrame runtime beyond scoped local generated/source-free lanes.
- Universal native Vortex source, sink, operator, DType, nested/null, and metadata-only coverage.
- General Vortex reader/writer execution, object-store Vortex I/O, and production writer claims.
- Production output sink APIs, object-store output, catalog integration, and Iceberg/Delta commit
  semantics.
- Object-store providers, credential/probe policy, retries, checkpoints, coordinator/worker
  runtime, distributed execution, and object-store commits.
- Broad Spark-displacement/performance/superiority claims beyond current local benchmark evidence.
- Dynamic plugin ABI loading, arbitrary UDF runtimes, LLM/API calls, embeddings, and external
  effects.
- Full streaming runtime, object-store streaming reads, production spill/OOM enforcement, broad
  property/fuzz coverage, adaptive runtime filters, skew handling, broad retry/cancellation/commit
  execution, live profiling, metrics/exporter backends, and distributed runtime introspection.
- Public package publication, release tags, signing, attestations, managed platform lanes, and
  production platform integrations.

## Cleanup Actions in This Slice

- Add standard CLI help aliases while preserving the existing `help` command and registry-driven
  metadata.
- Expose those aliases in the command registry/status surface.
- Replace stale `pending` PR references for already-merged completed-ledger entries with #983 and
  #984.

## Follow-Through Queue

1. User-surface graduation matrix: map every command family and Python surface to one of
   `high_level_context`, `client_only`, `diagnostic_only`, `feature_gated`, or
   `not_user_facing`, with acceptance criteria for promotion.
2. Runtime gap burn-down map: completed by
   `docs/architecture/runtime-gap-family-burn-down.md` and
   `scripts/check_runtime_gap_family_burn_down.py`; each of the 38 global review blockers now maps
   to a family-owned runtime slice with explicit evidence, validator, no-fallback, and claim
   boundaries. Close each family only through implementation, deterministic admission, validators,
   or justified reclassification.
3. Freshness/consolidation sweep: remove duplicate completed-ledger wording, stale platform-specific
   validation text, and superseded blocker descriptions after each runtime family lands.
4. Package/deploy readiness gate: only after true runtime gaps are closed, run release/package
   evidence without `--allow-blocked` and attach package publication proof gates.
