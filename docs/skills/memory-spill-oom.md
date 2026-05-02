# Memory, Spill, and OOM Safety Skill

## Purpose

Use this skill when designing or implementing memory budgets, memory reservations, memory pressure behavior, spill policies, spill files, spillable operators, cleanup, or OOM-safe diagnostics.

ShardLoom should not rely on process OOM behavior. It should plan, reserve, spill, throttle, or fail deterministically before OOM where possible.

## When to use

Use this skill for tasks involving:

- Memory budgets.
- Memory pools.
- Memory reservations.
- Operator memory accounting.
- Memory pressure levels.
- Spill policies.
- Spill manager.
- Spill files.
- Spill partitions.
- Spill formats.
- Spill compression.
- Sort spill.
- Aggregate spill.
- Join spill.
- Shuffle spill.
- Window spill.
- Sink buffering.
- OOM diagnostics.
- Spill cleanup.

## Rules

- Avoid memory pressure first through pruning, streaming, zero-decode, late materialization, and adaptive sizing.
- Survive memory pressure through reservations, pressure detection, and spill.
- Do not wait for process OOM.
- Every stateful or memory-heavy operator should eventually declare memory behavior.
- Spill support must be explicit.
- Unsupported spill behavior must fail deterministically.
- Spill files must have cleanup semantics.
- Spill should be ShardLoom-native.
- Prefer columnar spill over row spill.
- Prefer encoded/Vortex-native spill where practical.
- Do not use Spark, DataFusion, DuckDB, Polars, or Velox as spill fallback.
- Do not hide full materialization when memory is insufficient.

## Required checks

For memory budget work:

- Is total budget represented?
- Is reservation behavior represented?
- Is release behavior represented?
- Are pressure levels represented?
- Are diagnostics deterministic?
- Is fallback status explicit?

For spill policy work:

- Is spill optional, required, disabled, or best effort?
- What happens when spill is unsupported?
- What happens when spill limit is exceeded?
- What happens when temp storage is unavailable?
- Are cleanup expectations defined?

For spillable operator work:

- Does the operator declare whether it can spill?
- Does it report current memory?
- Does it report estimated memory?
- Can it release memory?
- Can it create spill files?
- Can it read spilled state?
- Does it preserve correctness with nulls, ordering, grouping, or join semantics?
- Does it fail before OOM if unsupported?

For sink/output work:

- Does the sink buffer data?
- Can the sink stream?
- Can the sink spill?
- Does it require full materialization?
- Does it report memory pressure?
- Does it preserve Vortex metadata where possible?

## Red flags

- Letting memory-heavy operators allocate without reservation.
- Relying on OS OOM killer.
- Treating adaptive sizing as sufficient for all OOM cases.
- Treating streaming as sufficient for all OOM cases.
- Hidden full materialization.
- Spill files without cleanup.
- Spill decisions without diagnostics.
- Failing with vague "out of memory" messages.
- Adding Spark/DataFusion fallback for large workloads.
- Using row-oriented spill when columnar/encoded spill is practical.

## Example Codex prompt fragment

"Use the Memory, Spill, and OOM Safety skill. Represent memory reservations, pressure, spill policy, spill decisions, and cleanup explicitly. Unsupported spill behavior must fail deterministically before process OOM where possible. Do not add Spark, DataFusion, or fallback execution."
