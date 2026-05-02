# Optimizer, Adaptive Execution, Runtime Filters, and Skew Skill

## Purpose

Use this skill when designing or implementing optimizer rules, cost models, physical planning, adaptive execution, runtime filters, dynamic pruning, join strategy, aggregation strategy, skew handling, or optimizer diagnostics.

ShardLoom should avoid work before making work faster.

## When to use

Use this skill for tasks involving:

- Logical optimization.
- Physical optimization.
- Cost modeling.
- Predicate pushdown.
- Projection pushdown.
- Segment pruning.
- Runtime filters.
- Dynamic pruning.
- Adaptive execution.
- Join planning.
- Aggregation planning.
- Skew detection.
- Skew handling.
- Sink-driven planning.
- Memory/spill-aware planning.
- Optimizer explain output.

## Rules

- Metadata-only is preferred before reading.
- Segment pruning is preferred before encoded execution.
- Encoded execution is preferred before decode.
- Partial decode is preferred before full materialization.
- Late materialization is preferred before eager materialization.
- Shuffle avoidance is preferred before shuffle optimization.
- Distributed execution is used only when needed.
- Runtime filters must be conservative.
- Incorrect runtime filtering is a correctness bug.
- Adaptive execution must preserve correctness.
- Sink requirements must influence materialization.
- Memory/spill pressure must influence planning.
- Optimizer decisions must be diagnosable.
- No Spark or DataFusion fallback is allowed.

## Required checks

For optimizer rules:

- Does the rule preserve semantics?
- Are preconditions explicit?
- Are non-application reasons diagnosable?
- Are null semantics preserved?
- Are unsupported shapes handled explicitly?

For runtime filters:

- Is the filter conservative?
- Is false filtering impossible?
- Is null behavior defined?
- Is filter provenance tracked?
- Is diagnostic output available?

For adaptive execution:

- What runtime fact triggered adaptation?
- What changed?
- Is correctness preserved?
- Is explain/diagnostic output updated?
- Is fallback avoided?

For skew handling:

- How was skew detected?
- What mitigation was chosen?
- Does mitigation preserve semantics?
- What if mitigation is unsupported?

## Red flags

- Using Spark AQE or DataFusion optimizer as fallback.
- Runtime filters without correctness proof.
- Join strategy chosen without memory/spill awareness.
- Silent full materialization.
- Silent shuffle.
- Adaptive behavior invisible to explain output.
- Cost model that ignores output sink requirements.
- Skew ignored in distributed or large joins.

## Example Codex prompt fragment

"Use the Optimizer and Adaptive Execution skill. Prefer metadata-only, pruning, encoded execution, partial decode, late materialization, and shuffle avoidance. Runtime filters must be conservative. Adaptive changes must be diagnosable. Do not add Spark/DataFusion fallback."
