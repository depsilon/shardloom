# Vortex Statistics and Pruning Skill

## Purpose

Use this skill when implementing metadata-only answers, segment pruning, predicate pushdown,
projection pushdown, or statistics-aware execution.

The goal is to avoid reading, decoding, and materializing data whenever Vortex metadata can prove
work is unnecessary.

## When to use

Use this skill for tasks involving:

- Segment statistics.
- Array statistics.
- Metadata-only execution.
- Predicate pruning.
- Projection pruning.
- Null-count optimization.
- Constant-value optimization.
- Sorted-value optimization.
- Boolean true-count optimization.
- Run-count optimization.
- Cost estimation.

## Rules

- Always check whether a query can be answered from metadata before reading data.
- Always check whether a segment can be pruned before reading segment bytes.
- Use statistics conservatively. Incorrect pruning is a correctness bug.
- If statistics are missing or insufficient, fall back to ShardLoom-native encoded or decoded
  execution, not another engine.
- Do not assume statistics are exact unless the source contract says they are exact.
- Make pruning decisions explainable.
- Record whether a segment was read, pruned, or answered from metadata.
- Preserve statistics when writing Vortex output where possible.

## Required checks

For pruning logic:

- Predicate matches all rows.
- Predicate matches no rows.
- Predicate overlaps segment range.
- Predicate with null values.
- Predicate with missing statistics.
- Constant segment.
- Sorted segment.
- Empty segment.
- All-null segment.
- Unsupported predicate diagnostic.

For metadata-only answers:

- Count from length/null metadata.
- Min/max from statistics when valid.
- Boolean counts from statistics when valid.
- Constant-value result from statistics when valid.
- Explicit fallback to ShardLoom-native execution when metadata is insufficient.

## Red flags

- Pruning based on incomplete statistics without proof.
- Treating missing statistics as proof of absence.
- Ignoring nulls in range predicates.
- Reading data before checking available statistics.
- Not exposing pruning diagnostics.
- Using Spark/DataFusion to evaluate unpruned segments.

## Example Codex prompt fragment

"Use the Vortex Statistics and Pruning skill. Check metadata-only answers and segment pruning before
reads. Treat incorrect pruning as a correctness bug. Missing stats should trigger ShardLoom-native
execution, not fallback to another engine."
