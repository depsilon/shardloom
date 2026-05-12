# Encoded Execution Skill

## Purpose

Use this skill when designing or implementing execution over encoded columnar data.

The goal is to make ShardLoom compute directly over encoded segments where possible, rather than
always decoding into a generic representation first.

## When to use

Use this skill for tasks involving:

- Encoded predicates.
- Segment pruning.
- Statistics-only answers.
- Selection vectors.
- Late materialization.
- Partial decode.
- Encoded aggregates.
- Encoded joins.
- Null semantics.
- Operator kernels.
- Execution diagnostics.

## Rules

ShardLoom should evaluate work in this order:

1. Answer from metadata when possible.
2. Prune segments using statistics.
3. Evaluate predicates against encoded data where possible.
4. Decode only the required subset when necessary.
5. Materialize columns as late as possible.
6. Preserve selection vectors until materialization is required.
7. Fail explicitly for unsupported encoded operations.

Execution code should:

- Maintain correct null semantics.
- Preserve ordering when the plan requires ordering.
- Distinguish encoded execution from decoded reference execution.
- Allow decoded reference paths for tests, but not as hidden production fallback.
- Avoid row-wise fallback in hot paths unless explicitly justified.
- Track whether an operation was metadata-only, encoded, partially decoded, or fully materialized.
- Avoid unnecessary allocation and copying.
- Prefer batch-oriented and segment-oriented execution over row-oriented loops.

## Required checks

For encoded predicates:

- Test true, false, null, and mixed-null inputs.
- Test empty inputs.
- Test all-null inputs.
- Test low-cardinality and high-cardinality inputs.
- Compare against a decoded reference result for correctness.

For selection vectors:

- Test empty selections.
- Test full selections.
- Test sparse selections.
- Test dense selections.
- Test repeated application through multiple operators.

For late materialization:

- Confirm unused columns are not materialized.
- Confirm filtered-out rows are not materialized.
- Confirm required columns are materialized correctly.
- Confirm diagnostics show when materialization occurs.

## Red flags

- Immediately decoding all columns at scan time.
- Materializing columns before filters are applied.
- Ignoring null semantics in encoded predicates.
- Using row-wise loops as the default execution strategy.
- Treating decoded Arrow execution as the default engine model.
- Adding another engine to handle unsupported encoded operations.
- Making performance claims without decoded-reference correctness tests and benchmarks.

## Example Codex prompt fragment

When implementing encoded execution, include this instruction:

"Use the Encoded Execution skill. Prefer metadata, pruning, encoded evaluation, partial decode, and
late materialization in that order. Preserve selection vectors. Test null semantics and compare
against decoded reference results. Do not add fallback execution."
