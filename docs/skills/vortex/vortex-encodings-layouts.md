# Vortex Encodings and Layouts Skill

## Purpose

Use this skill when designing or implementing behavior that depends on Vortex physical encodings or layouts.

The goal is to let ShardLoom exploit encoded representations rather than erasing them.

## When to use

Use this skill for tasks involving:

- Dictionary encoding.
- Run-length encoding.
- FSST-like string encodings.
- FastLanes-like numeric encodings.
- ALP-like floating-point encodings.
- Constant arrays.
- Sorted arrays.
- Nested or struct layouts.
- Layout-aware pruning.
- Encoded predicate kernels.
- Encoded aggregate kernels.
- Partial decode.
- Late materialization.

## Rules

- Planning should inspect encoding and layout before choosing an execution strategy.
- Encoded operations should declare their supported encodings.
- Unsupported encoded operations should fail explicitly or choose a documented ShardLoom-native partial decode path.
- A partial decode path is not the same as fallback execution.
- A decoded reference path may be used for tests but must not become hidden production fallback.
- Avoid row-wise loops unless explicitly justified.
- Track whether an operation ran as metadata-only, encoded, partially decoded, or fully materialized.
- Preserve layout information until the materialization boundary.

## Required checks

For an encoded operation:

- Supported DTypes.
- Supported encodings.
- Supported layouts.
- Null semantics.
- Selection vector behavior.
- Materialization boundary.
- Decoded reference comparison.
- Unsupported diagnostics.

For partial decode:

- Why full decode is not required.
- Which buffers/values are decoded.
- How selections are preserved.
- How nulls are handled.
- Whether output remains compatible with Vortex-native persistence.

## Red flags

- Decoding all arrays before filtering.
- Materializing columns before projection pruning.
- Ignoring dictionary IDs for equality predicates.
- Ignoring run information for RLE-like encodings.
- Ignoring constant/sorted statistics.
- Losing selection vectors too early.
- Calling an external engine for unsupported encoded operations.

## Example Codex prompt fragment

"Use the Vortex Encodings and Layouts skill. Inspect encoding/layout first. Prefer encoded kernels, partial decode, and late materialization. Track execution state and compare against decoded reference tests."
