# Translation Layer Skill

## Purpose

Use this skill when implementing or designing output translation from ShardLoom's native execution
representation into Vortex, Parquet, Arrow IPC, Iceberg-compatible files, Delta-compatible files, or
other formats.

The goal is to preserve Vortex as the highest-fidelity native output while supporting practical
ecosystem compatibility.

## When to use

Use this skill for tasks involving:

- Vortex output.
- Parquet output.
- Arrow IPC output.
- Iceberg-compatible output.
- Delta-compatible output.
- Format bridge traits.
- Schema conversion.
- Metadata preservation.
- Translation reports.
- Output writers.
- Commit protocols.

## Rules

- Vortex is the native and highest-fidelity output target.
- Other formats are compatibility/export targets.
- Translation is not fallback execution.
- Translation must not require Spark or DataFusion.
- If a target cannot preserve Vortex-specific metadata, the loss should be explicit.
- Prefer a translation report or compatibility report over silent metadata loss.
- Schema, nullability, ordering, statistics, and layout hints should be preserved where possible.
- Translation should avoid unnecessary materialization when the target can support efficient
  columnar output.
- Output behavior should be deterministic and testable.
- Do not make Iceberg or Delta table semantics part of the core engine too early. Treat them
  initially as compatible output layouts unless an RFC says otherwise.

## Required checks

For each output target:

- Test empty output.
- Test simple schema output.
- Test nullable columns.
- Test unsupported schema diagnostics.
- Test deterministic metadata handling.
- Test whether physical optimization metadata is preserved, mapped, or intentionally lost.
- Confirm Vortex output remains available and higher-fidelity than compatibility targets.

For translation reports:

- State source representation.
- State target format.
- State preserved metadata.
- State degraded or dropped metadata.
- State unsupported features.
- State whether materialization was required.

## Red flags

- Treating Parquet as the primary output target.
- Dropping Vortex metadata silently.
- Using Spark or DataFusion to write outputs.
- Confusing export compatibility with execution fallback.
- Adding lakehouse transaction semantics before the core translation contract exists.
- Implementing a writer without unsupported-schema tests.

## Example Codex prompt fragment

When working on output formats, include this instruction:

"Use the Translation Layer skill. Preserve Vortex as the highest-fidelity native output. Treat
Parquet, Arrow IPC, Iceberg-compatible, and Delta-compatible outputs as compatibility exports.
Report metadata loss explicitly. Do not add fallback execution."
