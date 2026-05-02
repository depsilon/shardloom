# Vortex Internals Skill

## Purpose

Use this skill when working with Vortex-native input, Vortex-native output, encoded layouts, Vortex metadata, statistics, DTypes, arrays, encodings, scans, or persistence behavior.

The goal is to make Vortex a first-class native contract rather than a generic import/export detail.

## When to use

Use this skill for tasks involving:

- Vortex file reading.
- Vortex file writing.
- Vortex metadata inspection.
- Vortex array or DType mapping.
- Encoded layout modeling.
- Segment statistics.
- Byte ranges.
- Vortex output.
- Vortex round trips.
- Vortex-to-Arrow or Arrow-to-Vortex boundaries.

## Rules

- Vortex is a first-class native input target.
- Vortex is a first-class native output target.
- Vortex output is the highest-fidelity persistence target.
- Preserve Vortex-specific metadata whenever writing Vortex output.
- Do not decode Vortex data unless decoding is necessary for correctness or for a specific operator.
- Treat encoded layout, statistics, nullability, ordering, cardinality, and byte-range metadata as optimization inputs.
- Unsupported Vortex features must fail explicitly with clear diagnostics.
- Do not silently degrade Vortex-native execution into generic decoded execution.
- Do not use Vortex merely as a file source that immediately becomes decoded Arrow for all execution.
- Arrow interoperability is useful, but Arrow should not erase the Vortex-native execution model.
- If a translation target cannot preserve Vortex metadata, produce or plan for a translation report describing the loss.

## Required checks

For Vortex input changes:

- Validate empty and invalid paths.
- Validate missing metadata.
- Validate unsupported encodings.
- Validate nullability handling.
- Validate schema and DType mapping.
- Validate deterministic unsupported-feature errors.

For Vortex output changes:

- Confirm Vortex output is available as a native target.
- Confirm metadata and statistics are preserved where possible.
- Confirm round-trip tests exist or are planned.
- Confirm output behavior is deterministic.
- Confirm translation to lower-fidelity formats does not get confused with native Vortex output.

## Red flags

- "Read Vortex into Arrow and do everything from Arrow" as the default execution model.
- Dropping Vortex statistics or layout metadata without recording the loss.
- Treating Vortex output as optional.
- Treating Parquet as the main persistence target.
- Using DataFusion, Spark, or DuckDB to execute Vortex workloads.
- Failing unsupported Vortex behavior with vague errors.

## Example Codex prompt fragment

When working on Vortex functionality, include this instruction:

"Use the Vortex Internals skill. Preserve Vortex as native input and output. Avoid unnecessary decode. Preserve metadata and statistics where possible. Unsupported Vortex features must fail explicitly. Do not add Spark, DataFusion, or fallback execution."
