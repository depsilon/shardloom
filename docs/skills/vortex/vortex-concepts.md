# Vortex Concepts Skill

## Purpose

Use this skill when reasoning about Vortex DTypes, arrays, encodings, layouts, statistics, validity, or native execution boundaries.

The goal is to ensure ShardLoom understands Vortex's logical and physical model before implementing execution behavior.

## Key concepts

ShardLoom should model Vortex around these concepts:

- DType: the logical domain of values.
- Array: a typed view of values and buffers.
- Encoding: the physical representation used to store values.
- Layout: the organization of arrays, segments, and persisted data.
- Statistics: metadata that can short-circuit reads and compute.
- Validity: nullability and validity information.
- Scan: a request to read projected and filtered data.
- Sink: a request to write arrays or streams to storage.

## Rules

- Treat DTypes as logical, not physical.
- Do not assume one physical representation for a logical type.
- Do not assume data must be decoded before it can participate in execution.
- Preserve the distinction between logical schema and physical layout.
- Preserve nullability and validity information.
- Preserve statistics when possible.
- Use explicit unsupported-feature diagnostics for Vortex features ShardLoom does not yet support.
- Do not hide unsupported behavior behind Arrow conversion or external engine fallback.

## Required checks

Before implementing Vortex-related behavior, identify:

- Logical DType.
- Physical encoding.
- Layout shape.
- Available statistics.
- Validity/null representation.
- Whether the operation can be answered from metadata.
- Whether the operation can be executed against encoded data.
- Whether partial decode is sufficient.
- Whether full materialization is unavoidable.

## Red flags

- Treating Vortex as equivalent to Parquet.
- Treating Vortex as merely an Arrow import/export wrapper.
- Assuming decoded Arrow is the default execution representation.
- Ignoring physical encodings during planning.
- Ignoring statistics during planning.
- Dropping validity/null information.
- Silently converting unsupported Vortex data into a lossy representation.

## Example Codex prompt fragment

"Use the Vortex Concepts skill. Preserve the distinction between logical DType and physical encoding. Do not assume decoded Arrow execution. Identify DType, encoding, layout, statistics, and validity before implementing behavior."
